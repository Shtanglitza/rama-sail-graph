(ns rama-sail.migrate
  "Rewrites stored data that RDF4J 6 (RDF 1.2) cannot read, before upgrading.

   RDF-star let a triple term be a subject: `<< :a :b :c >> :source :hr`. In
   RDF 1.2 a triple term is only an object, and statements about a triple go
   through a reifier: `_:r rdf:reifies <<( :a :b :c )>>` and `_:r :source :hr`.
   RDF 1.2 SPARQL reads `<< :a :b :c >> :source ?s` that way too.

   `migrate!` rewrites every quad with a triple term in subject position (at
   any depth, also inside a triple-term object) to that shape:

     << s p o >> :x :y g   ->   R rdf:reifies << s p o >> g   and   R :x :y g

   with ONE reifier per distinct triple term, a blank node named after a hash
   of the term (`_:reif-<hash>`), so all statements about one triple stay on one
   node, as before, and running it again changes nothing. A triple term nested
   in the subject of another triple term is reified first, innermost out.

   It works on canonical strings and the module's :commit records, so it runs
   on the module before the RDF4J upgrade. Stop other writers while it runs:
   it reads a page, then writes its rewrite. `dry-run` reports what it would
   change without writing; `migrate!` finishes with a dry run that must find
   nothing left."
  (:require [com.rpl.rama :as rama]
            [rama-sail.bulk :as bulk]
            [rama-sail.module.indexer :as idx]
            [rama-sail.sail.serialization :as ser])
  (:import (java.nio.charset StandardCharsets)
           (java.security MessageDigest)))

(def rdf-reifies "<http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies>")

(def default-page-keys
  "Index keys (distinct subject or object terms) read per page."
  500)

;;; ---------------------------------------------------------------------------
;;; Rewriting (pure)
;;; ---------------------------------------------------------------------------

(defn triple-term? [^String s] (some? (ser/triple-term-parts s)))

(defn reifier
  "The blank node that reifies triple term `t`: named after a SHA-256 of its
   canonical string, so the same term always gets the same reifier."
  [^String t]
  (let [digest (.digest (MessageDigest/getInstance "SHA-256") (.getBytes t StandardCharsets/UTF_8))]
    (str "_:reif-" (subs (apply str (map #(format "%02x" (bit-and % 0xff)) digest)) 0 32))))

(defn rewrite-term
  "[term' reifications]: `t` with every triple term in the subject position of
   a triple term replaced by its reifier (innermost first), and the
   [reifier triple-term] pairs that need an rdf:reifies quad. A term without
   such nesting comes back unchanged with no pairs."
  [t]
  (if-let [[s p o] (ser/triple-term-parts t)]
    (let [[s' s-pairs] (if (triple-term? s)
                         (let [[inner pairs] (rewrite-term s)
                               r (reifier inner)]
                           [r (conj pairs [r inner])])
                         [s []])
          [o' o-pairs] (rewrite-term o)]
      [(if (and (= s s') (= o o')) t (str "<< " s' " " p " " o' " >>"))
       (into s-pairs o-pairs)])
    [t []]))

(defn rewrite-quad
  "nil when quad [s p o c] is readable by RDF4J 6. Otherwise
   {:del quad :add [quads] :nested? bool}: the quad moved to the reifier of its
   subject triple term (and/or with nested subject triple terms in its object
   reified), plus the rdf:reifies quads, all in graph c. :nested? marks a
   triple term in subject position INSIDE a triple term, whose rewrite changes
   what the term denotes."
  [[s p o c :as quad]]
  (let [[s' s-pairs] (if (triple-term? s)
                       (let [[t pairs] (rewrite-term s)
                             r (reifier t)]
                         [r (conj pairs [r t])])
                       [s []])
        [o' o-pairs] (rewrite-term o)
        pairs (into s-pairs o-pairs)]
    (when (seq pairs)
      {:del quad
       :add (into [[s' p o' c]] (for [[r t] pairs] [r rdf-reifies t c]))
       :nested? (> (count pairs) (if (triple-term? s) 1 0))})))

(defn commit-records
  "The :commit records for a page of rewrites: adds before deletes (a reader in
   between sees the statement twice rather than not at all), at most
   idx/max-commit-ops ops per record."
  [rewrites]
  (let [adds (distinct (mapcat :add rewrites))
        dels (distinct (map :del rewrites))
        ops (concat (map #(vector :add %) adds) (map #(vector :del %) dels))]
    (mapv #(vector :commit (vec %)) (partition-all idx/max-commit-ops ops))))

;;; ---------------------------------------------------------------------------
;;; Scanning
;;; ---------------------------------------------------------------------------

(defn- check-module! [cluster module-name]
  (try (rama/foreign-query cluster module-name "triple-term-quads")
       (catch Exception e
         (throw (ex-info (str "Module " module-name " has no triple-term-quads query topology; "
                              "update the module to a version that includes rama-sail.migrate")
                         {:module-name module-name} e)))))

(defn- scan!
  "Calls (f quads) for each page of quads whose `position` (:subject or
   :object) is a triple term."
  [qt position page-keys f]
  (loop [after nil]
    (let [page (rama/foreign-invoke-query qt position after page-keys)]
      (when (seq page)
        (f (vec (mapcat second page)))
        (recur (first (peek page)))))))

(defn- page-rewrites
  "Rewrites for one page. The object scan skips quads with a triple-term
   subject: the subject scan handles those."
  [position quads]
  (keep rewrite-quad (if (= position :object)
                       (remove #(triple-term? (first %)) quads)
                       quads)))

(defn- rewrite-all!
  [cluster module-name {:keys [page-keys write?] :or {page-keys default-page-keys}}]
  (let [qt (check-module! cluster module-name)
        depot (when write? (rama/foreign-depot cluster module-name "*triple-depot"))
        report (atom {:quads 0 :nested 0 :reifiers #{} :samples []})]
    (doseq [position [:subject :object]]
      (scan! qt position page-keys
             (fn [quads]
               (let [rewrites (page-rewrites position quads)]
                 (swap! report (fn [r]
                                 (-> r
                                     (update :quads + (count rewrites))
                                     (update :nested + (count (filter :nested? rewrites)))
                                     (update :reifiers into (for [rw rewrites
                                                                  [r p] (rest (:add rw))
                                                                  :when (= p rdf-reifies)]
                                                              r))
                                     (update :samples #(into % (take (- 5 (count %)) rewrites))))))
                 (when (and write? (seq rewrites))
                   (doseq [record (commit-records rewrites)]
                     (rama/foreign-append! depot record :append-ack)))))))
    (let [r @report]
      (assoc r :reifiers (count (:reifiers r))))))

(defn dry-run
  "What `migrate!` would change, without writing:
   {:quads n :reifiers n :nested n :samples [up to 5 rewrites]}.
   :quads are the quads RDF4J 6 cannot read, :reifiers the distinct reifiers
   they need, :nested the quads with a triple term in subject position inside
   a triple term (their rewrite changes what the term denotes; review them)."
  ([cluster module-name] (dry-run cluster module-name {}))
  ([cluster module-name opts] (rewrite-all! cluster module-name (assoc opts :write? false))))

(defn migrate!
  "Rewrites every quad RDF4J 6 cannot read (see the namespace docstring), waits
   until the indexer has applied the rewrite, then checks with a dry run.
   Returns {:rewritten <dry-run report before writing> :remaining <dry-run
   report after>}; :remaining :quads is 0 unless another writer added such
   quads meanwhile (run it again). Options: :page-keys (default 500)."
  ([cluster module-name] (migrate! cluster module-name {}))
  ([cluster module-name opts]
   (let [rewritten (rewrite-all! cluster module-name (assoc opts :write? true))]
     (bulk/await-applied! cluster module-name)
     {:rewritten rewritten
      :remaining (dry-run cluster module-name opts)})))

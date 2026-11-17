(ns rama-sail.sail.w3c.runner
  "Runs a W3C test manifest (SPARQL or RDF format suites) against a RamaSail
   repository and reports through clojure.test.

   Every test goes through the store:
   - SPARQL query/update evaluation: load the data, run, compare results.
     Consecutive tests on the same data share one load (tests are grouped by
     data); an update test always loads afresh.
   - SPARQL positive syntax: prepare AND evaluate (an update is rolled back),
     so a query the native compiler cannot handle fails here too.
   - SPARQL negative syntax: preparing must fail (the RDF4J parser's verdict).
   - RDF format evaluation and C14N: load the input into the store, read it
     back, compare (isomorphism) with the expected N-Triples / N-Quads. For
     C14N this checks the round trip through the store, not the canonical
     text: RamaSail does not write N-Triples itself.
   - RDF positive syntax: load and read back without error.
   - RDF negative syntax: parsing must fail (the RDF4J parser's verdict; the
     store is not involved).

   Queries and updates are parsed with their file's URL as base IRI:
   relative IRIs in them (GRAPH <ng-01.ttl>) name the data files.

   A sync commit waits for the indexer, seconds each, so commits are few. A
   suite's format tests load in ONE commit: every test's graphs are renamed
   to graphs of its own (urn:x-w3c-test:...), read back from those and
   renamed back. Blank nodes cannot clash, as every parse gets its own blank
   node prefix. If the store rejects that commit, each test loads on its own.

   A test listed in `expected-failures` must fail; if it passes the run fails
   too, so the list shrinks when a feature lands."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [is testing]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.sail.w3c.manifest-parser :as manifest]
            [rama-sail.sail.w3c.result-compare :as rc])
  (:import (java.lang AutoCloseable)
           (org.eclipse.rdf4j.model Resource Statement)
           (org.eclipse.rdf4j.model.impl LinkedHashModel SimpleValueFactory)
           (org.eclipse.rdf4j.model.util Models)
           (org.eclipse.rdf4j.query QueryLanguage)
           (org.eclipse.rdf4j.repository RepositoryConnection)
           (org.eclipse.rdf4j.repository.sail SailRepository)
           (org.eclipse.rdf4j.rio RDFFormat Rio)
           (org.eclipse.rdf4j.rio.helpers StatementCollector)))

(def ^:dynamic *repo*
  "The suite's repository, bound by `repo-fixture`."
  nil)

(defn repo-fixture
  "A :once fixture: an in-process cluster running the module, and a
   SailRepository on it with sync commits (each commit waits for the indexer),
   bound to `*repo*`."
  [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (let [repo (doto (SailRepository. (rsail/create-rama-sail ipc (rama/get-module-name RdfStorageModule)
                                                              {:sync-commits true}))
                 (.init))]
      (try (binding [*repo* repo] (f))
           (finally (.shutDown repo))))))

(def ^:private no-ctx (into-array Resource []))

(defn- format-of ^RDFFormat [^String file]
  (cond
    (str/ends-with? file ".ttl") RDFFormat/TURTLE
    (str/ends-with? file ".trig") RDFFormat/TRIG
    (str/ends-with? file ".nt") RDFFormat/NTRIPLES
    (str/ends-with? file ".nq") RDFFormat/NQUADS
    (str/ends-with? file ".rdf") RDFFormat/RDFXML
    :else (throw (ex-info "Unknown RDF format" {:file file}))))


(defn- base-iri
  "The base IRI a test file is parsed with: the manifest's mf:assumedTestBase
   (expected results use IRIs resolved against it), else the file's URL."
  [dir file assumed-base]
  (if assumed-base
    (str assumed-base file)
    (str (io/resource (str dir file)))))

(defn- in-transaction!
  "Runs (f) in a transaction that first empties the store; commits."
  [^RepositoryConnection conn f]
  (.begin conn)
  (try
    (.clear conn no-ctx)
    (f)
    (.commit conn)
    (catch Throwable e
      (.rollback conn)
      (throw e))))

(defn- reset-store!
  "Empties the store and loads `files` ([file base context-or-nil] ...) in ONE
   transaction."
  [^RepositoryConnection conn dir files]
  (in-transaction! conn
    #(doseq [[file base ctx] files
             :let [url (io/resource (str dir file))]]
       (when (seq (str/trim (slurp url)))
         (with-open [in (.openStream url)]
           (.add conn in ^String base (format-of file)
                 ^"[Lorg.eclipse.rdf4j.model.Resource;" (into-array Resource (if ctx [ctx] []))))))))

(defn- parse-file [dir file base]
  (let [collector (StatementCollector.)
        parser (doto (Rio/createParser (format-of file)) (.setRDFHandler collector))]
    (with-open [in (.openStream (io/resource (str dir file)))]
      (.parse parser in ^String base))
    (LinkedHashModel. (.getStatements collector))))

(defn- ok [] {:pass? true})
(defn- fail [msg] {:pass? false :message msg})

;;; --- SPARQL ---

(defn- data-key [dir {:keys [data-file graph-data-files]}]
  [dir data-file (vec graph-data-files)])

(defn- load-query-data!
  "Loads the test's data unless `loaded` (an atom: the data-key in the store,
   or nil when unknown) says it is there already."
  [^RepositoryConnection conn dir {:keys [data-file graph-data-files] :as t} loaded]
  (let [k (data-key dir t)]
    (when-not (= k @loaded)
      (reset! loaded nil)
      (reset-store! conn dir
                    (concat (when data-file [[data-file (base-iri dir data-file nil) nil]])
                            (for [g graph-data-files
                                  :let [base (base-iri dir g nil)]]
                              [g base (.createIRI (.getValueFactory conn) base)])))
      (reset! loaded k))))

(defn- query-evaluation [conn dir {:keys [query-file result-file] :as t} loaded]
  (load-query-data! conn dir t loaded)
  (let [q (slurp (io/resource (str dir query-file)))
        base (base-iri dir query-file nil)
        result (str dir result-file)]
    (case (rc/detect-query-type q)
      :select (rc/run-select-test conn q base result)
      :construct (rc/run-construct-test conn q base result)
      :ask (rc/run-ask-test conn q base result))))

(defn- update-evaluation [conn dir {:keys [update-file result-file] :as t} loaded]
  (reset! loaded nil)
  (load-query-data! conn dir t loaded)
  (reset! loaded nil)
  (rc/run-update-test conn (slurp (io/resource (str dir update-file))) (base-iri dir update-file nil)
                      (str dir result-file)))

(defn- query-text [dir {:keys [action-file]}]
  (slurp (io/resource (str dir action-file))))

(defn- prepare-query [^RepositoryConnection conn dir {:keys [action-file] :as t}]
  (.prepareQuery conn QueryLanguage/SPARQL (query-text dir t) (base-iri dir action-file nil)))

(defn- prepare-update [^RepositoryConnection conn dir {:keys [action-file] :as t}]
  (.prepareUpdate conn QueryLanguage/SPARQL (query-text dir t) (base-iri dir action-file nil)))

(defn- positive-query-syntax [^RepositoryConnection conn dir t]
  (let [q (prepare-query conn dir t)
        r (.evaluate q)]
    (when (instance? AutoCloseable r)
      (with-open [^AutoCloseable it r]
        (doall (iterator-seq it))))
    (ok)))

(defn- positive-update-syntax [^RepositoryConnection conn dir t]
  (.begin conn)
  (try
    (.execute (prepare-update conn dir t))
    (ok)
    (finally (.rollback conn))))

(defn- negative-syntax [prepare]
  (try
    (prepare)
    (fail "accepted, but the test is a syntax error")
    (catch Exception _ (ok))))

;;; --- RDF formats ---

(def ^:private format-evaluation-types
  #{:TestTurtleEval :TestTrigEval :TestXMLEval :TestNTriplesPositiveC14N :TestNQuadsPositiveC14N})
(def ^:private format-positive-types
  #{:TestTurtlePositiveSyntax :TestTrigPositiveSyntax :TestNTriplesPositiveSyntax :TestNQuadsPositiveSyntax})
(def ^:private format-negative-types
  #{:TestTurtleNegativeSyntax :TestTrigNegativeSyntax :TestNTriplesNegativeSyntax :TestNQuadsNegativeSyntax
    :TestXMLNegativeSyntax :TestTurtleNegativeEval :TestTrigNegativeEval})

(def ^:private vf (SimpleValueFactory/getInstance))

(defn test-id
  "A test's id: the local name of its IRI (names are not unique in every manifest)."
  [{:keys [iri]}]
  (last (str/split iri #"[#/]")))

(defn- own-graphs
  "The test's statements moved to graphs of its own: {:stmts [...] :back
   {own-graph original-graph-or-nil}}."
  [t model]
  (let [originals (distinct (map #(.getContext ^Statement %) model))
        own (zipmap originals (map #(.createIRI vf (str "urn:x-w3c-test:" (test-id t) "/" %))
                                   (range)))]
    {:stmts (mapv (fn [^Statement s]
                    (.createStatement vf (.getSubject s) (.getPredicate s) (.getObject s)
                                      ^Resource (own (.getContext s))))
                  model)
     :back (into {} (map (fn [[orig g]] [g orig])) own)}))

(defn- read-back
  "The test's statements from the store, in their original graphs."
  [^RepositoryConnection conn back]
  (let [model (LinkedHashModel.)]
    (when (seq back)
      (with-open [it (.getStatements conn nil nil nil false
                                     ^"[Lorg.eclipse.rdf4j.model.Resource;" (into-array Resource (keys back)))]
        (doseq [^Statement s (iterator-seq it)]
          (.add ^java.util.Collection model
                (.createStatement vf (.getSubject s) (.getPredicate s) (.getObject s)
                                  ^Resource (back (.getContext s)))))))
    model))

(defn- format-check
  "Result of a loaded format test, from its statements read back."
  [dir {:keys [type result-file assumed-base]} actual]
  (if (format-positive-types type)
    (ok)
    (let [expected (parse-file dir result-file (base-iri dir result-file assumed-base))]
      (if (Models/isomorphic actual expected)
        (ok)
        (fail (str "store contents not isomorphic to the expected result. Actual "
                   (.size ^LinkedHashModel actual) " statements, expected " (.size expected)
                   "\n  actual:   " (pr-str (mapv str (take 4 actual)))
                   "\n  expected: " (pr-str (mapv str (take 4 expected)))))))))

(defn- prepare-format-test
  "Parses the test's input (a positive test that fails to parse fails here):
   {:test t :loaded {:stmts :back}} or {:test t :result failure}."
  [dir {:keys [action-file assumed-base] :as t}]
  (try
    {:test t :loaded (own-graphs t (parse-file dir action-file (base-iri dir action-file assumed-base)))}
    (catch Throwable e
      {:test t :result (fail (str "Exception: " (.getMessage e)))})))

(defn- load-and-check!
  "Loads the `prepared` tests in one commit and checks each: {test result}.
   When the store rejects the commit, loads and checks each test on its own."
  [^RepositoryConnection conn dir prepared]
  (let [load! (fn [entries]
                (in-transaction! conn
                  #(doseq [{:keys [loaded]} entries]
                     (.add conn ^Iterable (:stmts loaded) no-ctx))))
        check (fn [{:keys [test loaded]}]
                (try
                  (format-check dir test (read-back conn (:back loaded)))
                  (catch Throwable e (fail (str "Exception: " (.getMessage e))))))
        loadable (filterv :loaded prepared)
        batch-error (when (seq loadable)
                      (try (load! loadable) nil (catch Throwable e e)))]
    (into {} (for [p prepared]
               [(:test p)
                (cond
                  (:result p) (:result p)
                  (nil? batch-error) (check p)
                  :else (try (load! [p]) (check p)
                             (catch Throwable e (fail (str "Exception: " (.getMessage e))))))]))))

(defn- format-negative-syntax
  "The parser's verdict: nothing reaches the store (RDF4J parses before adding)."
  [dir {:keys [action-file assumed-base]}]
  (negative-syntax #(parse-file dir action-file (base-iri dir action-file assumed-base))))

(def ^:private format-loaded-types (into format-evaluation-types format-positive-types))

(defn- run-one
  "Runs one test that is not a loaded format test; `loaded` as in
   load-query-data!."
  [^RepositoryConnection conn dir {:keys [type] :as t} loaded]
  (try
    (cond
      (= type :query-evaluation) (query-evaluation conn dir t loaded)
      (= type :update-evaluation) (update-evaluation conn dir t loaded)
      (= type :PositiveSyntaxTest) (positive-query-syntax conn dir t)
      (= type :PositiveUpdateSyntaxTest) (positive-update-syntax conn dir t)
      (= type :NegativeSyntaxTest) (negative-syntax #(prepare-query conn dir t))
      (= type :NegativeUpdateSyntaxTest) (negative-syntax #(prepare-update conn dir t))
      (format-negative-types type) (format-negative-syntax dir t)
      :else (fail (str "unsupported test type " type)))
    (catch Throwable e
      (when (.isActive conn) (.rollback conn))
      (reset! loaded nil)
      (fail (str "Exception: " (.getMessage e))))))

(defn run-tests
  "Runs manifest entries of `dir`: {test {:pass? bool :message str}}. The
   format tests load in one commit, then the others run grouped by data."
  [repo dir tests]
  (with-open [^RepositoryConnection conn (.getConnection repo)]
    (let [{formats true others false} (group-by #(boolean (format-loaded-types (:type %))) tests)
          loaded (atom nil)]
      (merge (when (seq formats)
               (load-and-check! conn dir (mapv #(prepare-format-test dir %) formats)))
             (into {} (for [t (sort-by #(pr-str (data-key dir %)) others)]
                        [t (run-one conn dir t loaded)]))))))

(defn run-test
  "Runs one manifest entry; {:pass? bool :message str}."
  [repo dir t]
  (get (run-tests repo dir [t]) t))

(defn run-suite
  "Runs every test of `dir`/manifest.ttl. `expected-failures` maps test ids
   (see test-id) to the reason they fail. Reports through clojure.test;
   returns the counts."
  [repo dir expected-failures]
  (let [tests (manifest/parse-manifest (str dir "manifest.ttl"))
        results (run-tests repo dir tests)
        counts (atom {:total 0 :pass 0 :fail 0 :expected-fail 0 :unexpected-pass 0})]
    (doseq [{:keys [name] :as t} tests
            :let [id (test-id t)]]
      (let [{:keys [pass? message]} (results t)
            expected-fail? (contains? expected-failures id)
            outcome (cond (and pass? expected-fail?) :unexpected-pass
                          pass? :pass
                          expected-fail? :expected-fail
                          :else :fail)]
        (swap! counts #(-> % (update :total inc) (update outcome inc)))
        (testing (str dir id " (" name ")")
          (case outcome
            :fail (is false message)
            :unexpected-pass (is false (str "passes, but is listed in expected-failures ("
                                            (expected-failures id) "): remove it"))
            nil))))
    (doseq [n (keys expected-failures)
            :when (not-any? #(= n (test-id %)) tests)]
      (is false (str dir ": expected failure " (pr-str n) " is not a test in the manifest")))
    (let [{:keys [total pass expected-fail] :as c} @counts]
      (println (format "%-45s %3d tests: %3d pass, %3d expected failures" dir total pass expected-fail))
      c)))

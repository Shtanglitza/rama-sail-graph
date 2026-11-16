(ns rama-sail.migrate-test
  "rama-sail.migrate: rewriting subject-position triple terms to reifiers.
   Pure rewrite tests, then the whole migration on an IPC store."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.bulk :as bulk]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.migrate :as m]
            [rama-sail.module.indexer :as idx]))

(def module-name (rama/get-module-name RdfStorageModule))
(def ^:dynamic *ipc* nil)

(use-fixtures :once (fn [f]
                      (with-open [ipc (rtest/create-ipc)]
                        (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
                        (binding [*ipc* ipc] (f)))))

(def a "<http://ex/a>")
(def knows "<http://ex/knows>")
(def b "<http://ex/b>")
(def src "<http://ex/source>")
(def t1 (str "<< " a " " knows " " b " >>"))
(def g1 "<http://ex/g1>")
(def g2 "<http://ex/g2>")

(deftest test-reifier
  (is (= (m/reifier t1) (m/reifier t1)) "the same term always gets the same reifier")
  (is (not= (m/reifier t1) (m/reifier (str "<< " a " " knows " <http://ex/c> >>"))))
  (is (re-matches #"_:reif-[0-9a-f]{32}" (m/reifier t1)) "a valid N-Triples blank node label"))

(deftest test-rewrite-term
  (testing "terms without a triple term in subject position are unchanged"
    (is (= ["<http://ex/x>" []] (m/rewrite-term "<http://ex/x>")))
    (is (= [t1 []] (m/rewrite-term t1)))
    (let [obj-nested (str "<< " a " " knows " " t1 " >>")]
      (is (= [obj-nested []] (m/rewrite-term obj-nested)) "a triple term may be an object")))
  (testing "a triple term in subject position is replaced by its reifier, innermost first"
    (let [r1 (m/reifier t1)
          n (str "<< " t1 " " src " <http://ex/carol> >>")
          n' (str "<< " r1 " " src " <http://ex/carol> >>")]
      (is (= [n' [[r1 t1]]] (m/rewrite-term n)))
      (let [r-n' (m/reifier n')]
        (is (= [(str "<< " r-n' " " src " \"x\" >>") [[r1 t1] [r-n' n']]]
               (m/rewrite-term (str "<< " n " " src " \"x\" >>"))))))))

(deftest test-rewrite-quad
  (let [r1 (m/reifier t1)]
    (is (nil? (m/rewrite-quad [a knows b g1])))
    (is (nil? (m/rewrite-quad ["<http://ex/x>" "<http://ex/claims>" t1 g1])) "triple-term object: readable")
    (is (= {:del [t1 src "<http://ex/hr>" g1]
            :add [[r1 src "<http://ex/hr>" g1] [r1 m/rdf-reifies t1 g1]]
            :nested? false}
           (m/rewrite-quad [t1 src "<http://ex/hr>" g1])))
    (let [n (str "<< " t1 " " src " <http://ex/carol> >>")
          n' (str "<< " r1 " " src " <http://ex/carol> >>")]
      (is (= {:del ["<http://ex/y>" "<http://ex/claims>" n g1]
              :add [["<http://ex/y>" "<http://ex/claims>" n' g1] [r1 m/rdf-reifies t1 g1]]
              :nested? true}
             (m/rewrite-quad ["<http://ex/y>" "<http://ex/claims>" n g1]))))))

(deftest test-commit-records
  (let [rewrites (keep m/rewrite-quad [[t1 src "\"1\"" g1] [t1 src "\"2\"" g1]])
        [[_ ops]] (m/commit-records rewrites)]
    (is (= [:add :add :add :del :del] (map first ops))
        "adds first; the shared rdf:reifies quad once")
    (with-redefs [idx/max-commit-ops 2]
      (is (= [2 2 1] (map (comp count second) (m/commit-records rewrites)))
          "records are split at max-commit-ops"))))

(defn- store []
  (set (rama/foreign-invoke-query (rama/foreign-query *ipc* module-name "find-triples") nil nil nil nil)))

(deftest test-migrate
  (let [carol "<http://ex/carol>"
        r1 (m/reifier t1)
        n (str "<< " t1 " " src " " carol " >>")      ;; nested: t1 as a subject inside a term
        n' (str "<< " r1 " " src " " carol " >>")
        r2 (m/reifier n')
        plain [[a knows b g1] ["<http://ex/x>" "<http://ex/claims>" t1 g1]]
        old [[t1 "<http://ex/confidence>" "\"0.9\"" g1]
             [t1 src carol g1]
             [t1 src "<http://ex/dave>" g2]
             ["<http://ex/y>" "<http://ex/claims>" n g1]
             [n "<http://ex/certainty>" "\"high\"" g1]]
        ;; enough distinct subject terms for several pages at :page-keys 7
        many (vec (for [i (range 30)]
                    [(str "<< <http://ex/s" i "> <http://ex/p> <http://ex/o> >>") "<http://ex/q>" "\"v\"" g2]))
        expected (into (set plain)
                       (concat [[r1 "<http://ex/confidence>" "\"0.9\"" g1]
                                [r1 src carol g1]
                                [r1 src "<http://ex/dave>" g2]
                                [r1 m/rdf-reifies t1 g1]
                                [r1 m/rdf-reifies t1 g2]
                                ["<http://ex/y>" "<http://ex/claims>" n' g1]
                                [r2 "<http://ex/certainty>" "\"high\"" g1]
                                [r2 m/rdf-reifies n' g1]]
                               (mapcat (fn [[t q v g]] [[(m/reifier t) q v g] [(m/reifier t) m/rdf-reifies t g]]) many)))]
    (bulk/load-quads! *ipc* module-name (concat plain old many))

    (testing "dry run counts without writing"
      (let [report (m/dry-run *ipc* module-name {:page-keys 7})]
        (is (= 35 (:quads report)))
        (is (= 32 (:reifiers report)) "r1, r2 and one per many-term")
        (is (= 2 (:nested report)))
        (is (= 5 (count (:samples report))))
        (is (= (set (concat plain old many)) (store)))))

    (testing "migrate! rewrites everything, then finds nothing left"
      (let [{:keys [rewritten remaining]} (m/migrate! *ipc* module-name {:page-keys 7})]
        (is (= 35 (:quads rewritten)))
        (is (zero? (:quads remaining)))
        (is (= expected (store)))))

    (testing "the indexes agree: the reifier is found as a subject, the term as an object"
      (let [ft (rama/foreign-query *ipc* module-name "find-triples")]
        (is (= #{[r1 "<http://ex/confidence>" "\"0.9\"" g1] [r1 src carol g1] [r1 src "<http://ex/dave>" g2]
                 [r1 m/rdf-reifies t1 g1] [r1 m/rdf-reifies t1 g2]}
               (set (rama/foreign-invoke-query ft r1 nil nil nil))))
        (is (= #{["<http://ex/x>" "<http://ex/claims>" t1 g1] [r1 m/rdf-reifies t1 g1] [r1 m/rdf-reifies t1 g2]}
               (set (rama/foreign-invoke-query ft nil nil t1 nil))))
        (is (empty? (rama/foreign-invoke-query ft t1 nil nil nil)) "no quad has t1 as subject any more")
        (is (= (count expected)
               (rama/foreign-invoke-query (rama/foreign-query *ipc* module-name "count-statements") nil)))))

    (testing "running it again changes nothing"
      (let [{:keys [rewritten remaining]} (m/migrate! *ipc* module-name {:page-keys 7})]
        (is (zero? (:quads rewritten)))
        (is (zero? (:quads remaining)))
        (is (= expected (store)))))))

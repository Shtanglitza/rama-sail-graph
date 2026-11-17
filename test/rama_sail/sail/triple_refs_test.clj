(ns rama-sail.sail.triple-refs-test
  "rama-sail.sail.triple-refs: triple-term patterns rewritten before
   compilation. Plan shapes; the results are in triple-term-join-test."
  (:require [clojure.test :refer [deftest is testing]]
            [rama-sail.sail.compilation :refer [tuple-expr->plan]]
            [rama-sail.sail.optimization :as opt]
            [rama-sail.sail.triple-refs :as tr])
  (:import (org.eclipse.rdf4j.query.algebra Join StatementPattern TripleRef Var)
           (org.eclipse.rdf4j.query.parser.sparql SPARQLParser)))

(defn- parse [q]
  (.getTupleExpr (.parseQuery (SPARQLParser.) (str "PREFIX ex: <http://ex/> " q) nil)))

(defn- plan [q] (tuple-expr->plan (tr/pair-triple-refs (parse q))))

(defn- nodes [p] (filter map? (tree-seq coll? seq p)))
(defn- ops [p] (set (keep :op (nodes p))))
(defn- bgps [p] (filter #(= :bgp (:op %)) (nodes p)))

(def abc "<< <http://ex/a> <http://ex/b> <http://ex/c> >>")

(def reifies "<http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies>")

(defn- optimized [q] (opt/optimize-plan (plan q)))

(deftest test-constant-term-is-an-index-lookup
  (testing "a constant term becomes the constant of the pattern that uses it"
    (let [p (plan "SELECT ?x WHERE { ?x ex:claims <<( ex:a ex:b ex:c )>> }")]
      (is (= [abc] (map (comp :o :pattern) (bgps p))))
      (is (not-any? #{:filter :bind} (ops p)) "no scan and filter")))
  (testing "after another pattern, too"
    (let [p (plan "SELECT ?x WHERE { ?x ex:q ?y . ex:z ex:claims <<( ex:a ex:b ex:c )>> }")]
      (is (= #{["?x" "?y"] ["<http://ex/z>" abc]} (set (map (comp (juxt :s :o) :pattern) (bgps p)))))))
  (testing "used by two patterns: the constant is not a join key"
    (let [p (plan "SELECT * WHERE { ?x ex:p <<( ex:a ex:b ex:c )>> . ?y ex:q <<( ex:a ex:b ex:c )>> }")]
      (is (= [abc abc] (map (comp :o :pattern) (bgps p))))
      (is (= [[]] (map :join-vars (filter #(= :join (:op %)) (nodes p)))))))
  (testing "nested: the inner constant makes the outer one constant"
    (let [p (plan "SELECT ?x WHERE { ?x ex:r <<( ex:p ex:q <<( ex:a ex:b ex:c )>> )>> }")]
      (is (= [(str "<< <http://ex/p> <http://ex/q> " abc " >>")] (map (comp :o :pattern) (bgps p))))))
  (testing "a reified triple: the term is the object of the rdf:reifies lookup"
    (let [p (plan "SELECT ?o WHERE { << ex:a ex:b ex:c >> ex:p ?o }")]
      (is (= #{[reifies abc] ["<http://ex/p>" "?o"]} (set (map (comp (juxt :p :o) :pattern) (bgps p)))))
      (is (not-any? #{:filter :bind} (ops p))))))

(deftest test-reified-triple-plans
  (testing "a constant triple: found by its term, then its reifiers' annotations looked up"
    (let [p (optimized "SELECT ?o WHERE { << ex:a ex:b ex:c >> ex:p ?o }")
          enrich (first (filter #(= :batch-enrich (:op %)) (nodes p)))]
      (is (= "<http://ex/p>" (:predicate enrich)))
      (is (= [[reifies abc]] (map (comp (juxt :p :o) :pattern) (bgps p))))))
  (testing "a selective annotation drives the join; rdf:reifies is looked up per reifier, not scanned"
    (let [p (optimized "SELECT ?y WHERE { << ex:a ex:b ?y >> ex:source ex:hr }")
          enrich (first (filter #(= :batch-enrich (:op %)) (nodes p)))]
      (is (= reifies (:predicate enrich)))
      (is (= [["<http://ex/source>" "<http://ex/hr>"]] (map (comp (juxt :p :o) :pattern) (bgps p))))
      (is (not (contains? (ops p) :join)))
      (is (some #(= :triple-object (:type %)) (nodes p)) "the term's part is bound after the lookup"))))

(deftest test-term-with-variables
  (testing "attached to the pattern that binds the term, not to its neighbour"
    (let [p (plan "SELECT ?a WHERE { ?a ex:knows ex:bob . ex:carol ex:claims <<( ?a ex:knows ex:bob )>> }")
          join (first (filter #(= :join (:op %)) (nodes p)))]
      (is (= ["?a"] (:join-vars join)) "the part bound from the term is a join key")
      (is (some #(= :triple-subject (:type %)) (nodes p)))
      (is (some #(= :is-triple (:type %)) (nodes p)) "non-triple objects are filtered out")))
  (testing "nested terms with variables are answered natively"
    (let [p (plan "SELECT * WHERE { ex:c ex:certain <<( <<( ?a ex:knows ?b )>> ex:source ?s )>> }")]
      (is (= 1 (count (bgps p))))
      (is (not (contains? (ops p) :join))))))

(deftest test-copy
  (let [q "SELECT ?a WHERE { ?a ex:knows ex:bob . ex:carol ex:claims <<( ?a ex:knows ex:bob )>> }"
        te (parse q)
        before (str te)]
    (tr/pair-triple-refs te)
    (is (= before (str te)) "the parsed query is left as it was (the fallback may need it)"))
  (let [te (parse "SELECT * WHERE { ?s ?p ?o }")]
    (is (identical? te (tr/pair-triple-refs te)) "no triple terms: nothing copied")))

(deftest test-unpaired-term
  (testing "a term that no pattern binds is left alone and not compiled"
    (let [te (Join. (TripleRef. (Var. "s") (Var. "p") (Var. "o") (Var. "t"))
                    (StatementPattern. (Var. "x") (Var. "y") (Var. "z")))
          paired (tr/pair-triple-refs te)]
      (is (tr/unpaired? paired))
      (is (thrown-with-msg? UnsupportedOperationException #"no other pattern binds"
                            (tuple-expr->plan paired))))))

(deftest test-triple-functions
  (is (= {:type :triple-subject :arg {:type :var :name "?t"}}
         (->> (nodes (plan "SELECT * WHERE { ?x ?y ?t BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#subject>(?t) AS ?s) }"))
              (some :bindings) first :expr)))
  (is (thrown? UnsupportedOperationException
               (plan "SELECT * WHERE { ?x ?y ?t BIND(<http://ex/fn>(?t) AS ?s) }"))
      "other functions still go to the fallback"))

(ns rama-sail.sail.triple-refs-test
  "rama-sail.sail.triple-refs: triple-term patterns rewritten before
   compilation. Plan shapes; the results are in triple-term-join-test."
  (:require [clojure.test :refer [deftest is testing]]
            [rama-sail.sail.compilation :refer [tuple-expr->plan]]
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

(deftest test-constant-term-is-an-index-lookup
  (testing "a constant term becomes the constant of the pattern that uses it"
    (let [p (plan "SELECT ?o WHERE { << ex:a ex:b ex:c >> ex:p ?o }")]
      (is (= [abc] (map (comp :s :pattern) (bgps p))))
      (is (not-any? #{:filter :bind} (ops p)) "no scan and filter")))
  (testing "after another pattern, too"
    (let [p (plan "SELECT ?x WHERE { ?x ex:q ?y . ex:z ex:claims << ex:a ex:b ex:c >> }")]
      (is (= #{["?x" "?y"] ["<http://ex/z>" abc]} (set (map (comp (juxt :s :o) :pattern) (bgps p)))))))
  (testing "used by two patterns: the constant is not a join key"
    (let [p (plan "SELECT * WHERE { << ex:a ex:b ex:c >> ex:p ?x ; ex:q ?y }")]
      (is (= [abc abc] (map (comp :s :pattern) (bgps p))))
      (is (= [[]] (map :join-vars (filter #(= :join (:op %)) (nodes p)))))))
  (testing "nested: the inner constant makes the outer one constant"
    (let [p (plan "SELECT ?o WHERE { << << ex:a ex:b ex:c >> ex:p ex:q >> ex:r ?o }")]
      (is (= [(str "<< " abc " <http://ex/p> <http://ex/q> >>")] (map (comp :s :pattern) (bgps p)))))))

(deftest test-term-with-variables
  (testing "attached to the pattern that binds the term, not to its neighbour"
    (let [p (plan "SELECT ?a WHERE { ?a ex:knows ex:bob . ex:carol ex:claims << ?a ex:knows ex:bob >> }")
          join (first (filter #(= :join (:op %)) (nodes p)))]
      (is (= ["?a"] (:join-vars join)) "the part bound from the term is a join key")
      (is (some #(= :triple-subject (:type %)) (nodes p)))
      (is (some #(= :is-triple (:type %)) (nodes p)) "non-triple objects are filtered out")))
  (testing "nested terms with variables are answered natively"
    (let [p (plan "SELECT * WHERE { << << ?a ex:knows ?b >> ex:source ?s >> ex:certainty ?c }")]
      (is (= 1 (count (bgps p))))
      (is (not (contains? (ops p) :join))))))

(deftest test-copy
  (let [q "SELECT ?a WHERE { ?a ex:knows ex:bob . ex:carol ex:claims << ?a ex:knows ex:bob >> }"
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

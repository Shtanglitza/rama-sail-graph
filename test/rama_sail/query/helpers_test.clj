(ns rama-sail.query.helpers-test
  "Pure unit tests (no IPC) for hash-join compatibility semantics and
   ORDER BY sort keys."
  (:require [clojure.test :refer [deftest is testing]]
            [rama-sail.query.helpers :as qh]))

;;; ---------------------------------------------------------------------------
;;; SPARQL solution compatibility
;;; ---------------------------------------------------------------------------

(deftest test-compatible-bindings
  (testing "bindings agreeing on shared vars are compatible"
    (is (true? (qh/compatible-bindings? {"?x" "<a>" "?y" "<b>"}
                                        {"?x" "<a>" "?z" "<c>"}))))
  (testing "bindings disagreeing on a shared var are incompatible"
    (is (false? (qh/compatible-bindings? {"?x" "<a>"}
                                         {"?x" "<b>"}))))
  (testing "a var bound on only one side is compatible with anything"
    (is (true? (qh/compatible-bindings? {"?x" "<a>"}
                                        {"?y" "<b>"})))
    (is (true? (qh/compatible-bindings? {} {"?y" "<b>"})))))

;;; ---------------------------------------------------------------------------
;;; Hash join with unbound join variables
;;; ---------------------------------------------------------------------------

(deftest test-hash-join-basic
  (testing "rows join on equal key values and merge"
    (let [idx (qh/build-hash-index [{"?x" "<a>" "?y" "<b>"}
                                    {"?x" "<c>" "?y" "<d>"}]
                                   ["?x"])
          matches (qh/probe-hash-index idx {"?x" "<a>" "?z" "<e>"} ["?x"])]
      (is (= [{"?x" "<a>" "?y" "<b>" "?z" "<e>"}] (vec matches)))))
  (testing "no key match returns nil"
    (let [idx (qh/build-hash-index [{"?x" "<a>"}] ["?x"])]
      (is (nil? (qh/probe-hash-index idx {"?x" "<zzz>"} ["?x"]))))))

(deftest test-hash-join-unbound-build-side
  (testing "a build-side row missing the join var joins with every probe row"
    ;; SPARQL compatibility: an unbound variable is compatible with anything.
    ;; Such rows come from OPTIONAL/UNION sides feeding a join.
    (let [idx (qh/build-hash-index [{"?y" "<opt>"}          ;; ?x unbound
                                    {"?x" "<a>" "?y" "<b>"}]
                                   ["?x"])
          matches (qh/probe-hash-index idx {"?x" "<a>"} ["?x"])]
      (is (= #{{"?x" "<a>" "?y" "<b>"}
               {"?x" "<a>" "?y" "<opt>"}}
             (set matches))
          "Probe must see both the keyed match and the wildcard row"))))

(deftest test-hash-join-unbound-probe-side
  (testing "a probe row missing the join var considers all indexed rows"
    (let [idx (qh/build-hash-index [{"?x" "<a>" "?y" "<b>"}
                                    {"?x" "<c>" "?y" "<d>"}]
                                   ["?x"])
          matches (qh/probe-hash-index idx {"?z" "<e>"} ["?x"])]
      (is (= 2 (count matches))
          "Unbound probe joins with every indexed row"))))

(deftest test-hash-join-compatibility-on-non-key-vars
  (testing "shared non-key vars must agree even though they are not join keys"
    (let [idx (qh/build-hash-index [{"?x" "<a>" "?y" "<b>"}] ["?x"])]
      (is (nil? (qh/probe-hash-index idx {"?x" "<a>" "?y" "<DIFFERENT>"} ["?x"]))
          "Disagreement on shared ?y must reject the merge")
      (is (= 1 (count (qh/probe-hash-index idx {"?x" "<a>" "?y" "<b>"} ["?x"])))
          "Agreement on shared ?y must accept the merge"))))

(deftest test-left-join-unbound-join-var
  (testing "left row with unbound join var still matches compatible right rows"
    (let [idx (qh/build-hash-index [{"?x" "<a>" "?y" "<b>"}] ["?x"])
          result (qh/apply-left-join-with-condition idx {"?z" "<e>"} ["?x"] nil)]
      (is (= [{"?x" "<a>" "?y" "<b>" "?z" "<e>"}] (vec result))
          "Row must be joined, not passed through as unmatched")))
  (testing "left row with no compatible right rows passes through unchanged"
    (let [idx (qh/build-hash-index [{"?x" "<a>"}] ["?x"])
          result (qh/apply-left-join-with-condition idx {"?x" "<zzz>"} ["?x"] nil)]
      (is (= [{"?x" "<zzz>"}] (vec result))))))

(deftest test-multi-left-join-applies-optionals-in-idx-order
  ;; The topology collects optionals in completion order; OPTIONALs sharing a
  ;; variable do not commute, so they must be applied in :idx (query) order.
  (let [opt (fn [i rows] {:idx i :hash-idx (qh/build-hash-index rows []) :join-vars [] :condition nil})
        first-opt (opt 0 [{"?a" "1" "?c" "<c>"}])
        second-opt (opt 1 [{"?a" "<n1>" "?e" "10"}])
        expected [{"?a" "1" "?c" "<c>"}]]
    (is (= expected (qh/apply-multi-left-join [{}] [first-opt second-opt])))
    (is (= expected (qh/apply-multi-left-join [{}] [second-opt first-opt]))
        "completion order must not matter")))

;;; ---------------------------------------------------------------------------
;;; ORDER BY sort keys — mixed-type totality
;;; ---------------------------------------------------------------------------

(defn- sort-values
  "Sort serialized term values via compute-sort-key/sort-keyed-rows."
  [values ascending]
  (let [specs [{:expr {:type :var :name "?v"} :ascending ascending}]
        keyed (map (fn [v] [(qh/compute-sort-key {"?v" v} specs) {"?v" v}])
                   values)]
    (map #(get-in % [1 "?v"]) (qh/sort-keyed-rows keyed specs))))

(deftest test-sort-key-mixed-types-total
  (testing "mixed numeric/string/IRI/bnode column sorts without throwing"
    ;; Regression: this threw ClassCastException (compare Double vs String)
    (let [values ["\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>"
                  "\"abc\""
                  "<http://ex/iri>"
                  "_:b1"
                  "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"]
          sorted (vec (sort-values values true))]
      (is (= ["_:b1"
              "<http://ex/iri>"
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"
              "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>"
              "\"abc\""]
             sorted)
          "bnodes < IRIs < numeric literals (numeric order) < other literals"))))

(deftest test-sort-key-descending-reverses-everything
  (testing "DESC reverses both type rank and within-type order"
    (let [values ["\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>"
                  "\"abc\""
                  "<http://ex/iri>"
                  "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"]
          sorted (vec (sort-values values false))]
      (is (= ["\"abc\""
              "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>"
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"
              "<http://ex/iri>"]
             sorted)))))

(deftest test-sort-key-null-placement
  (testing "unbound sorts first ASC and last DESC"
    (let [rows [{"?v" "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"} {}]
          specs-asc [{:expr {:type :var :name "?v"} :ascending true}]
          specs-desc [{:expr {:type :var :name "?v"} :ascending false}]
          sort-rows (fn [specs]
                      (map second
                           (qh/sort-keyed-rows
                            (map (fn [r] [(qh/compute-sort-key r specs) r]) rows)
                            specs)))]
      (is (= [{} {"?v" "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"}]
             (vec (sort-rows specs-asc))))
      (is (= [{"?v" "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"} {}]
             (vec (sort-rows specs-desc)))))))

;;; ---------------------------------------------------------------------------
;;; Self-join pair generation (W1: reflexive ?a = ?b pairs)
;;; ---------------------------------------------------------------------------

(defn- sj-pairs
  "Run process-subject-groups over `groups` and return the [left right]
   subject pairs (a seq, so multiplicity is observable)."
  [groups filter-spec & [limit]]
  (->> (qh/process-subject-groups groups "?a" "?b" "?o" filter-spec limit)
       (map (juxt #(get % "?a") #(get % "?b")))))

(deftest test-self-join-keeps-reflexive-pairs
  (let [groups {"\"NYC\"" ["<x>" "<y>"]
                "\"LA\"" ["<z>"]}]
    (testing "no filter: every triple joins with itself (n² pairs per group)"
      (is (= #{["<x>" "<x>"] ["<x>" "<y>"] ["<y>" "<x>"] ["<y>" "<y>"] ["<z>" "<z>"]}
             (set (sj-pairs groups nil))))
      (is (= 5 (count (sj-pairs groups nil)))))
    (testing "a predicate whose objects are all unique still yields one row per triple"
      (is (= #{["<p>" "<p>"] ["<q>" "<q>"]}
             (set (sj-pairs {"\"1\"" ["<p>"] "\"2\"" ["<q>"]} nil)))))
    (testing ":ne rejects only the equal pair"
      (is (= #{["<x>" "<y>"] ["<y>" "<x>"]}
             (set (sj-pairs groups {:op :ne :left "?a" :right "?b"})))))
    (testing "< <= > >= between IRIs are a type error (strict SPARQL): no pairs"
      (doseq [op [:lt :le :gt :ge]]
        (is (empty? (sj-pairs groups {:op op :left "?a" :right "?b"})) (str op))))
    (testing "result-limit path agrees with the unlimited path"
      (is (= 5 (count (sj-pairs groups nil 100))))
      (is (= 3 (count (sj-pairs groups nil 3))))
      (is (= 1 (count (sj-pairs {"\"LA\"" ["<z>"]} nil 10)))
          "singleton groups count toward the limit too"))))

;;; ---------------------------------------------------------------------------
;;; DISTINCT order preservation (W2) / GRAPH ?g scope (W3)
;;; ---------------------------------------------------------------------------

(deftest test-distinct-preserving-order
  (let [rows [{"?n" "\"Eve\""} {"?n" "\"Carol\""} {"?n" "\"Eve\""}
              {"?n" "\"Bob\""} {"?n" "\"Carol\""} {"?n" "\"Alice\""}]]
    (is (= [{"?n" "\"Eve\""} {"?n" "\"Carol\""} {"?n" "\"Bob\""} {"?n" "\"Alice\""}]
           (qh/distinct-preserving-order rows))
        "first occurrence of each solution, in input (ORDER BY) order")
    (is (= [] (qh/distinct-preserving-order [])))
    (is (vector? (qh/distinct-preserving-order (seq rows))))))

(deftest test-drop-default-graph-quads
  (let [dc "::default::"
        quads [["<s1>" "<p>" "<o>" dc]
               ["<s2>" "<p>" "<o>" "<g1>"]
               ["<s3>" "<p>" "<o>" dc]
               ["<s4>" "<p>" "<o>" "<g2>"]]]
    (is (= [["<s2>" "<p>" "<o>" "<g1>"] ["<s4>" "<p>" "<o>" "<g2>"]]
           (qh/drop-default-graph-quads quads dc)))
    (is (= [] (qh/drop-default-graph-quads [] dc)))))

;;; ---------------------------------------------------------------------------
;;; Self-join ordering filters match the expression engine (bnodes / triple terms)
;;; ---------------------------------------------------------------------------

(deftest test-self-join-ordering-is-strict-sparql
  ;; Same comparator as the expression engine (expr/compare-terms): subjects
  ;; are IRIs, blank nodes or triple terms, none of which is order-comparable,
  ;; so an ordering FILTER keeps no pair (BSBM QJ4's ?product1 < ?product2
  ;; returns no rows, as in RDF4J). != is RDFterm-equal.
  (let [f (fn [op] {:op op :left "?a" :right "?b"})
        ok? (fn [op s1 s2] (qh/apply-inequality-filter s1 s2 (f op) "?a" "?b"))]
    (testing "IRIs, blank nodes and triple terms never satisfy an ordering"
      (doseq [op [:lt :le :gt :ge]
              [s1 s2] [["<http://ex/a>" "<http://ex/b>"] ["<http://ex/a>" "<http://ex/a>"]
                       ["_:b1" "_:b2"] ["<http://ex/a>" "_:b1"] ["_:b1" "<http://ex/a>"]
                       ["<< <http://ex/s> <http://ex/p> <http://ex/o> >>" "<http://ex/a>"]]]
        (is (false? (ok? op s1 s2)) (str op " " s1 " " s2))))
    (testing "!= is plain term inequality, bnodes included"
      (is (true? (ok? :ne "_:b1" "_:b2")))
      (is (false? (ok? :ne "_:b1" "_:b1")))
      (is (true? (ok? :ne "<http://ex/a>" "_:b1")))))
  (testing "STR(?a) op STR(?b): subjects compared by their STR (the IRI string)"
    (let [g {"\"X\"" ["<http://ex/i2>" "<http://ex/i10>" "<http://ex/i1>"]}
          pairs (fn [op] (set (sj-pairs g {:op op :left "?a" :right "?b" :str? true})))]
      (is (= #{["<http://ex/i1>" "<http://ex/i10>"] ["<http://ex/i1>" "<http://ex/i2>"]
               ["<http://ex/i10>" "<http://ex/i2>"]}
             (pairs :lt))
          "codepoint order of the IRI strings: i1 < i10 < i2")
      (is (= #{["<http://ex/i10>" "<http://ex/i1>"] ["<http://ex/i2>" "<http://ex/i1>"]
               ["<http://ex/i2>" "<http://ex/i10>"]}
             (pairs :gt)))
      (is (= 6 (count (pairs :le))) "3 strict + 3 reflexive")
      (let [iri-str (fn [s] (subs s 1 (dec (count s))))]
        (is (= (pairs :lt) (set (filter (fn [[a b]] (neg? (compare (iri-str a) (iri-str b)))) (pairs :ne))))
            "fast path agrees with filtering the general path by IRI string"))))
  (testing "pair generation, sorted fast path included"
    (let [groups {"\"X\"" ["_:b1" "<http://ex/i1>" "_:b2" "<http://ex/i2>"]}]
      (doseq [op [:lt :le :gt :ge]]
        (is (empty? (sj-pairs groups {:op op :left "?a" :right "?b"})) (str op)))
      (is (= 12 (count (sj-pairs groups {:op :ne :left "?a" :right "?b"})))))))

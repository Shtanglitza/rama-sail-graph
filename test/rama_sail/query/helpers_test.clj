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

;;; ---------------------------------------------------------------------------
;;; ORDER BY sort keys — mixed-type totality
;;; ---------------------------------------------------------------------------

(defn- sort-values
  "Sort serialized term values via compute-sort-key/sort-keyed-rows."
  [values ascending]
  (let [specs [{:expr {:type :var :name "?v"} :ascending ascending}]
        keyed (map (fn [v] [(qh/compute-sort-key {"?v" v} specs) {"?v" v}])
                   values)]
    (map #(get-in % [1 "?v"]) (qh/sort-keyed-rows keyed))))

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
                            (map (fn [r] [(qh/compute-sort-key r specs) r]) rows))))]
      (is (= [{} {"?v" "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"}]
             (vec (sort-rows specs-asc))))
      (is (= [{"?v" "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"} {}]
             (vec (sort-rows specs-desc)))))))

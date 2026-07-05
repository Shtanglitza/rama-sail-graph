(ns rama-sail.query.expr-test
  "Pure unit tests (no IPC) for the SPARQL expression engine: term parsing,
   the datatype-aware numeric tower, three-valued logic, and aggregation."
  (:require [clojure.test :refer [deftest is testing]]
            [rama-sail.query.expr :as e]
            [rama-sail.query.aggregation :as agg]))

(def ^:private xsd-int "^^<http://www.w3.org/2001/XMLSchema#integer>")
(def ^:private xsd-dec "^^<http://www.w3.org/2001/XMLSchema#decimal>")
(def ^:private xsd-dbl "^^<http://www.w3.org/2001/XMLSchema#double>")
(def ^:private xsd-bool "^^<http://www.w3.org/2001/XMLSchema#boolean>")

(defn- v [name] {:type :var :name name})
(defn- c [val] {:type :const :val val})
(defn- cmp* [op l r] {:type :cmp :op op :left l :right r})

;;; ---------------------------------------------------------------------------
;;; parse-term
;;; ---------------------------------------------------------------------------

(deftest test-parse-term
  (testing "literal with @ inside the lexical value"
    (is (= {:kind :literal :lexical "user@example.com" :lang nil :datatype nil}
           (e/parse-term "\"user@example.com\""))))
  (testing "literal with ^^ inside the lexical value"
    (is (= {:kind :literal :lexical "a^^b" :lang nil :datatype nil}
           (e/parse-term "\"a^^b\""))))
  (testing "language-tagged literal whose value contains @"
    (is (= {:kind :literal :lexical "a@b" :lang "en" :datatype nil}
           (e/parse-term "\"a@b\"@en"))))
  (testing "typed literal"
    (is (= {:kind :literal :lexical "5" :lang nil
            :datatype "http://www.w3.org/2001/XMLSchema#integer"}
           (e/parse-term (str "\"5\"" xsd-int)))))
  (testing "escaped quote inside literal"
    (is (= "say \\\"hi\\\"" (:lexical (e/parse-term "\"say \\\"hi\\\"\"@en"))))
    (is (= "en" (:lang (e/parse-term "\"say \\\"hi\\\"\"@en")))))
  (testing "IRI, bnode, bare token"
    (is (= {:kind :iri :value "http://ex/a"} (e/parse-term "<http://ex/a>")))
    (is (= {:kind :bnode :id "b1"} (e/parse-term "_:b1")))
    (is (= {:kind :other :value "20"} (e/parse-term "20")))))

;;; ---------------------------------------------------------------------------
;;; STR / LANG / DATATYPE — the @/^^ corruption family
;;; ---------------------------------------------------------------------------

(deftest test-str-function
  (testing "STR of a literal containing @ returns the full value"
    ;; Regression: used to return "use" for "user@example.com"
    (is (= "\"user@example.com\""
           (e/eval-expr {:type :str :arg (v "?x")} {"?x" "\"user@example.com\""}))))
  (testing "STR of a literal containing ^^ returns the full value"
    (is (= "\"a^^b\""
           (e/eval-expr {:type :str :arg (v "?x")} {"?x" "\"a^^b\""}))))
  (testing "STR strips language tag and datatype"
    (is (= "\"chat\"" (e/eval-expr {:type :str :arg (c "\"chat\"@fr")} {})))
    (is (= "\"5\"" (e/eval-expr {:type :str :arg (c (str "\"5\"" xsd-int))} {}))))
  (testing "STR of an IRI"
    (is (= "\"http://ex/a\"" (e/eval-expr {:type :str :arg (c "<http://ex/a>")} {}))))
  (testing "STR of unbound is an error"
    (is (e/error? (e/eval-expr {:type :str :arg (v "?missing")} {})))))

(deftest test-lang-function
  (testing "LANG of untagged literal containing @ is empty, not garbage"
    ;; Regression: "a@b" used to yield a malformed literal claiming language b"
    (is (= "\"\"" (e/eval-expr {:type :lang :arg (c "\"a@b\"")} {}))))
  (testing "LANG of tagged literal"
    (is (= "\"en\"" (e/eval-expr {:type :lang :arg (c "\"hi\"@en")} {}))))
  (testing "LANG of an IRI is an error"
    (is (e/error? (e/eval-expr {:type :lang :arg (c "<http://ex/a>")} {})))))

(deftest test-datatype-function
  (testing "DATATYPE of literal containing ^^ in its value is xsd:string"
    (is (= "<http://www.w3.org/2001/XMLSchema#string>"
           (e/eval-expr {:type :datatype :arg (c "\"a^^b\"")} {}))))
  (testing "DATATYPE of typed and tagged literals"
    (is (= "<http://www.w3.org/2001/XMLSchema#integer>"
           (e/eval-expr {:type :datatype :arg (c (str "\"5\"" xsd-int))} {})))
    (is (= "<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>"
           (e/eval-expr {:type :datatype :arg (c "\"hi\"@en")} {})))))

;;; ---------------------------------------------------------------------------
;;; Numeric tower
;;; ---------------------------------------------------------------------------

(deftest test-large-integer-precision
  (testing "integers above 2^53 no longer compare equal"
    ;; Regression: both parsed to the same Double
    (let [a (str "\"9007199254740993\"" xsd-int)
          b (str "\"9007199254740992\"" xsd-int)]
      (is (false? (e/eval-expr (cmp* :eq (c a) (c b)) {})))
      (is (true? (e/eval-expr (cmp* :ne (c a) (c b)) {})))
      (is (true? (e/eval-expr (cmp* :gt (c a) (c b)) {}))))))

(deftest test-decimal-precision
  (testing "decimals compare exactly"
    (let [a (str "\"0.1\"" xsd-dec)
          b (str "\"0.10\"" xsd-dec)]
      (is (true? (e/eval-expr (cmp* :eq (c a) (c b)) {}))))))

(deftest test-plain-strings-are-not-numeric
  (testing "plain literals compare as strings per SPARQL"
    ;; "10" < "9" lexically (was numeric before: 10 > 9)
    (is (true? (e/eval-expr (cmp* :lt (c "\"10\"") (c "\"9\"")) {}))))
  (testing "typed integers still compare numerically"
    (is (false? (e/eval-expr (cmp* :lt (c (str "\"10\"" xsd-int))
                                   (c (str "\"9\"" xsd-int))) {}))))
  (testing "bare internal-API tokens keep numeric leniency"
    (is (true? (e/eval-expr (cmp* :gt (c "10") (c "9")) {})))))

(deftest test-mixed-type-promotion
  (testing "integer vs decimal vs double compare correctly"
    (is (true? (e/eval-expr (cmp* :eq (c (str "\"1\"" xsd-int))
                                  (c (str "\"1.0\"" xsd-dec))) {})))
    (is (true? (e/eval-expr (cmp* :lt (c (str "\"1\"" xsd-int))
                                  (c (str "\"1.5\"" xsd-dbl))) {})))))

(deftest test-arithmetic-typing
  (testing "integer + integer is xsd:integer"
    (is (= (str "\"2\"" xsd-int)
           (e/eval-expr {:type :math :op :plus
                         :left (c (str "\"1\"" xsd-int))
                         :right (c (str "\"1\"" xsd-int))} {}))))
  (testing "integer / integer is xsd:decimal"
    (is (= (str "\"0.5\"" xsd-dec)
           (e/eval-expr {:type :math :op :divide
                         :left (c (str "\"1\"" xsd-int))
                         :right (c (str "\"2\"" xsd-int))} {}))))
  (testing "integer overflow promotes instead of wrapping"
    (is (= (str "\"18446744073709551614\"" xsd-int)
           (e/eval-expr {:type :math :op :plus
                         :left (c (str "\"9223372036854775807\"" xsd-int))
                         :right (c (str "\"9223372036854775807\"" xsd-int))} {}))))
  (testing "exact division by zero is an error; double division is INF"
    (is (e/error? (e/eval-expr {:type :math :op :divide
                                :left (c (str "\"1\"" xsd-int))
                                :right (c (str "\"0\"" xsd-int))} {})))
    (is (= (str "\"INF\"" xsd-dbl)
           (e/eval-expr {:type :math :op :divide
                         :left (c (str "\"1.0\"" xsd-dbl))
                         :right (c (str "\"0.0\"" xsd-dbl))} {}))))
  (testing "non-numeric operand is an error"
    (is (e/error? (e/eval-expr {:type :math :op :plus
                                :left (c "\"abc\"")
                                :right (c (str "\"1\"" xsd-int))} {})))))

;;; ---------------------------------------------------------------------------
;;; EBV and three-valued logic
;;; ---------------------------------------------------------------------------

(deftest test-effective-boolean-value
  (testing "plain string \"0\" is TRUE (non-empty string, not numeric zero)"
    ;; Regression: parse-numeric treated it as 0.0 → false
    (is (true? (e/sparql-ebv "\"0\""))))
  (testing "empty string is false, typed zero is false"
    (is (false? (e/sparql-ebv "\"\"")))
    (is (false? (e/sparql-ebv (str "\"0\"" xsd-int)))))
  (testing "boolean lexical forms including \"1\""
    (is (true? (e/sparql-ebv (str "\"true\"" xsd-bool))))
    (is (true? (e/sparql-ebv (str "\"1\"" xsd-bool))))
    (is (false? (e/sparql-ebv (str "\"false\"" xsd-bool)))))
  (testing "IRI and unbound are type errors"
    (is (e/error? (e/sparql-ebv "<http://ex/a>")))
    (is (e/error? (e/sparql-ebv nil)))))

(deftest test-three-valued-logic
  (testing "negation over a type error stays an error → filter excludes"
    ;; Regression: FILTER(!(?x = "a")) with unbound ?x used to INCLUDE the row
    (let [not-eq {:type :logic :op :not :arg (cmp* :eq (v "?x") (c "\"a\""))}]
      (is (e/error? (e/eval-expr not-eq {})))
      (is (false? (e/evaluate-filter-cond not-eq {})))))
  (testing "false AND error = false; true OR error = true"
    (let [err (cmp* :eq (v "?missing") (c "\"a\""))
          f (c "\"\"")
          t (c "\"x\"")]
      (is (false? (e/eval-expr {:type :logic :op :and :left f :right err} {})))
      (is (true? (e/eval-expr {:type :logic :op :or :left t :right err} {})))
      (is (e/error? (e/eval-expr {:type :logic :op :and :left t :right err} {})))
      (is (e/error? (e/eval-expr {:type :logic :op :or :left f :right err} {}))))))

(deftest test-coalesce-keeps-false
  (testing "COALESCE(BOUND(?x), \"fallback\") returns false, not the fallback"
    ;; Regression: `some` treated false as absence
    (is (false? (e/eval-expr {:type :coalesce
                              :args [{:type :bound :var "?x"} (c "\"fallback\"")]}
                             {}))))
  (testing "COALESCE skips unbound and errors"
    (is (= "\"fallback\""
           (e/eval-expr {:type :coalesce
                         :args [(v "?missing")
                                (cmp* :eq (v "?missing") (c "\"a\""))
                                (c "\"fallback\"")]}
                        {})))))

;;; ---------------------------------------------------------------------------
;;; Regex safety
;;; ---------------------------------------------------------------------------

(deftest test-regex
  (testing "invalid pattern degrades to filter-false instead of throwing"
    ;; Regression: PatternSyntaxException aborted the whole query
    (let [bad {:type :regex :arg (c "\"abc\"") :pattern (c "\"(\"")}]
      (is (e/error? (e/eval-expr bad {})))
      (is (false? (e/evaluate-filter-cond bad {})))))
  (testing "matching and case-insensitive flag"
    (is (true? (e/eval-expr {:type :regex :arg (c "\"Hello\"")
                             :pattern (c "\"^h\"") :flags (c "\"i\"")} {})))
    (is (false? (e/eval-expr {:type :regex :arg (c "\"Hello\"")
                              :pattern (c "\"^h\"")} {})))))

;;; ---------------------------------------------------------------------------
;;; sameTerm / IN
;;; ---------------------------------------------------------------------------

(deftest test-same-term-and-in
  (testing "sameTerm with unbound argument is an error"
    (is (e/error? (e/eval-expr {:type :same-term :left (v "?x") :right (c "\"a\"")} {}))))
  (testing "IN uses value equality across numeric types"
    (is (true? (e/eval-expr {:type :in :arg (c (str "\"1\"" xsd-int))
                             :set [(c (str "\"1.0\"" xsd-dec))]} {})))
    (is (false? (e/eval-expr {:type :in :arg (c (str "\"2\"" xsd-int))
                              :set [(c (str "\"1\"" xsd-int))]} {})))))

;;; ---------------------------------------------------------------------------
;;; Aggregation
;;; ---------------------------------------------------------------------------

(defn- run-agg
  "Fold rows through the aggregate machinery and return the formatted result."
  [agg-fn distinct? arg rows]
  (let [spec {:fn agg-fn :distinct distinct? :arg arg}
        state (reduce (fn [st row]
                        (agg/update-agg-state
                         agg-fn st
                         (agg/extract-value-for-agg row {:arg arg})))
                      (agg/init-agg-state agg-fn distinct?)
                      rows)]
    (agg/format-agg-result agg-fn (agg/compute-final-agg agg-fn state))))

(deftest test-count-distinct-star
  (testing "COUNT(DISTINCT *) counts distinct rows, not 1"
    ;; Regression: all rows collapsed onto a shared ::row marker
    (is (= (str "\"3\"" xsd-int)
           (run-agg :count true nil
                    [{"?x" "<a>"} {"?x" "<b>"} {"?x" "<c>"} {"?x" "<a>"}]))))
  (testing "COUNT(*) counts all rows including duplicates"
    (is (= (str "\"4\"" xsd-int)
           (run-agg :count false nil
                    [{"?x" "<a>"} {"?x" "<b>"} {"?x" "<c>"} {"?x" "<a>"}])))))

(deftest test-sum-exactness-and-overflow
  (testing "SUM of large longs does not overflow or crash formatting"
    ;; Regression: (long 1.8e19) threw IllegalArgumentException
    (let [big (str "\"9223372036854775807\"" xsd-int)]
      (is (= (str "\"18446744073709551614\"" xsd-int)
             (run-agg :sum false (v "?x") [{"?x" big} {"?x" big}])))))
  (testing "SUM of integers stays integer"
    (is (= (str "\"6\"" xsd-int)
           (run-agg :sum false (v "?x")
                    [{"?x" (str "\"1\"" xsd-int)}
                     {"?x" (str "\"2\"" xsd-int)}
                     {"?x" (str "\"3\"" xsd-int)}])))))

(deftest test-avg-exact-decimal
  (testing "AVG of integers is an exact decimal without float artifacts"
    ;; 0.1+0.2+0.3 as decimals: no 0.30000000000000004
    (is (= (str "\"0.2\"" xsd-dec)
           (run-agg :avg false (v "?x")
                    [{"?x" (str "\"0.1\"" xsd-dec)}
                     {"?x" (str "\"0.2\"" xsd-dec)}
                     {"?x" (str "\"0.3\"" xsd-dec)}])))
    (is (= (str "\"1.5\"" xsd-dec)
           (run-agg :avg false (v "?x")
                    [{"?x" (str "\"1\"" xsd-int)}
                     {"?x" (str "\"2\"" xsd-int)}])))))

(deftest test-min-max-datatype-aware
  (testing "MIN/MAX order numerically by datatype"
    (is (= (str "\"9\"" xsd-int)
           (run-agg :min false (v "?x")
                    [{"?x" (str "\"10\"" xsd-int)} {"?x" (str "\"9\"" xsd-int)}])))
    (is (= (str "\"10\"" xsd-int)
           (run-agg :max false (v "?x")
                    [{"?x" (str "\"10\"" xsd-int)} {"?x" (str "\"9\"" xsd-int)}])))))

(deftest test-distinct-merge
  (testing "distributed merge of DISTINCT states dedups across partitions"
    (let [s1 (-> (agg/init-agg-state :count true)
                 (as-> st (agg/update-agg-state :count st {"?x" "<a>"}))
                 (as-> st (agg/update-agg-state :count st {"?x" "<b>"})))
          s2 (-> (agg/init-agg-state :count true)
                 (as-> st (agg/update-agg-state :count st {"?x" "<b>"}))
                 (as-> st (agg/update-agg-state :count st {"?x" "<c>"})))
          merged (agg/merge-agg-states :count s1 s2)]
      (is (= 3 (agg/compute-final-agg :count merged))))))

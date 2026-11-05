(ns rama-sail.query.term-order-test
  "The shared term comparator (expr/terms-equal, expr/compare-terms,
   expr/order-key) and the bugs it fixes: W4 (REGEX on escaped text), W5
   (ORDER BY / MIN / MAX on serialized strings), W6 (boolean results vs
   boolean literals), W9 (locale-dependent number formatting). Pure — no IPC."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [rama-sail.query.aggregation :as agg]
            [rama-sail.query.expr :as expr]
            [rama-sail.query.helpers :as qh]
            [rama-sail.sail.serialization :as ser])
  (:import (java.util Locale)
           (org.eclipse.rdf4j.model.impl SimpleValueFactory)
           (org.eclipse.rdf4j.model.vocabulary XSD)))

(def ^:private xsd "http://www.w3.org/2001/XMLSchema#")
(defn- lit [lex dt] (str "\"" lex "\"^^<" xsd dt ">"))
(defn- int-lit [n] (lit n "integer"))
(def ^:private T (lit "true" "boolean"))
(def ^:private F (lit "false" "boolean"))

(defn- ev
  "Evaluate an expression map; ::error becomes :err for readable assertions."
  ([e] (ev e {}))
  ([e bindings] (let [v (expr/eval-expr e bindings)] (if (expr/error? v) :err v))))

(defn- c [v] {:type :const :val v})
(defn- cmp [op l r] (ev {:type :cmp :op op :left (c l) :right (c r)}))

;;; ---------------------------------------------------------------------------
;;; = != < <= > >= : every cell of a 27-term matrix against Apache Jena
;;; ---------------------------------------------------------------------------

(def ^:private oracle
  (edn/read-string (slurp (io/resource "term-comparison-oracle.edn"))))

(defn- strict-ordering-deviation?
  "Deliberate deviation from Jena (decided Nov 4, 2026: strict SPARQL, as
   RDF4J): ordering a term that has no SPARQL order (an IRI, an opaque
   literal) against ITSELF is a type error; Jena answers it as equal."
  [op a b]
  (and (#{:lt :le :gt :ge} op) (= a b) (not (expr/order-comparable? a))))

(deftest test-comparison-matrix-matches-jena
  (let [{:keys [terms ops]} oracle
        decode {\t true \f false \e :err}]
    (doseq [[op rows] ops
            i (range (count terms))
            j (range (count terms))
            :let [a (terms i) b (terms j)
                  expected (if (strict-ordering-deviation? op a b)
                             :err
                             (decode (nth (rows i) j)))]]
      (is (= expected (cmp op a b)) (str a " " (name op) " " b)))))

;;; ---------------------------------------------------------------------------
;;; Decision 1: ordering IRIs is a type error (strict SPARQL, as RDF4J)
;;; ---------------------------------------------------------------------------

(deftest test-iris-are-not-order-comparable
  (doseq [op [:lt :le :gt :ge]]
    (is (= :err (cmp op "<http://ex/a>" "<http://ex/b>")))
    (is (= :err (cmp op "<http://ex/a>" "<http://ex/a>")))
    (is (= :err (cmp op "_:b1" "_:b2"))))
  (is (false? (expr/order-comparable? "<http://ex/a>")))
  (is (true? (expr/order-comparable? (int-lit 1))))
  (testing "= / != on IRIs is term identity"
    (is (true? (cmp :eq "<http://ex/a>" "<http://ex/a>")))
    (is (false? (cmp :eq "<http://ex/a>" "<http://ex/b>")))
    (is (false? (cmp :eq "<http://ex/a>" "\"http://ex/a\"")))))

;;; ---------------------------------------------------------------------------
;;; Decision 2: literals of different known value classes are unequal (Jena)
;;; ---------------------------------------------------------------------------

(deftest test-cross-datatype-equality
  (testing "different known value classes: false (not a type error)"
    (is (false? (cmp :eq (int-lit 4) "\"a\"@en")))
    (is (true? (cmp :ne (int-lit 4) "\"a\"@en")))
    (is (false? (cmp :eq (int-lit 1) "\"1\"")))
    (is (false? (cmp :eq T "\"true\"")))
    (is (false? (cmp :eq (lit "2008-06-20" "date") (lit "2008-06-20T00:00:00Z" "dateTime")))))
  (testing "same value, different lexical form or datatype: true"
    (is (true? (cmp :eq (int-lit 1) (lit "1.0" "decimal"))))
    (is (true? (cmp :eq (int-lit 1) (lit "1.0E0" "double"))))
    (is (true? (cmp :eq "\"a\"" (lit "a" "string"))))
    (is (true? (cmp :eq (lit "2008-06-20T10:00:00Z" "dateTime")
                    (lit "2008-06-20T12:00:00+02:00" "dateTime")))))
  (testing "value unknown -> type error"
    (is (= :err (cmp :eq "\"x\"^^<http://ex/dt>" "\"y\"^^<http://ex/dt>")))
    (is (= :err (cmp :eq (lit "abc" "integer") (int-lit 1))))
    (is (= :err (cmp :eq (lit "2008-06-20T00:00:00Z" "dateTime")
                     (lit "2008-06-20T00:00:00" "dateTime")))
        "timezone vs no timezone within 14 hours is indeterminate"))
  (testing "NaN is not equal to itself (IEEE), but sameTerm is"
    (is (false? (cmp :eq (lit "NaN" "double") (lit "NaN" "double"))))
    (is (true? (ev {:type :same-term :left (c (lit "NaN" "double")) :right (c (lit "NaN" "double"))})))))

(deftest test-temporal-ordering
  (is (true? (cmp :lt (lit "2008-06-20T00:00:00Z" "dateTime") (lit "2008-06-25T00:00:00" "dateTime")))
      "timezone vs no timezone, more than 14 hours apart: determinate")
  (is (= :err (cmp :lt (lit "2008-06-20T00:00:00Z" "dateTime") (lit "2008-06-20T00:00:00" "dateTime"))))
  (is (true? (cmp :lt (lit "2008-06-21T05:00:00+02:00" "dateTime") (lit "2008-06-20T23:00:00-05:00" "dateTime")))
      "compared as instants (03:00Z < 04:00Z), not as strings")
  (is (true? (cmp :lt (lit "1999" "gYear") (lit "2001" "gYear"))))
  (is (true? (cmp :lt (lit "10:00:00" "time") (lit "11:00:00" "time"))))
  (is (= :err (cmp :lt (lit "2008-06-20" "date") (lit "2008-06-21T00:00:00Z" "dateTime")))
      "different temporal datatypes are not order-comparable"))

;;; ---------------------------------------------------------------------------
;;; W5: ORDER BY / MIN / MAX use term values, not serialized strings
;;; ---------------------------------------------------------------------------

(defn- order-by
  ([values] (order-by values true))
  ([values asc?]
   (let [specs [{:expr {:type :var :name "?v"} :ascending asc?}]]
     (->> values
          (map (fn [v] (let [row (if v {"?v" v} {})] [(qh/compute-sort-key row specs) row])))
          (#(qh/sort-keyed-rows % specs))
          (mapv #(get-in % [1 "?v"]))))))

(deftest test-order-by-uses-values
  (testing "strings by codepoint of the unescaped value (no closing quote)"
    (is (= ["\"New\"" "\"New York\""] (order-by ["\"New York\"" "\"New\""])))
    (is (= ["\"a\"" "\"a\\\"b\"" "\"ab\""] (order-by ["\"ab\"" "\"a\\\"b\"" "\"a\""]))
        "escaped quote compares as the quote character"))
  (testing "IRIs by their IRI string (no closing >)"
    (is (= ["<http://ex/item1>" "<http://ex/item10>"]
           (order-by ["<http://ex/item10>" "<http://ex/item1>"]))))
  (testing "numbers by value across datatypes; dates as instants"
    (is (= [(int-lit -5) (int-lit 2) (lit "2.5" "decimal") (int-lit 10) (lit "INF" "double")]
           (order-by [(int-lit 10) (lit "INF" "double") (lit "2.5" "decimal") (int-lit -5) (int-lit 2)])))
    (is (= [(lit "2008-06-21T05:00:00+02:00" "dateTime") (lit "2008-06-20T23:00:00-05:00" "dateTime")]
           (order-by [(lit "2008-06-20T23:00:00-05:00" "dateTime") (lit "2008-06-21T05:00:00+02:00" "dateTime")]))
        "03:00Z before 04:00Z, the reverse of string order"))
  (testing "total order across kinds: unbound < blank node < IRI < literal < triple term"
    (let [triple "<< <http://ex/s> <http://ex/p> <http://ex/o> >>"]
      (is (= [nil "_:b1" "<http://ex/a>" (int-lit 1) "\"a\"" triple]
             (order-by [triple "\"a\"" (int-lit 1) "<http://ex/a>" "_:b1" nil])))
      (is (= [triple "\"a\"" (int-lit 1) "<http://ex/a>" "_:b1" nil]
             (order-by [nil "_:b1" "<http://ex/a>" (int-lit 1) "\"a\"" triple] false))
          "DESC reverses everything, unbound last")))
  (testing "literals < cannot compare: numbers, then date/time, then by datatype IRI (as RDF4J)"
    ;; rdf:langString (http://www.w3.org/1999/...) < xsd:boolean < xsd:string,
    ;; and http://ex/dt sorts before both namespaces
    (is (= [(int-lit 7) (lit "2001" "gYear") "\"x\"^^<http://ex/dt>" "\"caf\u00e9\"@fr" T "\"New\""]
           (order-by ["\"New\"" T "\"caf\u00e9\"@fr" "\"x\"^^<http://ex/dt>" (lit "2001" "gYear") (int-lit 7)])))
    (is (= [(lit "2008-06-20" "date") (lit "2008-06-19T00:00:00Z" "dateTime")]
           (order-by [(lit "2008-06-19T00:00:00Z" "dateTime") (lit "2008-06-20" "date")]))
        "different date/time datatypes group by datatype IRI"))
  (testing "ties keep input order (stable)"
    (is (= [(int-lit 1) (lit "1.0" "decimal")] (order-by [(int-lit 1) (lit "1.0" "decimal")])))))

(deftest test-order-key-agrees-with-ordering-operator
  ;; Wherever < is defined (and not a tie), ORDER BY puts the values in the
  ;; same order. (Exception by design, not covered here: xsd:time values that
  ;; mix timezone / no timezone — the key reads a missing timezone as UTC.)
  (let [terms (into (:terms oracle)
                    [(lit "2008-06-20T10:00:00.5Z" "dateTime") (lit "2008-06-19T23:00:00" "dateTime")
                     (lit "1999" "gYear") (lit "2001" "gYear") (int-lit -5) (lit "1.0" "decimal")])]
    (doseq [a terms b terms
            :let [o (expr/compare-terms a b)]
            :when (and (not (expr/error? o)) (not (zero? o)))]
      (is (= (Integer/signum (int o))
             (Integer/signum (int (compare (expr/order-key a) (expr/order-key b)))))
          (str a " vs " b)))))

(defn- aggregate [agg-fn values]
  (let [state (reduce #(agg/update-agg-state agg-fn %1 %2) (agg/init-agg-state agg-fn) values)]
    (agg/format-agg-result agg-fn (agg/compute-final-agg agg-fn state))))

(deftest test-min-max-use-order-by-order
  (testing "MIN follows the RDF4J literal order: lang strings before simple strings"
    (is (= "\"caf\u00e9\"@fr" (aggregate :min ["\"New\"" "\"caf\u00e9\"@fr"]))))
  (testing "MIN(<a>, \"x\") is the IRI (IRIs sort before literals)"
    (is (= "<http://ex/a>" (aggregate :min ["\"x\"" "<http://ex/a>"])))
    (is (= "\"x\"" (aggregate :max ["\"x\"" "<http://ex/a>"]))))
  (testing "strings by value, numbers by value"
    (is (= "\"New\"" (aggregate :min ["\"New York\"" "\"New\""])))
    (is (= (int-lit 10) (aggregate :max [(int-lit 9) (int-lit 10) (int-lit 2)]))))
  (testing "combiner merge agrees with sequential update"
    (let [s1 (reduce #(agg/update-agg-state :min %1 %2) nil ["\"b\"" (int-lit 3)])
          s2 (reduce #(agg/update-agg-state :min %1 %2) nil ["<http://ex/z>" "\"a\""])]
      (is (= "<http://ex/z>" (agg/compute-final-agg :min (agg/merge-agg-states :min s1 s2))))))
  (testing "unbound values are skipped; all unbound -> no result"
    (is (= (int-lit 1) (aggregate :min [nil (int-lit 1) nil])))
    (is (nil? (aggregate :min [nil nil])))))

;;; ---------------------------------------------------------------------------
;;; W4: REGEX matches the unescaped value
;;; ---------------------------------------------------------------------------

(defn- regex
  ([text pattern] (regex text pattern nil))
  ([text pattern flags]
   (ev (cond-> {:type :regex :arg (c text) :pattern (c pattern)}
         flags (assoc :flags (c flags))))))

(deftest test-regex-on-unescaped-values
  (testing "escapes in the pattern reach java.util.regex unescaped"
    (is (true? (regex "\"a1\"" "\"\\\\d\"")) "\\d matches a digit")
    (is (false? (regex "\"ab\"" "\"\\\\d\"")))
    (is (true? (regex "\"a b\"" "\"\\\\s\"")))
    (is (true? (regex "\"a\\\"b\"" "\"\\\"\"")) "a quote in text and pattern"))
  (testing "escapes in the text are the real characters"
    (is (true? (regex "\"x\\ny\"" "\"x.y\"" "\"s\"")) "newline, dotall flag")
    (is (false? (regex "\"x\\ny\"" "\"x.y\""))))
  (testing "argument types (SPARQL REGEX): text string/lang literal, pattern simple string"
    (is (true? (regex "\"abc\"@en" "\"b\"")))
    (is (true? (regex "\"ABC\"" "\"b\"" "\"i\"")))
    (is (= :err (regex (int-lit 123) "\"1\"")))
    (is (= :err (regex "<http://ex/a>" "\"ex\"")))
    (is (= :err (regex "\"abc\"" "\"b\"@en")))
    (is (= :err (regex "\"abc\"" "\"(\"")) "invalid pattern is a type error")))

;;; ---------------------------------------------------------------------------
;;; W6: boolean expression results are xsd:boolean literals
;;; ---------------------------------------------------------------------------

(deftest test-boolean-results-are-boolean-literals
  (let [bound {:type :bound :var "?x"}
        unbound {}
        with-x {"?x" "\"1\""}]
    (testing "BOUND(?x) = true / false"
      (is (true? (ev {:type :cmp :op :eq :left bound :right (c T)} with-x)))
      (is (false? (ev {:type :cmp :op :eq :left bound :right (c T)} unbound)))
      (is (true? (ev {:type :cmp :op :eq :left bound :right (c F)} unbound))
          "used to be false: Boolean vs \"false\"^^xsd:boolean string"))
    (testing "IN, sameTerm and ordering accept boolean results"
      (is (true? (ev {:type :in :arg bound :set [(c T)]} with-x)))
      (is (true? (ev {:type :same-term :left bound :right (c F)} unbound)))
      (is (true? (ev {:type :cmp :op :lt :left bound :right (c T)} unbound)) "false < true"))
    (testing "LANG / DATATYPE of a boolean"
      (is (= "\"\"" (ev {:type :lang :arg bound} with-x)))
      (is (= (str "<" xsd "boolean>") (ev {:type :datatype :arg bound} with-x))))
    (testing "ORDER BY a boolean expression"
      (let [specs [{:expr bound :ascending true}]
            rows [with-x unbound]
            sorted (qh/sort-keyed-rows (map (fn [r] [(qh/compute-sort-key r specs) r]) rows) specs)]
        (is (= [unbound with-x] (mapv second sorted)) "false before true")))))

;;; ---------------------------------------------------------------------------
;;; W9: canonical numbers do not depend on the JVM locale
;;; ---------------------------------------------------------------------------

(deftest test-canonical-numbers-are-locale-independent
  (let [vf (SimpleValueFactory/getInstance)
        default (Locale/getDefault)]
    (try
      (doseq [loc [Locale/GERMANY (Locale. "tr" "TR") (Locale. "ar" "EG")]]
        (Locale/setDefault loc)
        (is (= (lit "1.0" "double") (ser/val->str (.createLiteral vf "1" XSD/DOUBLE))) (str loc))
        (is (= (lit "2.0" "float") (ser/val->str (.createLiteral vf "2" XSD/FLOAT))) (str loc))
        (is (true? (ev {:type :langmatches :left (c "\"TI\"") :right (c "\"ti\"")}))
            (str loc ": language ranges compare without the default locale")))
      (finally (Locale/setDefault default)))))

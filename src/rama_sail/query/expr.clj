(ns ^:no-doc rama-sail.query.expr
  "Expression evaluation for SPARQL FILTER and BIND expressions.

   Evaluates expression trees against variable bindings, supporting:
   - Comparisons: :eq, :ne, :lt, :le, :gt, :ge (numeric and string)
   - Logic: :and, :or, :not (three-valued, SPARQL error semantics)
   - Math: :plus, :minus, :multiply, :divide (typed numeric tower)
   - Functions: :str, :lang, :datatype, :coalesce, :bound, :if, :regex, ...

   All term values use N-Triples canonical format (e.g., \"\\\"value\\\"^^<datatype>\").

   Error semantics: SPARQL distinguishes false, unbound, and type error.
   eval-expr returns the sentinel ::error for a type error and nil for
   unbound. Errors propagate through most operators; FILTER treats an error
   as false (row excluded); BIND leaves the variable unbound; COALESCE skips
   both errors and unbound (but keeps a legitimate false).

   Term comparison is defined once, in the \"Term values\" section, and shared
   by every caller: `terms-equal` (= != IN, self-join !=), `compare-terms`
   (< <= > >=, self-join ordering) and `order-key` (ORDER BY, MIN/MAX)."
  (:require [clojure.string :as str])
  (:import (java.time LocalDateTime ZoneOffset)
           (java.util Locale)
           (javax.xml.datatype DatatypeConstants DatatypeFactory XMLGregorianCalendar)))

(def ^:const error
  "Sentinel returned by eval-expr for a SPARQL type error.
   Distinct from nil (unbound) and false (boolean result)."
  ::error)

(defn error? [v] (= v ::error))

(declare eval-expr sparql-ebv)

;;; ---------------------------------------------------------------------------
;;; Term parsing (one escape-aware scanner for all N-Triples string handling)
;;; ---------------------------------------------------------------------------

(defn triple-term?
  "Check if a serialized N-Triples string represents a triple term (<< ... >>)."
  [^String s]
  (and s (.startsWith s "<< ") (.endsWith s " >>")))

(defn- closing-quote-index
  "Index of the closing quote of a literal starting with a quote at index 0,
   skipping backslash-escaped characters. Returns -1 if not found."
  ^long [^String s]
  (let [len (.length s)]
    (loop [i 1]
      (if (>= i len)
        -1
        (let [ch (.charAt s i)]
          (cond
            (= ch \\) (recur (+ i 2))
            (= ch \") i
            :else (recur (inc i))))))))

(defn parse-term
  "Parse an N-Triples term string into a map, or nil for nil input.

   Returns one of:
   - {:kind :literal :lexical s :lang tag-or-nil :datatype iri-or-nil}
     (:lang and :datatype are mutually exclusive; both nil = simple literal)
   - {:kind :iri :value iri-string}
   - {:kind :bnode :id s}
   - {:kind :triple}                        (RDF-star triple term)
   - {:kind :other :value s}                (bare token, not valid N-Triples —
     tolerated for the internal plan API, e.g. {:type :const :val \"20\"})

   The lexical form is returned as serialized (escapes are NOT unescaped);
   canonical storage guarantees equal terms have equal serializations."
  [^String s]
  (when s
    (cond
      (triple-term? s)
      {:kind :triple}

      (.startsWith s "<")
      {:kind :iri :value (subs s 1 (max 1 (dec (count s))))}

      (.startsWith s "_:")
      {:kind :bnode :id (subs s 2)}

      (.startsWith s "\"")
      (let [close (closing-quote-index s)]
        (if (neg? close)
          {:kind :other :value s}
          (let [lexical (subs s 1 close)
                suffix (subs s (inc close))]
            (cond
              (= suffix "")
              {:kind :literal :lexical lexical :lang nil :datatype nil}

              (str/starts-with? suffix "@")
              {:kind :literal :lexical lexical :lang (subs suffix 1) :datatype nil}

              (str/starts-with? suffix "^^<")
              {:kind :literal :lexical lexical :lang nil
               :datatype (subs suffix 3 (max 3 (dec (count suffix))))}

              :else
              {:kind :other :value s}))))

      :else
      {:kind :other :value s})))

(defn- literal-of-kind? [term] (= :literal (:kind term)))

(def ^:private xsd "http://www.w3.org/2001/XMLSchema#")
(def ^:private xsd-string (str xsd "string"))
(def ^:private xsd-boolean (str xsd "boolean"))
(def ^:private xsd-integer (str xsd "integer"))
(def ^:private xsd-decimal (str xsd "decimal"))
(def ^:private xsd-double (str xsd "double"))
(def ^:private rdf-lang-string "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")

(def ^:private integer-datatypes
  (into #{} (map #(str xsd %))
        ["integer" "long" "int" "short" "byte"
         "nonNegativeInteger" "nonPositiveInteger" "negativeInteger" "positiveInteger"
         "unsignedLong" "unsignedInt" "unsignedShort" "unsignedByte"]))

(def ^:private float-datatypes #{(str xsd "double") (str xsd "float")})

(defn- parse-xsd-double
  "Parse an xsd:double/float lexical form; xsd uses INF/-INF, Java uses Infinity."
  ^Double [^String lex]
  (case lex
    "INF" Double/POSITIVE_INFINITY
    "+INF" Double/POSITIVE_INFINITY
    "-INF" Double/NEGATIVE_INFINITY
    "NaN" Double/NaN
    (Double/parseDouble lex)))

(defn- parse-integer-lexical
  "Parse an integer lexical form to Long (or BigInteger when out of range)."
  [^String lex]
  (try
    (Long/parseLong lex)
    (catch NumberFormatException _
      (BigInteger. lex))))

(defn- bare-token-number
  "Lenient numeric parse for bare (non-N-Triples) tokens used by the internal
   plan API, e.g. {:type :const :val \"20\"}. Integers stay integral."
  [^String s]
  (try
    (Long/parseLong s)
    (catch NumberFormatException _
      (try
        (let [d (Double/parseDouble s)] d)
        (catch NumberFormatException _ nil)))))

(defn term-numeric-value
  "Numeric value of a serialized term per its datatype, or nil when the term
   is not numeric. Integer datatypes yield Long/BigInteger, xsd:decimal yields
   BigDecimal, xsd:double/float yield Double. Plain and xsd:string literals
   are NOT numeric (SPARQL compares them as strings). Bare tokens (internal
   plan constants) parse leniently. Invalid lexical forms yield nil."
  [^String s]
  (when s
    (let [term (parse-term s)]
      (case (:kind term)
        :literal
        (let [{:keys [lexical lang datatype]} term]
          (when (nil? lang)
            (try
              (cond
                (contains? integer-datatypes datatype) (parse-integer-lexical lexical)
                (= xsd-decimal datatype) (BigDecimal. ^String lexical)
                (contains? float-datatypes datatype) (parse-xsd-double lexical)
                :else nil)
              (catch NumberFormatException _ nil))))

        :other
        (bare-token-number (:value term))

        nil))))

(defn parse-numeric
  "DEPRECATED lenient numeric parse retained for backward compatibility with
   direct API users and benchmarks: returns a Double for anything whose
   (first-quoted-segment or raw) text parses as a double, regardless of
   datatype. New code should use term-numeric-value, which respects
   datatypes per SPARQL."
  ^Double [^String s]
  (when s
    (try
      (let [clean (if (.startsWith s "\"")
                    (let [end (closing-quote-index s)]
                      (if (pos? end) (.substring s 1 end) s))
                    s)]
        (Double/parseDouble clean))
      (catch NumberFormatException _ nil))))

;;; ---------------------------------------------------------------------------
;;; Numeric tower: comparison and arithmetic with SPARQL type promotion
;;; ---------------------------------------------------------------------------

(defn compare-numbers
  "Compare two numbers with SPARQL promotion: if either is a double the
   comparison is in double space; otherwise exact (BigDecimal) comparison,
   so large integers and decimals never lose precision."
  [a b]
  (cond
    ;; fast path: the common integer case needs no BigDecimal allocation
    (and (instance? Long a) (instance? Long b)) (Long/compare (long a) (long b))
    (or (instance? Double a) (instance? Double b)) (Double/compare (double a) (double b))
    :else (.compareTo (bigdec a) (bigdec b))))

(defn- integral? [n]
  (or (instance? Long n) (instance? BigInteger n) (instance? Integer n)
      (instance? clojure.lang.BigInt n)))

(defn- num-result-literal
  "Serialize an arithmetic result as a typed N-Triples literal."
  [n]
  (cond
    (integral? n)
    (str "\"" n "\"^^<" xsd-integer ">")

    (instance? BigDecimal n)
    (let [^BigDecimal d n
          stripped (.stripTrailingZeros d)
          ;; stripTrailingZeros can produce exponent forms like 1E+1
          plain (.toPlainString stripped)]
      (str "\"" plain "\"^^<" xsd-decimal ">"))

    :else
    (let [d (double n)
          lex (cond
                (Double/isNaN d) "NaN"
                (= d Double/POSITIVE_INFINITY) "INF"
                (= d Double/NEGATIVE_INFINITY) "-INF"
                :else (str d))]
      (str "\"" lex "\"^^<" xsd-double ">"))))

(defn- num-binop
  "Apply arithmetic op with SPARQL promotion. Returns a number or ::error
   (division by zero in exact types; any nil operand handled by caller).
   integer / integer produces xsd:decimal per SPARQL."
  [op a b]
  (cond
    (or (instance? Double a) (instance? Double b))
    (let [x (double a) y (double b)]
      (case op
        :plus (+ x y)
        :minus (- x y)
        :multiply (* x y)
        ;; IEEE semantics: x/0 is INF/NaN, not an error
        :divide (/ x y)))

    (or (instance? BigDecimal a) (instance? BigDecimal b)
        (= op :divide))
    (let [x (bigdec a) y (bigdec b)]
      (case op
        :plus (.add x y)
        :minus (.subtract x y)
        :multiply (.multiply x y)
        :divide (if (zero? (.signum y))
                  ::error
                  (.divide x y java.math.MathContext/DECIMAL64))))

    :else
    ;; both integral; +'/-'/*' auto-promote to BigInt on overflow
    (let [r (case op
              :plus (+' a b)
              :minus (-' a b)
              :multiply (*' a b))]
      (if (instance? clojure.lang.BigInt r) (.toBigInteger ^clojure.lang.BigInt r) r))))

(defn add-numbers
  "Add two numbers with SPARQL promotion (exact for integers/decimals,
   double when either operand is a double). Used by SUM/AVG aggregation."
  [a b]
  (num-binop :plus a b))

;;; ---------------------------------------------------------------------------
;;; Triple term decomposition (RDF-star)
;;; ---------------------------------------------------------------------------

(defn- extract-triple-term-part
  "Extract subject (0), predicate (1), or object (2) from a serialized triple term.
   Returns the N-Triples string for the requested component, or nil on failure."
  [^String s ^long index]
  (when (triple-term? s)
    (let [inner (subs s 3 (- (count s) 3))  ;; strip "<< " and " >>"
          len (count inner)]
      ;; Parse terms one at a time, stop when we reach the requested index
      (loop [pos 0
             part-idx 0]
        (when (< pos len)
          (let [ch (.charAt inner pos)]
            (cond
              (Character/isWhitespace ch)
              (recur (inc pos) part-idx)

              ;; Nested triple term
              (and (< (inc pos) len) (= ch \<) (= (.charAt inner (inc pos)) \<))
              (let [end (loop [i (+ pos 2) depth 1]
                          (cond
                            (>= i (dec len)) (inc len)
                            (and (= (.charAt inner i) \<) (< (inc i) len) (= (.charAt inner (inc i)) \<))
                            (recur (+ i 2) (inc depth))
                            (and (= (.charAt inner i) \>) (< (inc i) len) (= (.charAt inner (inc i)) \>))
                            (if (= depth 1) (+ i 2) (recur (+ i 2) (dec depth)))
                            :else (recur (inc i) depth)))]
                (if (= part-idx index)
                  (subs inner pos end)
                  (recur end (inc part-idx))))

              ;; IRI
              (= ch \<)
              (let [end (inc (.indexOf inner ">" (int pos)))]
                (if (= part-idx index)
                  (subs inner pos end)
                  (recur end (inc part-idx))))

              ;; Literal
              (= ch \")
              (let [close-quote (loop [i (inc pos)]
                                  (cond
                                    (>= i len) i
                                    (= (.charAt inner i) \\) (recur (+ i 2))
                                    (= (.charAt inner i) \") i
                                    :else (recur (inc i))))
                    end (if (>= (inc close-quote) len)
                          (inc close-quote)
                          (let [next-ch (.charAt inner (inc close-quote))]
                            (cond
                              (= next-ch \@) (let [sp (.indexOf inner " " (int (inc close-quote)))]
                                               (if (neg? sp) len sp))
                              (= next-ch \^) (let [gt (.indexOf inner ">" (int (inc close-quote)))]
                                               (if (neg? gt) len (inc gt)))
                              :else (inc close-quote))))]
                (if (= part-idx index)
                  (subs inner pos end)
                  (recur end (inc part-idx))))

              ;; Blank node
              (and (= ch \_) (< (inc pos) len) (= (.charAt inner (inc pos)) \:))
              (let [end (loop [i (+ pos 2)]
                          (if (or (>= i len) (Character/isWhitespace (.charAt inner i)))
                            i (recur (inc i))))]
                (if (= part-idx index)
                  (subs inner pos end)
                  (recur end (inc part-idx))))

              :else (recur (inc pos) part-idx))))))))

;;; ---------------------------------------------------------------------------
;;; Regex cache
;;; ---------------------------------------------------------------------------

(def ^:private regex-cache-limit 512)
(def ^:private regex-cache (atom {}))

(defn- compiled-regex
  "Compile (or fetch cached) a regex Pattern. Returns ::error for an invalid
   pattern instead of throwing. The cache is bounded: data-driven patterns
   (from variables) cannot grow it without limit."
  [^String pattern ^long flags]
  (let [k [pattern flags]]
    (or (get @regex-cache k)
        (let [compiled (try
                         (java.util.regex.Pattern/compile pattern flags)
                         (catch java.util.regex.PatternSyntaxException _ ::error))]
          (when-not (error? compiled)
            (swap! regex-cache
                   (fn [c]
                     (let [c (if (>= (count c) regex-cache-limit) {} c)]
                       (assoc c k compiled)))))
          compiled))))

;;; ---------------------------------------------------------------------------
;;; Helpers shared by eval branches
;;; ---------------------------------------------------------------------------

(defn unescape-lexical
  "Unescape a serialized lexical form (the N-Triples escapes canonical
   storage writes: \\\\ \\\" \\n \\r, plus \\t \\b \\f \\' \\uXXXX \\UXXXXXXXX that
   hand-written plan constants may carry). Fast path: no backslash, no copy."
  ^String [^String s]
  (if (neg? (.indexOf s (int \\)))
    s
    (let [len (.length s)
          sb (StringBuilder. len)]
      (loop [i 0]
        (if (>= i len)
          (.toString sb)
          (let [ch (.charAt s i)]
            (if (and (= ch \\) (< (inc i) len))
              (let [nx (.charAt s (inc i))]
                (case nx
                  \n (do (.append sb \newline) (recur (+ i 2)))
                  \r (do (.append sb \return) (recur (+ i 2)))
                  \t (do (.append sb \tab) (recur (+ i 2)))
                  \b (do (.append sb \backspace) (recur (+ i 2)))
                  \f (do (.append sb \formfeed) (recur (+ i 2)))
                  (\" \' \\) (do (.append sb nx) (recur (+ i 2)))
                  (\u \U) (let [n (if (= nx \u) 4 8)
                                end (+ i 2 n)]
                            (if-let [cp (when (<= end len)
                                          (try (Integer/parseInt (subs s (+ i 2) end) 16)
                                               (catch NumberFormatException _ nil)))]
                              (do (.appendCodePoint sb (int cp)) (recur end))
                              (do (.append sb ch) (recur (inc i)))))
                  ;; unknown escape: keep the backslash literally
                  (do (.append sb ch) (recur (inc i)))))
              (do (.append sb ch) (recur (inc i))))))))))

(defn- lower-root
  "Locale-independent lower-casing (clojure.string/lower-case uses the JVM's
   default locale: on a Turkish JVM \"I\" would become a dotless i)."
  ^String [^String s]
  (.toLowerCase s Locale/ROOT))

(defn- plain-literal [s] (str "\"" s "\""))

(defn- boolean-literal [b]
  (if b
    (str "\"true\"^^<" xsd-boolean ">")
    (str "\"false\"^^<" xsd-boolean ">")))

(defn value->term-string
  "Convert an eval-expr result to an N-Triples term string for storing in a
   binding row: booleans become xsd:boolean literals; nil and ::error yield
   nil (the variable stays unbound); strings pass through."
  [v]
  (cond
    (nil? v) nil
    (error? v) nil
    (instance? Boolean v) (boolean-literal v)
    :else v))

(defn- as-term
  "An eval result as a term: a Java Boolean (the result of a comparison,
   BOUND, ...) IS the xsd:boolean literal; nil/::error pass through."
  [v]
  (if (instance? Boolean v) (boolean-literal v) v))

;;; ---------------------------------------------------------------------------
;;; Term values: the one comparator
;;;
;;; Every term is classified once into a value class; equality, the ordering
;;; operators and ORDER BY are all defined over these classes:
;;;
;;;   :numeric   valid integer/decimal/float/double literal (or bare number token)
;;;   :boolean   valid xsd:boolean
;;;   :temporal  valid XSD date/time type (dateTime, date, time, gYear, ...);
;;;              :datatype matters: a date never equals a dateTime
;;;   :string    simple literal or xsd:string (or bare non-numeric token)
;;;   :lang      language-tagged string
;;;   :opaque    any other datatype, or an ill-typed literal of a known one
;;;   :iri :bnode :triple
;;;
;;; Semantics (decided Nov 4, 2026; checked cell by cell against Apache Jena
;;; and RDF4J — see expr-test):
;;; - `=` follows RDFterm-equal as Jena implements it: literals of DIFFERENT
;;;   known value classes are simply unequal (false); an opaque literal that is
;;;   not the same term is a type error (its value is unknown); NaN = NaN is
;;;   false (IEEE / op:numeric-equal).
;;; - `<` and friends are defined only within numeric, string, boolean, the
;;;   same temporal datatype, and the same language tag (as Jena). Strict
;;;   SPARQL everywhere else: IRIs, blank nodes, opaque literals and mixed
;;;   classes are a type error.
;;; - ORDER BY / MIN / MAX: SPARQL's total order (unbound < blank nodes < IRIs <
;;;   literals < triple terms), using `<` within a class and a fixed class rank
;;;   across classes.
;;; ---------------------------------------------------------------------------

(def ^:private temporal-datatypes
  (into #{} (map #(str xsd %))
        ["dateTime" "dateTimeStamp" "date" "time"
         "gYear" "gYearMonth" "gMonth" "gMonthDay" "gDay"]))

(def ^:private ^DatatypeFactory datatype-factory (DatatypeFactory/newInstance))

(defn- parse-temporal
  "XMLGregorianCalendar for an XSD date/time lexical form, or nil if ill-typed.
   The JDK implements the XSD lexical spaces and the XSD partial order."
  ^XMLGregorianCalendar [^String lex]
  (try (.newXMLGregorianCalendar datatype-factory lex)
       (catch IllegalArgumentException _ nil)))

(defn- xsd-boolean-value [^String lex]
  (case lex ("true" "1") true ("false" "0") false nil))

(defn term-value
  "Classify a serialized term (or a Java Boolean eval result) into its value
   class; see the section comment. Returns nil for nil."
  [v]
  (when (some? v)
    (if (instance? Boolean v)
      {:class :boolean :v (boolean v)}
      (let [term (parse-term v)]
        (case (:kind term)
          :iri {:class :iri :s (:value term)}
          :bnode {:class :bnode :s (:id term)}
          :triple {:class :triple :s v}
          :other (if-let [n (bare-token-number (:value term))]
                   {:class :numeric :v n}
                   {:class :string :lex (unescape-lexical (:value term)) :xsd? false})
          :literal
          (let [{:keys [lexical lang datatype]} term]
            (cond
              lang {:class :lang :lex (unescape-lexical lexical) :lang lang}
              (or (nil? datatype) (= xsd-string datatype))
              {:class :string :lex (unescape-lexical lexical) :xsd? (some? datatype)}
              :else
              (or (when-let [n (try
                                 (cond
                                   (contains? integer-datatypes datatype) (parse-integer-lexical lexical)
                                   (= xsd-decimal datatype) (BigDecimal. ^String lexical)
                                   (contains? float-datatypes datatype) (parse-xsd-double lexical)
                                   :else nil)
                                 (catch NumberFormatException _ nil))]
                    {:class :numeric :v n})
                  (when (= xsd-boolean datatype)
                    (let [b (xsd-boolean-value lexical)]
                      (when (some? b) {:class :boolean :v b})))
                  (when (contains? temporal-datatypes datatype)
                    (when-let [cal (parse-temporal lexical)]
                      {:class :temporal :datatype datatype :cal cal}))
                  {:class :opaque :datatype datatype :lex lexical}))))))))

(defn- nan? [x] (and (instance? Double x) (Double/isNaN ^double x)))

(defn- lang= [a b] (.equalsIgnoreCase ^String (:lang a) ^String (:lang b)))

(defn- compare-temporal
  "XSD partial order: negative/zero/positive, or ::error when indeterminate
   (one value has a timezone and the other does not, within 14 hours)."
  [a b]
  (let [r (.compare ^XMLGregorianCalendar (:cal a) ^XMLGregorianCalendar (:cal b))]
    (condp = r
      DatatypeConstants/LESSER -1
      DatatypeConstants/EQUAL 0
      DatatypeConstants/GREATER 1
      ::error)))

(defn- comparable-classes?
  "True when `<` is defined between the two classified values."
  [a b]
  (let [c (:class a)]
    (and (= c (:class b))
         (case c
           (:numeric :string :boolean) true
           :temporal (= (:datatype a) (:datatype b))
           :lang (lang= a b)
           false))))

(defn- compare-values*
  "Compare two classified values known to be comparable-classes?."
  [a b]
  (case (:class a)
    :numeric (compare-numbers (:v a) (:v b))
    :boolean (compare (:v a) (:v b))
    :temporal (compare-temporal a b)
    (:string :lang) (compare (:lex a) (:lex b))))

(defn- compare-classified [a b]
  (if (and a b (comparable-classes? a b))
    (compare-values* a b)
    ::error))

(defn compare-terms
  "The ordering operators (< <= > >=): negative/zero/positive, or ::error when
   the operands are not order-comparable. Strict SPARQL: IRIs, blank nodes,
   triple terms, opaque literals and mixed value classes are a type error."
  [l r]
  (compare-classified (term-value l) (term-value r)))

(defn order-comparable?
  "True when `<` is defined between this term and another of its kind (used by
   the self-join rewrite to skip subjects that can never satisfy an ordering)."
  [v]
  (let [a (term-value v)]
    (boolean (and a (comparable-classes? a a)))))

(def ^:private literal-classes #{:numeric :boolean :temporal :string :lang :opaque})

(defn- equal-classified
  "terms-equal over already classified values (l and r are the terms)."
  [l r a b]
  (cond
    (or (nil? a) (nil? b)) ::error
    (= l r) (not (and (= :numeric (:class a)) (nan? (:v a))))
    (not (and (literal-classes (:class a)) (literal-classes (:class b)))) false
    (or (= :lang (:class a)) (= :lang (:class b)))
    (and (= :lang (:class a) (:class b)) (lang= a b) (= (:lex a) (:lex b)))
    (or (= :opaque (:class a)) (= :opaque (:class b))) ::error
    (not= (:class a) (:class b)) false
    (and (= :temporal (:class a)) (not= (:datatype a) (:datatype b))) false
    (and (= :numeric (:class a)) (or (nan? (:v a)) (nan? (:v b)))) false
    :else (let [c (compare-values* a b)]
            (if (error? c) ::error (zero? c)))))

(defn terms-equal
  "RDFterm-equal for = / != / IN: true, false or ::error (see the section
   comment)."
  [l r]
  (equal-classified l r (term-value l) (term-value r)))

;; Plan constants are classified once, not once per row: a FILTER like
;; ?d < "2008-06-20T00:00:00Z"^^xsd:dateTime would otherwise re-parse the
;; constant for every binding. Bounded like the regex cache; only :const
;; operands go through it, so data values never churn it.
(def ^:private const-value-cache-limit 1024)
(def ^:private ^java.util.concurrent.ConcurrentHashMap const-value-cache
  (java.util.concurrent.ConcurrentHashMap.))

(defn- const-term-value [^String s]
  (or (.get const-value-cache s)
      (let [t (term-value s)]
        (when t
          (when (>= (.size const-value-cache) const-value-cache-limit)
            (.clear const-value-cache))
          (.put const-value-cache s t))
        t)))

(defn- identity-only-term?
  "True for an IRI, blank node or triple term (equality is term identity,
   never order-comparable) — decided from the first character."
  [^String s]
  (or (.startsWith s "<") (.startsWith s "_:")))

(defn- bare-token?
  "A bare (non-N-Triples) token, as hand-written internal plan constants use
   ({:type :const :val \"25\"}); SPARQL never produces one."
  [^String s]
  (and (pos? (.length s))
       (let [ch (.charAt s 0)]
         (not (or (= ch \") (= ch \<) (.startsWith s "_:"))))))

(defn- lenient-compare
  "Internal plan API only (an operand is a bare token): compare the text of
   the two terms — numerically when both parse as numbers, else as strings —
   as the engine always has for bare tokens. ::error when a side has no text."
  [l r]
  (let [text (fn [s] (let [t (parse-term s)]
                       (case (:kind t)
                         :literal (unescape-lexical (:lexical t))
                         :other (:value t)
                         nil)))
        a (text l) b (text r)]
    (if (and a b)
      (let [na (bare-token-number a) nb (bare-token-number b)]
        (if (and na nb) (compare-numbers na nb) (compare a b)))
      ::error)))

(defn- operand-value
  "term-value of an evaluated operand; constants come from the cache."
  [expr v]
  (if (and (= :const (:type expr)) (string? v))
    (const-term-value v)
    (term-value v)))

;; ORDER BY key: a fixed-length vector of Comparable values, compared with
;; clojure.core/compare (Clojure orders vectors by LENGTH first, so every key
;; has the same length; the element types at each position are uniform within
;; a [kind group] prefix). Pure data on purpose: it can be computed once per
;; row, shipped between tasks, and used directly as the :sort value of Rama's
;; +limit / +top-monotonic.
;;
;; Literals that `<` cannot compare are ordered as RDF4J's ValueComparator
;; does (SPARQL leaves this to the implementation; Rama is an RDF4J SAIL, so
;; ORDER BY and MIN/MAX agree with RDF4J): numeric datatypes first, then
;; XSD date/time datatypes (each datatype by its IRI, then by value), then all
;; other literals by datatype IRI — simple literals are xsd:string and
;; language-tagged ones rdf:langString (RDF 1.1) — then language tag, then
;; value / lexical form.

(defn- temporal-sort-value
  "A total-order stand-in for an XSD date/time: its instant in epoch seconds,
   reading a missing timezone as UTC and missing fields as their minimum
   (agrees with every determinate XSD comparison except xsd:time values that
   mix timezone / no timezone). Computed from the parsed fields with
   java.time rather than via GregorianCalendar (several times cheaper)."
  [^XMLGregorianCalendar cal]
  (let [u DatatypeConstants/FIELD_UNDEFINED
        f (fn [^long x ^long d] (if (= x u) d x))
        hour (f (.getHour cal) 0)
        ldt (LocalDateTime/of (int (f (.getYear cal) 1970)) (int (f (.getMonth cal) 1))
                              (int (f (.getDay cal) 1)) (int (if (= hour 24) 0 hour))
                              (int (f (.getMinute cal) 0)) (int (f (.getSecond cal) 0)))
        ldt (if (= hour 24) (.plusDays ldt 1) ldt)
        epoch (- (.toEpochSecond ldt ZoneOffset/UTC) (* 60 (f (.getTimezone cal) 0)))
        frac (.getFractionalSecond cal)]
    (if frac (+ (bigdec epoch) frac) (bigdec epoch))))

(defn order-key
  "SPARQL ORDER BY key of an eval result (term string, Boolean, nil or
   ::error): [kind group sub a b c]. kind: 0 unbound/error, 1 blank node,
   2 IRI, 3 literal, 4 triple term. Literal groups: 0 numeric, 1 date/time,
   2 everything else (see the comment above)."
  [v]
  (let [t (when-not (error? v) (term-value v))]
    (if (nil? t)
      [0 0 0 0 "" ""]
      (case (:class t)
        :bnode [1 0 0 0 (:s t) ""]
        :iri [2 0 0 0 (:s t) ""]
        :triple [4 0 0 0 (:s t) ""]
        ;; NaN after every number (as compare-numbers); equal values of
        ;; different datatypes tie-break on the serialized term
        :numeric (let [n (:v t)]
                   [3 0 (if (nan? n) 1 0) (if (nan? n) 0 n) (str v) ""])
        :temporal [3 1 0 (:datatype t) (temporal-sort-value (:cal t)) (str v)]
        :boolean [3 2 0 xsd-boolean (if (:v t) "1" "0") ""]
        :string [3 2 0 xsd-string (:lex t) ""]
        :lang [3 2 0 rdf-lang-string (lower-root (:lang t)) (:lex t)]
        :opaque (if (or (contains? integer-datatypes (:datatype t))
                        (contains? float-datatypes (:datatype t))
                        (= xsd-decimal (:datatype t)))
                  ;; ill-typed numeric: with the numbers, after NaN
                  [3 0 2 0 (str v) ""]
                  [3 2 0 (:datatype t) (:lex t) ""])))))

(defn compare-order-keys
  "Compare two order-key vectors (ascending)."
  [a b]
  (compare a b))

(defn- string-literal-text
  "Unescaped text of a string or language-tagged literal (REGEX, LANGMATCHES
   arguments), or nil for any other term."
  [v]
  (let [t (term-value (as-term v))]
    (when (#{:string :lang} (:class t)) (:lex t))))

(defn- simple-string-text
  "Unescaped text of a simple / xsd:string literal (REGEX pattern and flags),
   or nil."
  [v]
  (let [t (term-value (as-term v))]
    (when (= :string (:class t)) (:lex t))))

;;; ---------------------------------------------------------------------------
;;; Expression evaluation
;;; ---------------------------------------------------------------------------

(defn eval-expr
  "Evaluate an expression tree against a map of variable bindings.

   Returns:
   - an N-Triples term string, for term-valued expressions
   - a Java Boolean, for boolean-valued expressions
   - nil for unbound
   - ::error (see `error`) for a SPARQL type error

   Callers at operator boundaries decide what an error means: FILTER excludes
   the row, BIND leaves the variable unbound, COALESCE tries the next branch."
  [expr bindings]
  (case (:type expr)
    :const (:val expr)
    :var   (get bindings (:name expr))

    :cmp
    (let [l (as-term (eval-expr (:left expr) bindings))
          r (as-term (eval-expr (:right expr) bindings))
          op (:op expr)]
      (if (or (nil? l) (error? l) (nil? r) (error? r))
        ::error
        (cond
          (or (identity-only-term? l) (identity-only-term? r))
          ;; an IRI / blank node / triple term: = is term identity and the
          ;; ordering operators are a type error — no classification needed
          (case op
            :eq (= l r)
            :ne (not= l r)
            ::error)

          (or (bare-token? l) (bare-token? r))
          (let [c (lenient-compare l r)]
            (if (error? c)
              ::error
              (case op
                :eq (zero? c) :ne (not (zero? c))
                :lt (neg? c) :le (not (pos? c)) :gt (pos? c) :ge (not (neg? c)))))

          :else
        (let [a (operand-value (:left expr) l)
              b (operand-value (:right expr) r)]
          (case op
            :eq (equal-classified l r a b)
            :ne (let [e (equal-classified l r a b)]
                  (if (error? e) ::error (not e)))
            (let [c (compare-classified a b)]
              (if (error? c)
                ::error
                (case op
                  :lt (neg? c)
                  :le (not (pos? c))
                  :gt (pos? c)
                  :ge (not (neg? c))))))))))

    :logic
    (case (:op expr)
      ;; Three-valued logic per SPARQL 17.2: an error is absorbed by a
      ;; dominating false (AND) / true (OR), otherwise it propagates.
      :and (let [l (sparql-ebv (eval-expr (:left expr) bindings))]
             (if (false? l)
               false
               (let [r (sparql-ebv (eval-expr (:right expr) bindings))]
                 (cond
                   (false? r) false
                   (or (error? l) (error? r)) ::error
                   :else true))))
      :or (let [l (sparql-ebv (eval-expr (:left expr) bindings))]
            (if (true? l)
              true
              (let [r (sparql-ebv (eval-expr (:right expr) bindings))]
                (cond
                  (true? r) true
                  (or (error? l) (error? r)) ::error
                  :else false))))
      :not (let [v (sparql-ebv (eval-expr (:arg expr) bindings))]
             (if (error? v) ::error (not v))))

    :math
    (let [l (term-numeric-value (let [v (eval-expr (:left expr) bindings)]
                                  (when (string? v) v)))
          r (term-numeric-value (let [v (eval-expr (:right expr) bindings)]
                                  (when (string? v) v)))]
      (if (or (nil? l) (nil? r))
        ::error
        (let [result (num-binop (:op expr) l r)]
          (if (error? result)
            ::error
            (num-result-literal result)))))

    :str
    (let [v (eval-expr (:arg expr) bindings)]
      (cond
        (nil? v) ::error
        (error? v) ::error
        (instance? Boolean v) (plain-literal (str v))
        :else
        (let [term (parse-term v)]
          (case (:kind term)
            :literal (plain-literal (:lexical term))
            :iri (plain-literal (:value term))
            ;; STR of a bnode is lenient (returns the id) — retained behavior
            :bnode (plain-literal (:id term))
            :triple ::error
            :other (plain-literal (:value term))))))

    :coalesce
    ;; First argument that is neither unbound nor an error; false is a value.
    (loop [args (:args expr)]
      (if (empty? args)
        ::error
        (let [v (eval-expr (first args) bindings)]
          (if (or (nil? v) (error? v))
            (recur (rest args))
            v))))

    :bound
    (some? (get bindings (:var expr)))

    :if
    (let [c (sparql-ebv (eval-expr (:condition expr) bindings))]
      (if (error? c)
        ::error
        (if c
          (eval-expr (:then expr) bindings)
          (eval-expr (:else expr) bindings))))

    :type-check
    (let [v (eval-expr (:arg expr) bindings)]
      (cond
        (nil? v) ::error
        (error? v) ::error
        (instance? Boolean v) (case (:check expr)
                                :is-literal true
                                :is-numeric false
                                false)
        :else
        (let [term (parse-term v)]
          (case (:check expr)
            :is-iri (= :iri (:kind term))
            :is-bnode (= :bnode (:kind term))
            :is-literal (literal-of-kind? term)
            :is-numeric (some? (term-numeric-value v))
            :is-triple (= :triple (:kind term))))))

    :lang
    (let [v (as-term (eval-expr (:arg expr) bindings))]
      (cond
        (or (nil? v) (error? v)) ::error
        (not (string? v)) ::error
        :else
        (let [term (parse-term v)]
          (if (literal-of-kind? term)
            (plain-literal (or (:lang term) ""))
            ::error))))

    :datatype
    (let [v (eval-expr (:arg expr) bindings)]
      (cond
        (or (nil? v) (error? v)) ::error
        (instance? Boolean v) (str "<" xsd-boolean ">")
        :else
        (let [term (parse-term v)]
          (if (literal-of-kind? term)
            (cond
              (:datatype term) (str "<" (:datatype term) ">")
              (:lang term) (str "<" rdf-lang-string ">")
              :else (str "<" xsd-string ">"))
            ::error))))

    :langmatches
    (let [lang-val (eval-expr (:left expr) bindings)
          pattern-val (eval-expr (:right expr) bindings)]
      (if (or (nil? lang-val) (error? lang-val)
              (nil? pattern-val) (error? pattern-val))
        ::error
        (let [lang (string-literal-text lang-val)
              pat (string-literal-text pattern-val)]
          (if (or (nil? lang) (nil? pat))
            ::error
            (if (= pat "*")
              (not (str/blank? lang))
              ;; RFC 4647 basic filtering: case-insensitive prefix match
              (let [lang-lc (lower-root lang)
                    pat-lc (lower-root pat)]
                (or (= lang-lc pat-lc)
                    (str/starts-with? lang-lc (str pat-lc "-")))))))))

    :regex
    (let [v (eval-expr (:arg expr) bindings)
          pat-val (eval-expr (:pattern expr) bindings)
          flags-val (when (:flags expr) (eval-expr (:flags expr) bindings))]
      (if (or (nil? v) (error? v) (nil? pat-val) (error? pat-val)
              (error? flags-val))
        ::error
        ;; SPARQL REGEX: text is a string or language-tagged literal, pattern
        ;; and flags are simple strings — all matched on their UNESCAPED
        ;; values (the stored form escapes \ " \n \r, so "\\d" used to
        ;; reach java.util.regex as a literal backslash).
        (let [text (string-literal-text v)
              pattern (simple-string-text pat-val)
              flag-str (when flags-val (simple-string-text flags-val))]
          (if (or (nil? text) (nil? pattern) (and flags-val (nil? flag-str)))
            ::error
            (let [flags-int (if flag-str
                              (reduce (fn [acc ch]
                                        (case ch
                                          \i (bit-or acc java.util.regex.Pattern/CASE_INSENSITIVE)
                                          \m (bit-or acc java.util.regex.Pattern/MULTILINE)
                                          \s (bit-or acc java.util.regex.Pattern/DOTALL)
                                          \x (bit-or acc java.util.regex.Pattern/COMMENTS)
                                          acc))
                                      0 flag-str)
                              0)
                  compiled (compiled-regex pattern flags-int)]
              (if (error? compiled)
                ::error
                (.find (.matcher ^java.util.regex.Pattern compiled ^String text))))))))

    :same-term
    (let [l (eval-expr (:left expr) bindings)
          r (eval-expr (:right expr) bindings)]
      (if (or (nil? l) (error? l) (nil? r) (error? r))
        ::error
        ;; canonical serialization ⇒ same term iff same string
        (= (as-term l) (as-term r))))

    :in
    (let [v (eval-expr (:arg expr) bindings)]
      (if (or (nil? v) (error? v))
        ::error
        ;; IN is equality against each member; a member error is absorbed by
        ;; a later true, otherwise the whole expression errors.
        (loop [members (:set expr) saw-error false]
          (if (empty? members)
            (if saw-error ::error false)
            (let [m (eval-expr (first members) bindings)]
              (if (or (nil? m) (error? m))
                (recur (rest members) true)
                (let [e (terms-equal (as-term v) (as-term m))]
                  (cond
                    (true? e) true
                    (error? e) (recur (rest members) true)
                    :else (recur (rest members) saw-error)))))))))

    ;; --- RDF-star Triple Term Functions ---

    :is-triple
    (let [v (eval-expr (:arg expr) bindings)]
      (if (or (nil? v) (error? v))
        ::error
        (and (string? v) (triple-term? v))))

    :triple-subject
    (let [v (eval-expr (:arg expr) bindings)]
      (if (or (nil? v) (error? v))
        ::error
        (or (extract-triple-term-part v 0) ::error)))

    :triple-predicate
    (let [v (eval-expr (:arg expr) bindings)]
      (if (or (nil? v) (error? v))
        ::error
        (or (extract-triple-term-part v 1) ::error)))

    :triple-object
    (let [v (eval-expr (:arg expr) bindings)]
      (if (or (nil? v) (error? v))
        ::error
        (or (extract-triple-term-part v 2) ::error)))

    :triple-constructor
    (let [s (eval-expr (:subject expr) bindings)
          p (eval-expr (:predicate expr) bindings)
          o (eval-expr (:object expr) bindings)]
      (if (or (nil? s) (error? s) (nil? p) (error? p) (nil? o) (error? o))
        ::error
        (str "<< " s " " p " " o " >>")))

    ;; Unknown expression type: a type error, not a silent unbound.
    ::error))

;;; ---------------------------------------------------------------------------
;;; Effective boolean value and filter entry point
;;; ---------------------------------------------------------------------------

(defn sparql-ebv
  "SPARQL Effective Boolean Value. Returns true, false, or ::error.
   Rules per SPARQL 17.2.2:
   - boolean → itself; xsd:boolean literal → its value (invalid lexical → false)
   - numeric literal → false for 0/NaN (invalid lexical → false)
   - simple/xsd:string literal → false for empty string
   - unbound, error, IRI, bnode, other → type error"
  [v]
  (cond
    (nil? v) ::error
    (error? v) ::error
    (instance? Boolean v) (boolean v)
    (not (string? v)) ::error
    :else
    (let [term (parse-term v)]
      (case (:kind term)
        :literal
        (let [{:keys [lexical lang datatype]} term]
          (cond
            (= xsd-boolean datatype)
            (case lexical
              ("true" "1") true
              false)

            (and (nil? lang) (or (nil? datatype) (= xsd-string datatype)))
            (not= "" lexical)

            :else
            (if-let [n (term-numeric-value v)]
              (and (not (zero? (compare-numbers n 0)))
                   (not (and (instance? Double n) (Double/isNaN ^double n))))
              ::error)))

        ;; bare tokens: numeric → number rules, else non-empty (leniency)
        :other
        (if-let [n (bare-token-number (:value term))]
          (and (not (zero? (compare-numbers n 0)))
               (not (and (instance? Double n) (Double/isNaN ^double n))))
          (not= "" (:value term)))

        ;; IRI / bnode / triple term
        ::error))))

(defn evaluate-filter-cond
  "Evaluate a filter condition and return a plain boolean.
   Per SPARQL, a row passes only when the EBV is true — both false and a
   type error exclude the row. Any thrown exception is contained to the row
   (degrades to false) rather than aborting the whole distributed query."
  [expr binding-map]
  (try
    (true? (sparql-ebv (eval-expr expr binding-map)))
    (catch Exception _ false)))

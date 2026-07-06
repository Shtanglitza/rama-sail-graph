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
   both errors and unbound (but keeps a legitimate false)."
  (:require [clojure.string :as str]))

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

(defn- string-like-literal?
  "True for literals that compare as strings: simple literals and xsd:string."
  [term]
  (and (literal-of-kind? term)
       (nil? (:lang term))
       (or (nil? (:datatype term)) (= xsd-string (:datatype term)))))

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
  (if (or (instance? Double a) (instance? Double b))
    (Double/compare (double a) (double b))
    (.compareTo (bigdec a) (bigdec b))))

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

(defn- lexical-or-raw
  "String content of a term: lexical form for literals, raw value for bare
   tokens, nil for other kinds."
  [^String s]
  (let [term (parse-term s)]
    (case (:kind term)
      :literal (:lexical term)
      :other (:value term)
      nil)))

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

(defn- eq-terms
  "SPARQL RDFterm-equal adapted to canonical serialized terms.
   Returns true, false, or ::error.
   - identical serializations → true (canonical storage ⇒ same term)
   - both numeric → numeric value equality
   - both string-like literals → lexical equality
   - both literals with equal lang tags → lexical equality
   - both literals, same non-numeric datatype → lexical equality (extension)
   - both literals otherwise → ::error (incomparable per spec)
   - different term kinds → false"
  [^String l ^String r]
  (if (= l r)
    true
    (let [ln (term-numeric-value l)
          rn (term-numeric-value r)]
      (if (and ln rn)
        (zero? (compare-numbers ln rn))
        (let [lt (parse-term l)
              rt (parse-term r)]
          (cond
            (and (literal-of-kind? lt) (literal-of-kind? rt))
            (cond
              (and (string-like-literal? lt) (string-like-literal? rt))
              (= (:lexical lt) (:lexical rt))

              (and (:lang lt) (:lang rt))
              (and (.equalsIgnoreCase ^String (:lang lt) ^String (:lang rt))
                   (= (:lexical lt) (:lexical rt)))

              (and (:datatype lt) (= (:datatype lt) (:datatype rt)))
              (= (:lexical lt) (:lexical rt))

              :else ::error)

            ;; bare tokens compare by raw string against anything string-like
            (or (= :other (:kind lt)) (= :other (:kind rt)))
            (= (lexical-or-raw l) (lexical-or-raw r))

            ;; different kinds (iri vs literal vs bnode): not equal
            :else false))))))

(defn- order-compare-terms
  "Ordering comparison for :lt/:le/:gt/:ge. Returns a negative/zero/positive
   long, or ::error when the terms are not order-comparable.
   - both numeric → numeric comparison
   - both string-like literals → codepoint comparison of lexical forms
   - both IRIs → codepoint comparison (documented extension, consistent with
     the self-join execution path)
   - both literals with the same lang or same datatype → lexical comparison
     (extension; makes ISO dateTime ordering work)
   - anything else → ::error"
  [^String l ^String r]
  (let [ln (term-numeric-value l)
        rn (term-numeric-value r)]
    (if (and ln rn)
      (compare-numbers ln rn)
      (let [lt (parse-term l)
            rt (parse-term r)]
        (cond
          (and (string-like-literal? lt) (string-like-literal? rt))
          (compare (:lexical lt) (:lexical rt))

          (and (= :iri (:kind lt)) (= :iri (:kind rt)))
          (compare l r)

          (and (literal-of-kind? lt) (literal-of-kind? rt)
               (or (and (:lang lt) (.equalsIgnoreCase ^String (:lang lt)
                                                      ^String (or (:lang rt) "")))
                   (and (:datatype lt) (= (:datatype lt) (:datatype rt)))))
          (compare (:lexical lt) (:lexical rt))

          ;; bare tokens: compare string content leniently
          (or (= :other (:kind lt)) (= :other (:kind rt)))
          (compare (or (lexical-or-raw l) l) (or (lexical-or-raw r) r))

          :else ::error)))))

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
    (let [l (eval-expr (:left expr) bindings)
          r (eval-expr (:right expr) bindings)
          op (:op expr)]
      (if (or (nil? l) (error? l) (nil? r) (error? r))
        ::error
        (case op
          :eq (eq-terms l r)
          :ne (let [e (eq-terms l r)]
                (if (error? e) ::error (not e)))
          (let [c (order-compare-terms l r)]
            (if (error? c)
              ::error
              (case op
                :lt (neg? c)
                :le (not (pos? c))
                :gt (pos? c)
                :ge (not (neg? c))))))))

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
    (let [v (eval-expr (:arg expr) bindings)]
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
        (let [lang (lexical-or-raw lang-val)
              pat (lexical-or-raw pattern-val)]
          (if (or (nil? lang) (nil? pat))
            ::error
            (if (= pat "*")
              (not (str/blank? lang))
              ;; RFC 4647 basic filtering: case-insensitive prefix match
              (let [lang-lc (str/lower-case lang)
                    pat-lc (str/lower-case pat)]
                (or (= lang-lc pat-lc)
                    (str/starts-with? lang-lc (str pat-lc "-")))))))))

    :regex
    (let [v (eval-expr (:arg expr) bindings)
          pat-val (eval-expr (:pattern expr) bindings)
          flags-val (when (:flags expr) (eval-expr (:flags expr) bindings))]
      (if (or (nil? v) (error? v) (nil? pat-val) (error? pat-val)
              (error? flags-val))
        ::error
        (let [text (lexical-or-raw v)
              pattern (lexical-or-raw pat-val)
              flag-str (when flags-val (lexical-or-raw flags-val))]
          (if (or (nil? text) (nil? pattern))
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
        (= l r)))

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
                (let [e (eq-terms v m)]
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

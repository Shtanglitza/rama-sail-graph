(ns rama-sail.query.expr
  "Expression evaluation for SPARQL FILTER and BIND expressions.

   Evaluates expression trees against variable bindings, supporting:
   - Comparisons: :eq, :ne, :lt, :le, :gt, :ge (numeric and string)
   - Logic: :and, :or, :not
   - Math: :plus, :minus, :multiply, :divide
   - Functions: :str, :coalesce, :bound

   All values use N-Triples canonical format (e.g., \"\\\"value\\\"^^<datatype>\")."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [org.eclipse.rdf4j.model Triple]))

(declare sparql-ebv)

(defn parse-numeric
  "Parse a numeric value from N-Triples format.
   Handles plain numbers and typed literals like \"42\"^^<xsd:integer>.
   Returns nil if parsing fails.

   Optimized: Uses direct string manipulation instead of regex for performance."
  ^Double [^String s]
  (when s
    (try
      (let [clean (if (.startsWith s "\"")
                    ;; Find closing quote after opening quote
                    (let [end (.indexOf s "\"" 1)]
                      (if (pos? end)
                        (.substring s 1 end)
                        s))
                    s)]
        (Double/parseDouble clean))
      (catch NumberFormatException _ nil))))

(defn triple-term?
  "Check if a serialized N-Triples string represents a triple term (<< ... >>)."
  [^String s]
  (and s (.startsWith s "<< ") (.endsWith s " >>")))

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

(defn eval-expr
  "Evaluate an expression tree against a map of variable bindings.

   Expression types:
   - {:type :const :val \"...\"}     - Constant value
   - {:type :var :name \"?x\"}       - Variable lookup
   - {:type :cmp :op :eq/:ne/... :left expr :right expr} - Comparison
   - {:type :logic :op :and/:or :left expr :right expr}  - Logic
   - {:type :logic :op :not :arg expr}                   - Negation
   - {:type :math :op :plus/... :left expr :right expr}  - Arithmetic
   - {:type :str :arg expr}          - STR() function
   - {:type :coalesce :args [exprs]} - COALESCE() function
   - {:type :bound :var \"?x\"}      - BOUND() function

   Returns the evaluated value in N-Triples format, or nil/false for failures."
  [expr bindings]
  (case (:type expr)
    :const (:val expr)
    :var   (get bindings (:name expr))
    :cmp
    (let [l-raw (eval-expr (:left expr) bindings)
          r-raw (eval-expr (:right expr) bindings)
          op    (:op expr)]
      (if (and l-raw r-raw)
        (let [l-num (parse-numeric l-raw)
              r-num (parse-numeric r-raw)]
          (if (and l-num r-num)
            ;; Numeric Comparison
            (case op
              :eq (= l-num r-num)
              :ne (not= l-num r-num)
              :lt (< l-num r-num)
              :le (<= l-num r-num)
              :gt (> l-num r-num)
              :ge (>= l-num r-num))
            ;; String Comparison (Fallback)
            (case op
              :eq (= l-raw r-raw)
              :ne (not= l-raw r-raw)
              ;; Basic string comparisons for IRIs/Literals
              ;; Cache compare result to avoid multiple calls
              (let [cmp (compare l-raw r-raw)]
                (case op
                  :lt (neg? cmp)
                  :le (not (pos? cmp))
                  :gt (pos? cmp)
                  :ge (not (neg? cmp))
                  false)))))
        false)) ;; If any operand matches nothing (nil), fail

    :logic
    (case (:op expr)
      :and (and (sparql-ebv (eval-expr (:left expr) bindings))
                (sparql-ebv (eval-expr (:right expr) bindings)))
      :or  (or (sparql-ebv (eval-expr (:left expr) bindings))
               (sparql-ebv (eval-expr (:right expr) bindings)))
      :not (not (sparql-ebv (eval-expr (:arg expr) bindings))))

    ;; --- BIND Expression Support ---
    :math
    (let [l (parse-numeric (eval-expr (:left expr) bindings))
          r (parse-numeric (eval-expr (:right expr) bindings))]
      (when (and l r)
        ;; Compute result, handling division by zero per SPARQL semantics (returns error/unbound)
        (let [result (case (:op expr)
                       :plus (+ l r)
                       :minus (- l r)
                       :multiply (* l r)
                       :divide (when-not (zero? r) (/ l r)))]
          (when result
            ;; Return as N-Triples typed literal
            (str "\"" result "\"^^<http://www.w3.org/2001/XMLSchema#double>")))))

    :str
    (let [v (eval-expr (:arg expr) bindings)]
      (when v
        ;; Extract label from literal, IRI, or BNode, return as plain literal
        (cond
          ;; Literal: "value"@lang or "value"^^type -> extract value
          ;; Find the closing quote of the lexical value (before @ or ^^)
          (str/starts-with? v "\"")
          (let [;; Find first @ or ^^ after position 1 (after opening quote)
                at-pos (str/index-of v "@" 1)
                caret-pos (str/index-of v "^^" 1)
                ;; Determine where the suffix starts (if any)
                suffix-start (cond
                               (and at-pos caret-pos) (min at-pos caret-pos)
                               at-pos at-pos
                               caret-pos caret-pos
                               :else nil)
                ;; The closing quote is right before the suffix, or at end if no suffix
                end-quote (if suffix-start
                            (dec suffix-start)
                            (str/last-index-of v "\""))]
            (str "\"" (subs v 1 end-quote) "\""))
          ;; IRI: <uri> -> return uri as string literal
          (str/starts-with? v "<")
          (str "\"" (subs v 1 (dec (count v))) "\"")
          ;; BNode: _:id -> return id (without _: prefix) as string literal per SPARQL semantics
          (str/starts-with? v "_:")
          (str "\"" (subs v 2) "\"")
          ;; Otherwise return as-is wrapped in quotes
          :else (str "\"" v "\""))))

    :coalesce
    ;; Use some for early termination - stops at first non-nil value
    (some #(eval-expr % bindings) (:args expr))

    :bound
    (some? (get bindings (:var expr)))

    :if
    (let [cond-val (sparql-ebv (eval-expr (:condition expr) bindings))]
      (if cond-val
        (eval-expr (:then expr) bindings)
        (eval-expr (:else expr) bindings)))

    :type-check
    (let [v (eval-expr (:arg expr) bindings)]
      (when v
        (case (:check expr)
          :is-iri (and (str/starts-with? v "<") (not (str/starts-with? v "<< ")))
          :is-bnode (str/starts-with? v "_:")
          :is-literal (str/starts-with? v "\"")
          :is-numeric (some? (parse-numeric v))
          :is-triple (triple-term? v))))

    :lang
    (let [v (eval-expr (:arg expr) bindings)]
      (if v
        (if (str/starts-with? v "\"")
          ;; Look for @lang tag: "text"@en
          (let [at-pos (str/last-index-of v "@")]
            (if (and at-pos (> at-pos 0)
                     ;; Make sure @ is after closing quote and not inside ^^<...>
                     (not (str/includes? (subs v at-pos) "^^")))
              (str "\"" (subs v (inc at-pos)) "\"")
              "\"\""))
          "\"\"")
        "\"\""))

    :datatype
    (let [v (eval-expr (:arg expr) bindings)]
      (when v
        (if (str/starts-with? v "\"")
          ;; Look for ^^<type>
          (let [caret-pos (str/index-of v "^^" 1)]
            (if caret-pos
              (subs v (+ caret-pos 2))  ;; Return <type>
              ;; Check for language tag -> rdf:langString
              (let [at-pos (str/last-index-of v "@")]
                (if (and at-pos (> at-pos 0))
                  "<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>"
                  "<http://www.w3.org/2001/XMLSchema#string>"))))
          nil)))

    :langmatches
    (let [lang-val (eval-expr (:left expr) bindings)
          pattern-val (eval-expr (:right expr) bindings)]
      (when (and lang-val pattern-val)
        ;; Extract string content from N-Triples literals
        (let [extract-str (fn [s]
                            (if (and (str/starts-with? s "\"") (>= (count s) 2))
                              (let [end (str/last-index-of s "\"")]
                                (if (and end (> end 0))
                                  (subs s 1 end)
                                  s))
                              s))
              lang (extract-str lang-val)
              pat (extract-str pattern-val)]
          (if (= pat "*")
            (not (str/blank? lang))
            ;; RFC 4647 basic filtering: case-insensitive prefix match
            (let [lang-lc (str/lower-case lang)
                  pat-lc (str/lower-case pat)]
              (or (= lang-lc pat-lc)
                  (str/starts-with? lang-lc (str pat-lc "-"))))))))

    :regex
    (let [v (eval-expr (:arg expr) bindings)
          pat-val (eval-expr (:pattern expr) bindings)
          flags-val (when (:flags expr) (eval-expr (:flags expr) bindings))]
      (when (and v pat-val)
        ;; Extract string content
        (let [extract-str (fn [s]
                            (if (and (str/starts-with? s "\"") (>= (count s) 2))
                              (let [end (str/last-index-of s "\"")]
                                (if (and end (> end 0))
                                  (subs s 1 end)
                                  s))
                              s))
              text (extract-str v)
              pattern (extract-str pat-val)
              flag-str (when flags-val (extract-str flags-val))
              flags-int (if flag-str
                          (reduce (fn [acc ch]
                                    (case ch
                                      \i (bit-or acc java.util.regex.Pattern/CASE_INSENSITIVE)
                                      \m (bit-or acc java.util.regex.Pattern/MULTILINE)
                                      \s (bit-or acc java.util.regex.Pattern/DOTALL)
                                      \x (bit-or acc java.util.regex.Pattern/COMMENTS)
                                      acc))
                                  0 flag-str)
                          0)
              compiled (java.util.regex.Pattern/compile pattern flags-int)]
          (.find (.matcher compiled text)))))

    :same-term
    (let [l (eval-expr (:left expr) bindings)
          r (eval-expr (:right expr) bindings)]
      (and (some? l) (some? r) (= l r)))

    :in
    (let [v (eval-expr (:arg expr) bindings)]
      (when v
        (let [v-num (parse-numeric v)]
          (boolean
           (some (fn [member-expr]
                   (let [m (eval-expr member-expr bindings)]
                     (when m
                       (if v-num
                         (when-let [m-num (parse-numeric m)]
                           (= v-num m-num))
                         (= v m)))))
                 (:set expr))))))

    ;; --- RDF-star Triple Term Functions ---

    :is-triple
    (let [v (eval-expr (:arg expr) bindings)]
      (triple-term? v))

    :triple-subject
    (let [v (eval-expr (:arg expr) bindings)]
      (extract-triple-term-part v 0))

    :triple-predicate
    (let [v (eval-expr (:arg expr) bindings)]
      (extract-triple-term-part v 1))

    :triple-object
    (let [v (eval-expr (:arg expr) bindings)]
      (extract-triple-term-part v 2))

    :triple-constructor
    (let [s (eval-expr (:subject expr) bindings)
          p (eval-expr (:predicate expr) bindings)
          o (eval-expr (:object expr) bindings)]
      (when (and s p o)
        (str "<< " s " " p " " o " >>")))

    ;; Default: log warning and return nil for unknown expressions
    (do
      (log/warn "Unhandled expression type:" (:type expr) "- returning nil. Expression:" (pr-str expr))
      nil)))

(defn sparql-ebv
  "Convert a value to SPARQL Effective Boolean Value (EBV).
   Rules per SPARQL spec:
   - true/false (Java boolean) -> as-is
   - xsd:boolean \"true\"/\"false\" typed literals -> parsed
   - Numeric 0 / NaN -> false, other numbers -> true
   - Empty string literal -> false, non-empty -> true
   - nil -> false"
  [v]
  (cond
    (nil? v) false
    (instance? Boolean v) (boolean v)
    (not (string? v)) (boolean v)
    ;; xsd:boolean typed literal
    (str/ends-with? v "^^<http://www.w3.org/2001/XMLSchema#boolean>")
    (str/includes? v "\"true\"")
    ;; Numeric: 0 and NaN are false
    :else (if-let [n (parse-numeric v)]
            (and (not (zero? n)) (not (Double/isNaN n)))
            ;; Plain string literal: empty is false
            (if (str/starts-with? v "\"")
              (not= v "\"\"")
              ;; IRIs, BNodes etc. are truthy
              true))))

(defn evaluate-filter-cond
  "Evaluate a filter condition expression and return boolean result.
   Uses SPARQL Effective Boolean Value (EBV) semantics."
  [expr binding-map]
  (sparql-ebv (eval-expr expr binding-map)))

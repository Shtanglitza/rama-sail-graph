(ns ^:no-doc rama-sail.sail.serialization
  (:require [clojure.string :as str]
            [rama-sail.terms :as terms])
  (:import (java.util Locale)
           (org.eclipse.rdf4j.model.vocabulary XSD)
           [org.eclipse.rdf4j.model Value Resource IRI Literal Literal$BaseDirection BNode TripleTerm ValueFactory]
           [org.eclipse.rdf4j.model.impl SimpleValueFactory]
           [org.eclipse.rdf4j.query.impl MapBindingSet SimpleBinding]))

(def DEFAULT-CONTEXT-VAL terms/DEFAULT-CONTEXT-VAL)

(def ^ValueFactory VF (SimpleValueFactory/getInstance))

;;; -----------------------------------------------------------------------------
;;; 1. Canonical String Serialization (N-Triples Style)
;;; -----------------------------------------------------------------------------

(defn- escape-str [s]
  (-> s
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")
      (str/replace "\n" "\\n")
      (str/replace "\r" "\\r")))

;; XSD integer types that should be canonicalized to Long
(def ^:private xsd-integer-types
  #{XSD/INTEGER XSD/INT XSD/LONG XSD/SHORT XSD/BYTE
    XSD/NON_NEGATIVE_INTEGER XSD/NON_POSITIVE_INTEGER
    XSD/POSITIVE_INTEGER XSD/NEGATIVE_INTEGER
    XSD/UNSIGNED_INT XSD/UNSIGNED_LONG XSD/UNSIGNED_SHORT XSD/UNSIGNED_BYTE})

(defn- canonicalize-literal
  "Canonicalize a typed literal's label for consistent storage and comparison.
   - Integer types: Remove leading zeros, normalize sign (e.g., '01' -> '1', '+5' -> '5')
   - Decimal: Normalize to canonical form
   - Float/Double: Normalize to canonical form
   - Boolean: Normalize to 'true'/'false'
   Returns the original label if canonicalization fails or is not applicable."
  [^String label ^IRI dtype]
  (try
    (cond
      ;; Integer types: parse and format back
      (contains? xsd-integer-types dtype)
      (str (Long/parseLong label))

      ;; Decimal: use BigDecimal for precision
      (= dtype XSD/DECIMAL)
      (let [bd (java.math.BigDecimal. label)]
        (.toPlainString (.stripTrailingZeros bd)))

      ;; Double: parse and format
      (= dtype XSD/DOUBLE)
      (let [d (Double/parseDouble label)]
        (if (Double/isFinite d)
          (if (== d (Math/floor d))
            ;; Whole number - format without unnecessary decimals. Locale/ROOT:
            ;; the default locale would write "1,0" on e.g. a German JVM (W9)
            (String/format Locale/ROOT "%.1f" (to-array [d]))
            (str d))
          ;; Special values: INF, -INF, NaN
          label))

      ;; Float: parse and format
      (= dtype XSD/FLOAT)
      (let [f (Float/parseFloat label)]
        (if (Float/isFinite f)
          (if (== f (Math/floor f))
            (String/format Locale/ROOT "%.1f" (to-array [(double f)]))
            (str f))
          label))

      ;; Boolean: normalize to lowercase
      (= dtype XSD/BOOLEAN)
      (let [lower (.toLowerCase (str/trim label) Locale/ROOT)]
        (cond
          (or (= lower "true") (= lower "1")) "true"
          (or (= lower "false") (= lower "0")) "false"
          :else label))

      ;; No canonicalization needed
      :else label)
    (catch Exception _
      ;; If parsing fails, return original label
      label)))

(defn val->str [^Value v]
  (cond
    (nil? v) nil

    ;; IRI: <http://example.com>
    (instance? IRI v)
    (str "<" (.toString v) ">")

    ;; BNode: _:node123
    (instance? BNode v)
    (str "_:" (.getID ^BNode v))

    ;; Triple term: << s p o >>
    (instance? TripleTerm v)
    (let [^TripleTerm t v]
      (str "<< " (val->str (.getSubject t)) " "
           (val->str (.getPredicate t)) " "
           (val->str (.getObject t)) " >>"))

    ;; Literal
    (instance? Literal v)
    (let [^Literal lit v
          label (.getLabel lit)
          lang-opt (.getLanguage lit)
          lang (.orElse lang-opt nil)
          dtype (.getDatatype lit)]
      (cond
        ;; Language-tagged literal: "text"@en, with a base direction
        ;; (RDF 1.2) as in N-Triples 1.2: "text"@ar--rtl
        lang
        (str "\"" (escape-str label) "\"@" lang
             (condp = (.getBaseDirection lit)
               Literal$BaseDirection/LTR "--ltr"
               Literal$BaseDirection/RTL "--rtl"
               ""))

        ;; Typed literal (non-string): "value"^^<datatype>
        (and dtype (not= dtype XSD/STRING))
        (let [canonical-label (canonicalize-literal label dtype)]
          (str "\"" (escape-str canonical-label) "\"^^<" (.toString ^IRI dtype) ">"))

        ;; Plain string literal: "text"
        :else
        (str "\"" (escape-str label) "\"")))

    ;; Fallback/Error
    :else (str v)))

;;; -----------------------------------------------------------------------------
;;; 2. Simple N-Triples Parser
;;; -----------------------------------------------------------------------------

(defn- unescape-str
  "Unescape N-Triples escape sequences in a single left-to-right pass. A
   backslash escapes the next character: \\\\ -> \\, \\\" -> \", \\n -> newline,
   \\r -> CR; any other backslash is passed through literally (matching the
   sequences escape-str produces). Scanning in one pass — rather than swapping a
   sentinel char in and out — means a literal that itself contains U+0000 (or any
   other character) survives unchanged, instead of a real NUL being rewritten to
   a backslash by the old placeholder trick."
  [^String s]
  (let [len (count s)
        sb (StringBuilder. len)]
    (loop [i 0]
      (if (>= i len)
        (.toString sb)
        (let [ch (.charAt s i)]
          (if (and (= ch \\) (< (inc i) len))
            (case (.charAt s (inc i))
              \n (do (.append sb \newline) (recur (+ i 2)))
              \r (do (.append sb \return) (recur (+ i 2)))
              \" (do (.append sb \") (recur (+ i 2)))
              \\ (do (.append sb \\) (recur (+ i 2)))
              ;; Unknown escape: keep the backslash, process the next char normally.
              (do (.append sb \\) (recur (inc i))))
            (do (.append sb ch)
                (recur (inc i)))))))))

(defn- skip-string-literal
  "Given the index of an opening double-quote in `s`, return the index just past
   the matching closing quote, honouring `\\\"` escapes. If the literal is
   unterminated, returns `len`."
  [^String s ^long start ^long len]
  (loop [j (inc start)]
    (cond
      (>= j len) len
      (= (.charAt s j) \\) (recur (+ j 2)) ;; skip the escaped char
      (= (.charAt s j) \") (inc j)
      :else (recur (inc j)))))

(defn- parse-triple-term-parts
  "Parse the inner content of a triple term (<< s p o >>) into three N-Triples term strings.
   Handles nested triple terms, IRIs, literals (with escapes), and blank nodes."
  [^String inner]
  (let [len (count inner)]
    (loop [pos 0
           parts []]
      (if (or (>= pos len) (= 3 (count parts)))
        parts
        (let [ch (.charAt inner pos)]
          (cond
            ;; Skip whitespace
            (Character/isWhitespace ch)
            (recur (inc pos) parts)

            ;; Nested triple term: << ... >>
            (and (< (inc pos) len)
                 (= ch \<)
                 (= (.charAt inner (inc pos)) \<))
            (let [;; Find matching >> accounting for nesting. Skip over string
                  ;; literals so a literal that contains "<<" or ">>" does not
                  ;; corrupt the depth count (would otherwise mis-locate the end
                  ;; and throw StringIndexOutOfBounds downstream).
                  end (loop [i (+ pos 2) depth 1]
                        (cond
                          (>= i (dec len)) (inc len) ;; malformed, return past end
                          (= (.charAt inner i) \")
                          (recur (skip-string-literal inner i len) depth)
                          (and (= (.charAt inner i) \<)
                               (< (inc i) len)
                               (= (.charAt inner (inc i)) \<))
                          (recur (+ i 2) (inc depth))
                          (and (= (.charAt inner i) \>)
                               (< (inc i) len)
                               (= (.charAt inner (inc i)) \>))
                          (if (= depth 1)
                            (+ i 2) ;; found matching >>
                            (recur (+ i 2) (dec depth)))
                          :else (recur (inc i) depth)))]
              (recur end (conj parts (subs inner pos end))))

            ;; IRI: <...>
            (= ch \<)
            (let [end (inc (.indexOf inner ">" (int pos)))]
              (recur end (conj parts (subs inner pos end))))

            ;; Literal: "..."[@lang | ^^<type>]
            (= ch \")
            (let [;; Find closing quote, handling escapes
                  close-quote (loop [i (inc pos)]
                                (cond
                                  (>= i len) i
                                  (= (.charAt inner i) \\) (recur (+ i 2)) ;; skip escaped char
                                  (= (.charAt inner i) \") i
                                  :else (recur (inc i))))
                  ;; Check for suffix after closing quote
                  end (if (>= (inc close-quote) len)
                        (inc close-quote)
                        (let [next-ch (.charAt inner (inc close-quote))]
                          (cond
                            ;; Language tag: @en
                            (= next-ch \@)
                            (let [space (.indexOf inner " " (int (inc close-quote)))]
                              (if (neg? space) len space))
                            ;; Datatype: ^^<...>
                            (= next-ch \^)
                            (let [gt (.indexOf inner ">" (int (inc close-quote)))]
                              (if (neg? gt) len (inc gt)))
                            :else (inc close-quote))))]
              (recur end (conj parts (subs inner pos end))))

            ;; Blank node: _:...
            (and (= ch \_)
                 (< (inc pos) len)
                 (= (.charAt inner (inc pos)) \:))
            (let [end (loop [i (+ pos 2)]
                        (if (or (>= i len) (Character/isWhitespace (.charAt inner i)))
                          i
                          (recur (inc i))))]
              (recur end (conj parts (subs inner pos end))))

            ;; Unexpected character — skip
            :else (recur (inc pos) parts)))))))

(defn triple-term-parts
  "The subject, predicate and object strings of a canonical triple term
   `<< s p o >>`, or nil when `s` is not one."
  [^String s]
  (when (and s (.startsWith s "<< ") (.endsWith s " >>"))
    (let [parts (parse-triple-term-parts (subs s 3 (- (count s) 3)))]
      (when (= 3 (count parts)) parts))))

(defn subject-triple-term-error
  "The error for a triple term stored in subject position: RDF 1.2 allows a
   triple term only as an object, and rama-sail.migrate rewrites such data."
  [^String s]
  (ex-info (str "A triple term is stored in subject position, which RDF 1.2 (RDF4J 6) does not "
                "allow: " s ". Rewrite the store with rama-sail.migrate (see the README, "
                "\"Before upgrading to RDF4J 6\") on the previous release.")
           {:type ::subject-triple-term :term s}))

(declare str->val)

(defn str->resource
  "str->val for a subject or graph position, where a triple term cannot be."
  ^Resource [^String s]
  (if (and s (.startsWith s "<< "))
    (throw (subject-triple-term-error s))
    (str->val s)))

(defn str->val
  "Parse an N-Triples formatted string into an RDF4J Value.
   Throws IllegalArgumentException for malformed input, and
   subject-triple-term-error for a triple term with a triple term as its
   subject."
  [^String s]
  (cond
    (nil? s) nil

    ;; Triple term: << s p o >>
    (.startsWith s "<< ")
    (let [inner (subs s 3 (- (count s) 3))  ;; strip "<< " and " >>"
          ;; Parse three N-Triples terms from the inner string.
          ;; Terms are: IRI (<...>), BNode (_:...), Literal ("..."), or nested triple term (<< ... >>)
          parts (parse-triple-term-parts inner)]
      (when (= 3 (count parts))
        (when (.startsWith ^String (nth parts 0) "<< ")
          (throw (subject-triple-term-error s)))
        (.createTripleTerm VF
                           ^Resource (str->val (nth parts 0))
                           ^IRI (str->val (nth parts 1))
                           (str->val (nth parts 2)))))

    ;; IRI: <...>
    (.startsWith s "<")
    (if (.endsWith s ">")
      (.createIRI VF (subs s 1 (dec (count s))))
      (throw (IllegalArgumentException.
              (str "Malformed IRI: missing closing '>': " s))))

    ;; BNode: _:...
    (.startsWith s "_:")
    (.createBNode VF (subs s 2))

    ;; Literal: "..."
    (.startsWith s "\"")
    (let [last-quote (.lastIndexOf s "\"")]
      (when (< last-quote 1)
        (throw (IllegalArgumentException.
                (str "Malformed literal: missing closing '\"': " s))))
      (if (= last-quote (dec (count s)))
        ;; Simple Literal: "foo"
        (.createLiteral VF ^String (unescape-str (subs s 1 last-quote)))

        ;; Typed or Lang Literal
        (let [suffix (subs s (inc last-quote))
              ^String label (unescape-str (subs s 1 last-quote))]
          (cond
            ;; Lang: "foo"@en, or with a base direction: "foo"@en--ltr
            (.startsWith suffix "@")
            (let [tag (subs suffix 1)
                  dir (.lastIndexOf tag "--")]
              (if (neg? dir)
                (.createLiteral VF label ^String tag)
                (.createLiteral VF label ^String (subs tag 0 dir)
                                (case (subs tag (+ dir 2))
                                  "ltr" Literal$BaseDirection/LTR
                                  "rtl" Literal$BaseDirection/RTL
                                  (throw (IllegalArgumentException.
                                          (str "Malformed base direction: " s)))))))

            ;; Typed: "foo"^^<...>
            (.startsWith suffix "^^<")
            (if (.endsWith suffix ">")
              (let [dtype-uri (subs suffix 3 (dec (count suffix)))]
                (.createLiteral VF label ^IRI (.createIRI VF dtype-uri)))
              (throw (IllegalArgumentException.
                      (str "Malformed typed literal: missing closing '>' in datatype: " s))))

            :else (.createLiteral VF label)))))

    ;; Fallback (should generally not happen with valid data)
    :else (.createLiteral VF ^String s)))

(defn quad->tuple [^Resource s ^IRI p ^Value o ^Resource c]
  [(val->str s) (val->str p) (val->str o) (if c (val->str c) DEFAULT-CONTEXT-VAL)])

(defn rama-result->binding-set [rama-map]
  (let [bs (MapBindingSet.)]
    (doseq [[k v] rama-map]
      (let [var-name (if (str/starts-with? k "?") (subs k 1) k)
            val      (str->val v)]
        (when (and val (not= v DEFAULT-CONTEXT-VAL)) ;; Don't bind the default context value
          (.addBinding bs (SimpleBinding. var-name val)))))
    bs))

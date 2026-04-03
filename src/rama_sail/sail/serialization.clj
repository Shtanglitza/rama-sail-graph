(ns rama-sail.sail.serialization
  (:require [clojure.string :as str])
  (:import (org.eclipse.rdf4j.model.vocabulary XSD)
           [org.eclipse.rdf4j.model Value Resource IRI Literal BNode Triple ValueFactory]
           [org.eclipse.rdf4j.model.impl SimpleValueFactory]
           [org.eclipse.rdf4j.query.impl MapBindingSet SimpleBinding]))

;; Internal marker for default graph context. Uses a non-IRI format that cannot
;; be created through normal RDF4J APIs, preventing user data collision.
;; The "::" prefix is invalid in IRIs, making this a safe internal-only value.
(def DEFAULT-CONTEXT-VAL "::rama-internal::default-graph")

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
            ;; Whole number - format without unnecessary decimals
            (format "%.1f" d)
            (str d))
          ;; Special values: INF, -INF, NaN
          label))

      ;; Float: parse and format
      (= dtype XSD/FLOAT)
      (let [f (Float/parseFloat label)]
        (if (Float/isFinite f)
          (if (== f (Math/floor f))
            (format "%.1f" (double f))
            (str f))
          label))

      ;; Boolean: normalize to lowercase
      (= dtype XSD/BOOLEAN)
      (let [lower (str/lower-case (str/trim label))]
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

    ;; Triple term (RDF-star): << s p o >>
    (instance? Triple v)
    (let [^Triple t v]
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
        ;; Language-tagged literal: "text"@en
        lang
        (str "\"" (escape-str label) "\"@" lang)

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
  "Unescape N-Triples escape sequences. Process escaped backslashes first
   using a placeholder to avoid interference with other escape sequences.
   E.g., '\\\\n' (escaped backslash + n) should become '\\n' (backslash + n),
   not backslash + newline."
  [s]
  (-> s
      (str/replace "\\\\" "\u0000")  ; placeholder for escaped backslash
      (str/replace "\\\"" "\"")
      (str/replace "\\n" "\n")
      (str/replace "\\r" "\r")
      (str/replace "\u0000" "\\")))

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
            (let [;; Find matching >> accounting for nesting
                  end (loop [i (+ pos 2) depth 1]
                        (cond
                          (>= i (dec len)) (inc len) ;; malformed, return past end
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

(defn str->val
  "Parse an N-Triples formatted string into an RDF4J Value.
   Throws IllegalArgumentException for malformed input."
  [^String s]
  (cond
    (nil? s) nil

    ;; Triple term (RDF-star): << s p o >>
    (.startsWith s "<< ")
    (let [inner (subs s 3 (- (count s) 3))  ;; strip "<< " and " >>"
          ;; Parse three N-Triples terms from the inner string.
          ;; Terms are: IRI (<...>), BNode (_:...), Literal ("..."), or nested Triple (<< ... >>)
          parts (parse-triple-term-parts inner)]
      (when (= 3 (count parts))
        (.createTriple VF
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
            ;; Lang: "foo"@en
            (.startsWith suffix "@")
            (.createLiteral VF label ^String (subs suffix 1))

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

(defn triple->tuple [^Resource s ^IRI p ^Value o]
  [(val->str s) (val->str p) (val->str o)])

(defn quad->tuple [^Resource s ^IRI p ^Value o ^Resource c]
  [(val->str s) (val->str p) (val->str o) (if c (val->str c) DEFAULT-CONTEXT-VAL)])

;; BNode Skolemization
;; In RDF, blank nodes are locally scoped. When _:b1 is used in one transaction
;; and _:b1 in another, they should be distinct nodes. We achieve this by
;; generating unique IDs per connection/transaction.

(defn skolemize-bnode
  "Get or create a skolemized ID for a blank node within a transaction.
   The bnode-map tracks original BNode IDs to their unique skolemized IDs.
   This ensures that within a single transaction, the same _:id refers to
   the same node, but across transactions they are distinct."
  [^BNode bnode bnode-map]
  (let [original-id (.getID bnode)]
    (if-let [skolem-id (get @bnode-map original-id)]
      skolem-id
      (let [new-id (str original-id "-" (java.util.UUID/randomUUID))]
        (swap! bnode-map assoc original-id new-id)
        new-id))))

(defn skolemize-value
  "Convert an RDF value to its canonical string representation with BNode skolemization.
   For BNodes, uses the bnode-map to generate unique IDs per transaction.
   For Triple terms, recursively skolemizes inner BNodes."
  [^Value v bnode-map]
  (cond
    (instance? BNode v)
    (str "_:" (skolemize-bnode v bnode-map))

    (instance? Triple v)
    (let [^Triple t v]
      (str "<< " (skolemize-value (.getSubject t) bnode-map) " "
           (val->str (.getPredicate t)) " "
           (skolemize-value (.getObject t) bnode-map) " >>"))

    :else (val->str v)))

(defn skolemized-quad->tuple
  "Create a quad tuple with BNode skolemization applied."
  [^Resource s ^IRI p ^Value o ^Resource c bnode-map]
  [(skolemize-value s bnode-map)
   (val->str p)  ;; Predicates are always IRIs, never BNodes
   (skolemize-value o bnode-map)
   (if c (skolemize-value c bnode-map) DEFAULT-CONTEXT-VAL)])

(defn rama-result->binding-set [rama-map]
  (let [bs (MapBindingSet.)]
    (doseq [[k v] rama-map]
      (let [var-name (if (str/starts-with? k "?") (subs k 1) k)
            val      (str->val v)]
        (when (and val (not= v DEFAULT-CONTEXT-VAL)) ;; Don't bind the default context value
          (.addBinding bs (SimpleBinding. var-name val)))))
    bs))

(ns rama-sail.sail.serialization-roundtrip-test
  "Generative round-trip tests for the canonical N-Triples-style serialization
   in rama-sail.sail.serialization.

   For every generated RDF4J Value v the following must hold:
     value-level:  (= v (str->val (val->str v)))          [RDF4J .equals]
     string-level: (= (val->str v) (val->str (str->val (val->str v))))

   Numeric/boolean typed literals are canonicalized by design (e.g. \"007\" ->
   \"7\"), so value-level equality is deliberately NOT asserted for them; only
   string-level idempotence (stability of the canonical form) is checked.

   No test.check dependency exists in this project, so generation is
   hand-rolled with java.util.Random and a FIXED SEED — failures are fully
   reproducible. Assertion messages include the failing value's toString."
  (:require [clojure.test :refer :all]
            [rama-sail.sail.serialization :refer [val->str str->val]])
  (:import (java.util Random)
           (org.eclipse.rdf4j.model IRI Value)
           (org.eclipse.rdf4j.model.impl SimpleValueFactory)
           (org.eclipse.rdf4j.model.vocabulary XSD)))

(def ^:private ^org.eclipse.rdf4j.model.ValueFactory VF
  (SimpleValueFactory/getInstance))

(def ^:private SEED 421994)

;;; ---------------------------------------------------------------------------
;;; Adversarial building blocks
;;; ---------------------------------------------------------------------------

(def ^:private iri-strings
  "IRIs with unicode, percent-escapes, query strings and fragments."
  ["http://example.com"
   "http://example.com/"
   "http://example.com/path#fragment"
   "http://example.com/a/b/c?x=1&y=2#frag-ment"
   "http://example.com/%20space%2Fslash%3F"
   "http://example.com/%C3%A9%C3%A8"
   "http://example.com/π/résumé"
   "http://example.com/日本語/パス"
   "http://example.com/😀/emoji#🎉"
   "urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66"
   "mailto:user@example.com"
   "http://example.com/a#b#c"
   "http://example.com/~user_name-x.y+z"
   "https://example.com:8443/deep/path/../weird"])

(def ^:private adversarial-lexicals
  "Lexical forms designed to break naive serializers/parsers."
  [""                                       ;; empty string
   "simple"
   "with \"double quotes\""
   "\"leading and trailing\""
   "\""                                     ;; single double-quote
   "back\\slash"
   "\\"                                     ;; single backslash
   "ends with backslash\\"
   "\\\""                                   ;; backslash + quote
   "line1\nline2"
   "\n"
   "tab\there"
   "\t"
   "carriage\rreturn"
   "\r\n"
   "at@sign"
   "@en"                                    ;; looks like a bare language tag
   "x@en"
   "caret^^caret"
   "^^<http://www.w3.org/2001/XMLSchema#integer>"  ;; looks like a datatype suffix
   "<not-an-iri>"
   "<"
   ">"
   "a < b > c"
   "emoji 😀🎉🔥"
   "中文字符串"
   "日本語のテキスト"
   "mixed 中文 and English"
   "\"nested\"@en"                          ;; looks like a lang-tagged N-Triples literal
   "\"nested\"^^<http://ex/dt>"             ;; looks like a typed N-Triples literal
   "_:looks-like-a-bnode"
   "<< <s> <p> <o> >>"                      ;; looks like an RDF-star triple term
   "  surrounding whitespace  "
   ;; A label containing the NUL character U+0000 now round-trips: unescape-str
   ;; scans in a single pass instead of using U+0000 as an internal placeholder,
   ;; so a real NUL is preserved rather than corrupted into a backslash on parse.
   ;; (Directly covered by test-nul-character-roundtrip.)
   "combining é (é)"])

(def ^:private language-tags
  ["en" "en-US" "de" "fr-CA" "zh-Hans" "x-klingon" "en-GB-oed"])

(def ^:private non-numeric-datatype-iris
  "Typed-literal datatypes that the serializer does NOT canonicalize.
   Deliberately excludes numeric/boolean XSD types (integer, decimal, double,
   float, boolean, ...) whose labels are canonicalized by design."
  ["http://www.w3.org/2001/XMLSchema#date"
   "http://www.w3.org/2001/XMLSchema#dateTime"
   "http://www.w3.org/2001/XMLSchema#gYear"
   "http://www.w3.org/2001/XMLSchema#anyURI"
   "http://www.w3.org/2001/XMLSchema#hexBinary"
   "http://example.com/custom-datatype"
   "http://example.com/dt/π#日本語"
   "urn:my:datatype"])

(def ^:private bnode-ids
  ["b1" "node123" "genid-0af3" "a-b_c.d" "x" "0" "b-with-many-chars-0123456789"])

;;; ---------------------------------------------------------------------------
;;; Seeded generation (no test.check — hand-rolled, deterministic)
;;; ---------------------------------------------------------------------------

(defn- pick [^Random rnd coll]
  (nth coll (.nextInt rnd (count coll))))

(defn- gen-iri ^IRI [^Random rnd]
  (.createIRI VF ^String (pick rnd iri-strings)))

(defn- gen-plain-literal [^Random rnd]
  (.createLiteral VF ^String (pick rnd adversarial-lexicals)))

(defn- gen-lang-literal [^Random rnd]
  (.createLiteral VF
                  ^String (pick rnd adversarial-lexicals)
                  ^String (pick rnd language-tags)))

(defn- gen-typed-literal [^Random rnd]
  (.createLiteral VF
                  ^String (pick rnd adversarial-lexicals)
                  (.createIRI VF ^String (pick rnd non-numeric-datatype-iris))))

(defn- gen-bnode [^Random rnd]
  (.createBNode VF ^String (pick rnd bnode-ids)))

(defn- gen-simple-value
  "A value that may appear inside an RDF-star triple term."
  ^Value [^Random rnd]
  (case (.nextInt rnd 5)
    0 (gen-iri rnd)
    1 (gen-plain-literal rnd)
    2 (gen-lang-literal rnd)
    3 (gen-typed-literal rnd)
    4 (gen-bnode rnd)))

(defn- gen-triple-term
  "RDF-star triple value. Subject is a Resource (IRI/BNode), predicate an IRI,
   object any simple value — including nested triple terms and literals whose
   labels contain '<<' or '>>', which now round-trip after parse-triple-term-parts
   was fixed to skip string literals when scanning for the closing '>>'."
  ^Value [^Random rnd object-gen]
  (.createTriple VF
                 (if (zero? (.nextInt rnd 2)) (gen-iri rnd) (gen-bnode rnd))
                 (gen-iri rnd)
                 ^Value (object-gen rnd)))

(defn- gen-value ^Value [^Random rnd]
  (let [n (.nextInt rnd 12)]
    (cond
      (< n 2) (gen-iri rnd)
      (< n 4) (gen-plain-literal rnd)
      (< n 6) (gen-lang-literal rnd)
      (< n 8) (gen-typed-literal rnd)
      (< n 10) (gen-bnode rnd)
      (= n 10) (gen-triple-term rnd gen-simple-value)
      ;; nested triple-in-triple (object is itself a triple term). Uses the
      ;; unrestricted generator now that nested literals containing "<<"/">>"
      ;; round-trip (parse-triple-term-parts skips string literals).
      :else (gen-triple-term rnd
                             (fn [^Random r]
                               (gen-triple-term r gen-simple-value))))))

;;; ---------------------------------------------------------------------------
;;; Round-trip assertions
;;; ---------------------------------------------------------------------------

(defn- assert-roundtrip!
  "Full round-trip: value equality plus string idempotence."
  [^Value v]
  (let [s  (val->str v)
        v' (str->val s)]
    (is (= v v')
        (str "value-level roundtrip failed for value: " (pr-str (str v))
             "\n  serialized: " (pr-str s)
             "\n  parsed back: " (pr-str (str v'))))
    (when v'
      (is (= s (val->str v'))
          (str "string-level idempotence failed for value: " (pr-str (str v))
               "\n  first serialization:  " (pr-str s)
               "\n  second serialization: " (pr-str (val->str v')))))))

(deftest test-generated-values-roundtrip
  (testing "several hundred seeded adversarial values round-trip exactly"
    (let [rnd (Random. SEED)]
      (dotimes [_ 400]
        (assert-roundtrip! (gen-value rnd))))))

(deftest test-exhaustive-adversarial-literals
  (testing "every adversarial lexical round-trips as plain, lang-tagged and typed literal"
    (doseq [lexical adversarial-lexicals]
      (assert-roundtrip! (.createLiteral VF ^String lexical))
      (doseq [tag language-tags]
        (assert-roundtrip! (.createLiteral VF ^String lexical ^String tag)))
      (doseq [dt non-numeric-datatype-iris]
        (assert-roundtrip! (.createLiteral VF ^String lexical
                                           (.createIRI VF ^String dt)))))))

(deftest test-exhaustive-iris-and-bnodes
  (testing "every adversarial IRI and bnode id round-trips"
    (doseq [iri iri-strings]
      (assert-roundtrip! (.createIRI VF ^String iri)))
    (doseq [id bnode-ids]
      (assert-roundtrip! (.createBNode VF ^String id)))))

(deftest test-explicit-nested-triple-roundtrip
  (testing "a hand-built triple-in-triple round-trips"
    (let [inner (.createTriple VF
                               (.createIRI VF "http://ex/s2")
                               (.createIRI VF "http://ex/p2")
                               (.createLiteral VF "inner \"label\"\nwith@stuff^^<x>" "en-US"))
          outer (.createTriple VF
                               (.createBNode VF "outer")
                               (.createIRI VF "http://ex/π#p")
                               inner)]
      (assert-roundtrip! outer))))

(deftest test-numeric-literals-string-idempotence-only
  (testing "numeric/boolean literals: canonical form must be STABLE (value equality intentionally not required)"
    (let [cases [["007"    XSD/INTEGER]
                 ["+5"     XSD/INTEGER]
                 ["-042"   XSD/INTEGER]
                 ["42"     XSD/INT]
                 ["1.50"   XSD/DECIMAL]
                 ["0.000"  XSD/DECIMAL]
                 ["1e3"    XSD/DOUBLE]
                 ["2.5"    XSD/DOUBLE]
                 ["INF"    XSD/DOUBLE]
                 ["3.0"    XSD/FLOAT]
                 ["true"   XSD/BOOLEAN]
                 ["TRUE"   XSD/BOOLEAN]
                 ["1"      XSD/BOOLEAN]
                 ["not-a-number" XSD/INTEGER]]] ;; unparseable: label kept as-is
      (doseq [[label dtype] cases]
        (let [v  (.createLiteral VF ^String label ^IRI dtype)
              s  (val->str v)
              v' (str->val s)]
          (is (some? v')
              (str "numeric literal failed to parse back: " (pr-str (str v))
                   " serialized as " (pr-str s)))
          (is (= s (val->str v'))
              (str "canonicalization not stable for: " (pr-str (str v))
                   "\n  first serialization:  " (pr-str s)
                   "\n  second serialization: " (pr-str (val->str v')))))))))

(deftest test-nul-character-roundtrip
  ;; Regression: unescape-str used U+0000 as an internal placeholder, corrupting
  ;; any real NUL in a label into a backslash. The single-pass scanner preserves it.
  (testing "a literal label containing U+0000 round-trips"
    (assert-roundtrip! (.createLiteral VF ^String (str "null" (char 0) "char")))
    (assert-roundtrip! (.createLiteral VF ^String (str (char 0))))
    (assert-roundtrip! (.createLiteral VF ^String (str "a" (char 0) "b") "en"))
    (assert-roundtrip! (.createLiteral VF ^String (str "x" (char 0) "y")
                                       (.createIRI VF "http://ex/dt")))))

(deftest test-nested-triple-term-with-delimiter-literal-roundtrip
  ;; Regression: parse-triple-term-parts scanned nested << >> without skipping
  ;; string literals, so a nested literal containing >> or << threw
  ;; StringIndexOutOfBounds. The end-finder now skips literals.
  (testing "a nested triple whose inner literal contains << or >> round-trips"
    (let [inner (.createTriple VF
                               (.createIRI VF "http://ex/s2")
                               (.createIRI VF "http://ex/p2")
                               (.createLiteral VF "has << and >> inside"))
          outer (.createTriple VF
                               (.createIRI VF "http://ex/s1")
                               (.createIRI VF "http://ex/p1")
                               inner)]
      (assert-roundtrip! outer)))
  (testing "a nested literal that looks exactly like a triple term round-trips"
    (let [inner (.createTriple VF
                               (.createIRI VF "http://ex/s2")
                               (.createIRI VF "http://ex/p2")
                               (.createLiteral VF "<< <s> <p> <o> >>"))
          outer (.createTriple VF
                               (.createBNode VF "outer")
                               (.createIRI VF "http://ex/p1")
                               inner)]
      (assert-roundtrip! outer))))

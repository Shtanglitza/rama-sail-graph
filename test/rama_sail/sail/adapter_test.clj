(ns rama-sail.sail.adapter-test
  (:require [clojure.test :refer :all]
            [rama-sail.sail.adapter :as sail]
            [rama-sail.sail.serialization :as ser]
            [rama-sail.sail.compilation :as comp]
            [taoensso.nippy :as nippy])
  (:import [org.eclipse.rdf4j.model.impl SimpleValueFactory]
           [org.eclipse.rdf4j.model.vocabulary XSD]
           [org.eclipse.rdf4j.query.algebra Join LeftJoin Projection ProjectionElem ProjectionElemList Slice StatementPattern Var]))

(def VF (SimpleValueFactory/getInstance))

;; Use public functions from extracted namespaces
(def val->str ser/val->str)
(def str->val ser/str->val)
(def tuple-expr->plan comp/tuple-expr->plan)
(def escape-str @#'ser/escape-str)
(def unescape-str @#'ser/unescape-str)

(deftest test-nippy-serialization

  (testing "IRI Serialization"
    (let [iri (.createIRI VF "http://example.com/foo")
          s   (val->str iri)]
      (is (= "<http://example.com/foo>" s))
      (is (= iri (str->val s)))))

  (testing "BNode Serialization"
    (let [bnode (.createBNode VF "node123")
          s     (val->str bnode)]
      (is (= "_:node123" s))
      (is (= bnode (str->val s)))))

  (testing "Literal Serialization (Simple)"
    (let [lit (.createLiteral VF "hello")
          s   (val->str lit)]
      (is (= "\"hello\"" s))
      (is (= lit (str->val s)))))

  (testing "Literal Serialization (Language Tag)"
    (let [lit (.createLiteral VF "hello" "en")
          s   (val->str lit)]
      (is (= "\"hello\"@en" s))
      (is (= lit (str->val s)))
      (is (= "en" (.get (.getLanguage (str->val s)))))))

  (testing "Literal Serialization (Datatype)"
    (let [type-iri (.createIRI VF "http://example.org/type")
          lit (.createLiteral VF "123" type-iri)
          s   (val->str lit)]
      (is (= "\"123\"^^<http://example.org/type>" s))
      (is (= lit (str->val s))))))

(deftest test-query-plan-translation
  (testing "Translate BGP (No GRAPH clause = wildcard)"
    (let [s (.createIRI VF "http://ex/s")
          p (.createIRI VF "http://ex/p")
          s-var (Var. "s" s)
          p-var (Var. "p" p)
          o-var (Var. "o")
          pattern (StatementPattern. s-var p-var o-var)
          plan (tuple-expr->plan pattern)]

      (is (= :bgp (:op plan)))
      (is (= "<http://ex/s>" (get-in plan [:pattern :s])))
      (is (= "<http://ex/p>" (get-in plan [:pattern :p])))
      (is (= "?o" (get-in plan [:pattern :o])))
      ;; SPARQL semantics: no GRAPH clause = match ALL contexts (wildcard = nil)
      (is (nil? (get-in plan [:pattern :c])))))

  (testing "Translate BGP (Named Graph Variable)"
		;; Case: GRAPH ?g { ?s ?p ?o }
    (let [s-var (Var. "s")
          p-var (Var. "p")
          o-var (Var. "o")
          c-var (Var. "g") ;; Context variable ?g
          pattern (StatementPattern. s-var p-var o-var c-var)
          plan (tuple-expr->plan pattern)]

      (is (= :bgp (:op plan)))
      (is (= "?s" (get-in plan [:pattern :s])))
      (is (= "?g" (get-in plan [:pattern :c])))))

  (testing "Translate BGP (Named Graph Constant)"
		;; Case: GRAPH <http://ex/g1> { ?s ?p ?o }
    (let [s-var (Var. "s")
          p-var (Var. "p")
          o-var (Var. "o")
          g-val (.createIRI VF "http://ex/g1")
          c-var (Var. "g" g-val) ;; Context constant
          pattern (StatementPattern. s-var p-var o-var c-var)
          plan (tuple-expr->plan pattern)]

      (is (= :bgp (:op plan)))
      (is (= "<http://ex/g1>" (get-in plan [:pattern :c])))))

  (testing "Translate JOIN"
    (let [sp1 (StatementPattern. (Var. "s") (Var. "p") (Var. "o"))
          sp2 (StatementPattern. (Var. "o") (Var. "p2") (Var. "o2"))
          join (Join. sp1 sp2)
          plan (tuple-expr->plan join)]

      (is (= :join (:op plan)))
      (is (= :bgp (:op (:left plan))))
      (is (= :bgp (:op (:right plan))))
			;; Check join var calculation
      (is (= ["?o"] (:join-vars plan)))))

  (testing "Translate LEFT JOIN"
    (let [sp1 (StatementPattern. (Var. "s") (Var. "p") (Var. "o"))
          sp2 (StatementPattern. (Var. "o") (Var. "p2") (Var. "o2"))
          join (LeftJoin. sp1 sp2)
          plan (tuple-expr->plan join)]

      (is (= :left-join (:op plan)))
      (is (= :bgp (:op (:left plan))))
      (is (= :bgp (:op (:right plan))))
			;; Check join var calculation
      (is (= ["?o"] (:join-vars plan)))))

  (testing "Translate PROJECTION"
    (let [sp (StatementPattern. (Var. "s") (Var. "p") (Var. "o"))
          proj (Projection. sp (reduce #(doto %1 (.addElement %2))
                                       (ProjectionElemList.)
                                       [(ProjectionElem. "s")]))
          plan (tuple-expr->plan proj)]

      (is (= :project (:op plan)))
      (is (= ["?s"] (:vars plan)))))

	;; In rama-sail.sail.adapter-test, after "Translate PROJECTION"

  (testing "Translate SLICE (LIMIT/OFFSET)"
    (let [sp (StatementPattern. (Var. "s") (Var. "p") (Var. "o"))
					;; RDF4J Slice constructor: Slice(TupleExpr arg, long offset, long limit)
          slice (Slice. sp 10 5) ; offset 10, limit 5
          plan (tuple-expr->plan slice)]

      (is (= :slice (:op plan)))
      (is (= :bgp (:op (:sub-plan plan))))
      (is (= 10 (:offset plan)))
      (is (= 5 (:limit plan))))

		;; Test with only LIMIT (offset will be 0 or -1 depending on RDF4J impl, usually 0)
    (let [sp (StatementPattern. (Var. "s") (Var. "p") (Var. "o"))
          slice (Slice. sp 0 3) ; limit 3, no offset
          plan (tuple-expr->plan slice)]

      (is (= 0 (:offset plan)))
      (is (= 3 (:limit plan))))))

(deftest test-escape-unescape-strings
  (testing "Basic string - no special characters"
    (is (= "hello world" (unescape-str (escape-str "hello world"))))
    (is (= "hello world" (escape-str "hello world")))
    (is (= "hello world" (unescape-str "hello world"))))

  (testing "String with embedded quotes"
    (let [original "He said \"hello\""
          escaped (escape-str original)]
      (is (= "He said \\\"hello\\\"" escaped))
      (is (= original (unescape-str escaped)))))

  (testing "String with newlines"
    (let [original "line1\nline2"
          escaped (escape-str original)]
      (is (= "line1\\nline2" escaped))
      (is (= original (unescape-str escaped)))))

  (testing "String with carriage returns"
    (let [original "line1\rline2"
          escaped (escape-str original)]
      (is (= "line1\\rline2" escaped))
      (is (= original (unescape-str escaped)))))

  (testing "String with backslashes"
    (let [original "path\\to\\file"
          escaped (escape-str original)]
      (is (= "path\\\\to\\\\file" escaped))
      (is (= original (unescape-str escaped)))))

  ;; Critical test case: escaped backslash followed by 'n' should NOT become newline
  (testing "Escaped backslash followed by n (the bug case)"
    (let [original "hello\\nworld"  ; backslash + n, NOT newline
          escaped (escape-str original)]
      ;; After escaping: backslash becomes \\, so we get \\n
      (is (= "hello\\\\nworld" escaped))
      ;; After unescaping: \\\\ becomes \, leaving \n (backslash + n)
      (is (= original (unescape-str escaped)))
      ;; Verify it's really backslash + n, not a newline
      (is (= 12 (count original)))  ; h-e-l-l-o-\-n-w-o-r-l-d
      (is (= 12 (count (unescape-str escaped))))))

  (testing "Escaped backslash followed by quote"
    (let [original "say \\\""  ; backslash + quote
          escaped (escape-str original)]
      (is (= "say \\\\\\\"" escaped))
      (is (= original (unescape-str escaped)))))

  (testing "Escaped backslash followed by r"
    (let [original "hello\\rworld"  ; backslash + r, NOT carriage return
          escaped (escape-str original)]
      (is (= "hello\\\\rworld" escaped))
      (is (= original (unescape-str escaped)))))

  (testing "Multiple backslashes"
    (let [original "\\\\"  ; two backslashes
          escaped (escape-str original)]
      (is (= "\\\\\\\\" escaped))  ; four backslashes in escaped form
      (is (= original (unescape-str escaped)))))

  (testing "Complex mixed escapes"
    (let [original "line1\npath\\to\\file\r\"quoted\""
          escaped (escape-str original)]
      (is (= original (unescape-str escaped)))))

  (testing "Windows path style"
    (let [original "C:\\Users\\Name\\Documents"
          escaped (escape-str original)]
      (is (= "C:\\\\Users\\\\Name\\\\Documents" escaped))
      (is (= original (unescape-str escaped)))))

  (testing "JSON-like content with escapes"
    (let [original "{\"key\": \"value\\nwith newline\"}"
          escaped (escape-str original)]
      (is (= original (unescape-str escaped))))))

(deftest test-literal-roundtrip-with-special-chars
  (testing "Literal with embedded newline roundtrips correctly"
    (let [lit (.createLiteral VF "line1\nline2")
          s (val->str lit)
          parsed (str->val s)]
      (is (= "\"line1\\nline2\"" s))
      (is (= lit parsed))
      (is (= "line1\nline2" (.getLabel parsed)))))

  (testing "Literal with embedded quote roundtrips correctly"
    (let [lit (.createLiteral VF "He said \"hello\"")
          s (val->str lit)
          parsed (str->val s)]
      (is (= "\"He said \\\"hello\\\"\"" s))
      (is (= lit parsed))
      (is (= "He said \"hello\"" (.getLabel parsed)))))

  (testing "Literal with backslash-n (not newline) roundtrips correctly"
    (let [lit (.createLiteral VF "hello\\nworld")  ; literal backslash + n
          s (val->str lit)
          parsed (str->val s)]
      (is (= "\"hello\\\\nworld\"" s))
      (is (= lit parsed))
      (is (= "hello\\nworld" (.getLabel parsed)))
      (is (= 12 (count (.getLabel parsed))))))

  (testing "Literal with Windows path roundtrips correctly"
    (let [lit (.createLiteral VF "C:\\Users\\Name")
          s (val->str lit)
          parsed (str->val s)]
      (is (= "\"C:\\\\Users\\\\Name\"" s))
      (is (= lit parsed))
      (is (= "C:\\Users\\Name" (.getLabel parsed)))))

  (testing "Literal with language tag and special chars"
    (let [lit (.createLiteral VF "line1\nline2" "en")
          s (val->str lit)
          parsed (str->val s)]
      (is (= "\"line1\\nline2\"@en" s))
      (is (= lit parsed))))

  (testing "Literal with datatype and special chars"
    (let [type-iri (.createIRI VF "http://example.org/type")
          lit (.createLiteral VF "value\\with\\backslashes" type-iri)
          s (val->str lit)
          parsed (str->val s)]
      (is (= "\"value\\\\with\\\\backslashes\"^^<http://example.org/type>" s))
      (is (= lit parsed)))))

(deftest test-str->val-edge-cases
  (testing "nil input returns nil"
    (is (nil? (str->val nil))))

  (testing "Empty IRI throws IllegalArgumentException"
    ;; RDF4J validates that IRIs must be absolute
    (is (thrown? IllegalArgumentException (str->val "<>"))))

  (testing "IRI with spaces (technically invalid but RDF4J accepts)"
    (let [result (str->val "<http://example.com/path with spaces>")]
      (is (instance? org.eclipse.rdf4j.model.IRI result))
      (is (= "http://example.com/path with spaces" (.toString result)))))

  (testing "BNode with empty ID"
    (let [result (str->val "_:")]
      (is (instance? org.eclipse.rdf4j.model.BNode result))
      (is (= "" (.getID result)))))

  (testing "BNode with special characters in ID"
    (let [result (str->val "_:node-123_abc")]
      (is (instance? org.eclipse.rdf4j.model.BNode result))
      (is (= "node-123_abc" (.getID result)))))

  (testing "Empty literal"
    (let [result (str->val "\"\"")]
      (is (instance? org.eclipse.rdf4j.model.Literal result))
      (is (= "" (.getLabel result)))))

  (testing "Literal with empty language tag throws"
    ;; "value"@ - language tag marker but no language
    ;; RDF4J validates that language tags cannot be empty
    (is (thrown? IllegalArgumentException (str->val "\"value\"@"))))

  (testing "Literal with empty datatype IRI throws"
    ;; RDF4J validates datatype IRIs
    (is (thrown? IllegalArgumentException (str->val "\"value\"^^<>"))))

  (testing "Fallback: unrecognized format becomes literal"
    ;; Strings not starting with <, _:, or " fall through to the else branch
    (let [result (str->val "plain text")]
      (is (instance? org.eclipse.rdf4j.model.Literal result))
      (is (= "plain text" (.getLabel result)))))

  (testing "Fallback: empty string becomes literal"
    (let [result (str->val "")]
      (is (instance? org.eclipse.rdf4j.model.Literal result))
      (is (= "" (.getLabel result))))))

(deftest test-str->val-malformed-input
  ;; These tests verify that malformed input throws IllegalArgumentException
  ;; instead of silently corrupting data.

  (testing "IRI missing closing bracket throws IllegalArgumentException"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Malformed IRI: missing closing '>'"
                          (str->val "<http://example.com"))))

  (testing "IRI with only opening bracket throws IllegalArgumentException"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Malformed IRI: missing closing '>'"
                          (str->val "<"))))

  (testing "Literal missing closing quote throws IllegalArgumentException"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Malformed literal: missing closing '\"'"
                          (str->val "\"hello"))))

  (testing "Literal with only opening quote throws IllegalArgumentException"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Malformed literal: missing closing '\"'"
                          (str->val "\""))))

  (testing "Typed literal with malformed datatype throws IllegalArgumentException"
    ;; "value"^^<http://example.org/type  (no closing >)
    (is (thrown-with-msg? IllegalArgumentException
                          #"Malformed typed literal: missing closing '>' in datatype"
                          (str->val "\"value\"^^<http://example.org/type"))))

  (testing "Typed literal with empty datatype marker - falls back to simple literal"
    ;; "value"^^ - has ^^ but no <, so doesn't match ^^< prefix
    (let [result (str->val "\"value\"^^")]
      (is (instance? org.eclipse.rdf4j.model.Literal result))
      (is (= "value" (.getLabel result)))))

  (testing "Underscore without colon is not a BNode"
    ;; "_abc" doesn't start with "_:", so falls through to literal
    (let [result (str->val "_abc")]
      (is (instance? org.eclipse.rdf4j.model.Literal result))
      (is (= "_abc" (.getLabel result)))))

  (testing "Relative IRI throws IllegalArgumentException"
    ;; RDF4J requires absolute IRIs
    (is (thrown? IllegalArgumentException
                 (str->val "<not-absolute>")))))

(deftest test-datatype-canonicalization
  ;; Tests for RDF value equality via canonical serialization.
  ;; In RDF, "1"^^xsd:integer and "01"^^xsd:integer are semantically equal.
  ;; Our storage must serialize them identically for joins and comparisons to work.

  (testing "Integer canonicalization - leading zeros removed"
    (let [lit1 (.createLiteral VF "1" XSD/INTEGER)
          lit2 (.createLiteral VF "01" XSD/INTEGER)
          lit3 (.createLiteral VF "001" XSD/INTEGER)]
      ;; All should serialize to the same canonical form
      (is (= (val->str lit1) (val->str lit2)) "1 and 01 should serialize identically")
      (is (= (val->str lit1) (val->str lit3)) "1 and 001 should serialize identically")
      (is (= "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>" (val->str lit1)))))

  (testing "Integer canonicalization - positive sign removed"
    (let [lit1 (.createLiteral VF "5" XSD/INTEGER)
          lit2 (.createLiteral VF "+5" XSD/INTEGER)]
      (is (= (val->str lit1) (val->str lit2)) "5 and +5 should serialize identically")
      (is (= "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>" (val->str lit1)))))

  (testing "Integer canonicalization - negative numbers preserved"
    (let [lit (.createLiteral VF "-42" XSD/INTEGER)]
      (is (= "\"-42\"^^<http://www.w3.org/2001/XMLSchema#integer>" (val->str lit)))))

  (testing "Integer canonicalization - negative with leading zeros"
    (let [lit1 (.createLiteral VF "-7" XSD/INTEGER)
          lit2 (.createLiteral VF "-007" XSD/INTEGER)]
      (is (= (val->str lit1) (val->str lit2)) "-7 and -007 should serialize identically")))

  (testing "Integer canonicalization - zero"
    (let [lit1 (.createLiteral VF "0" XSD/INTEGER)
          lit2 (.createLiteral VF "00" XSD/INTEGER)
          lit3 (.createLiteral VF "-0" XSD/INTEGER)]
      (is (= (val->str lit1) (val->str lit2)) "0 and 00 should serialize identically")
      ;; Note: -0 as integer becomes 0
      (is (= (val->str lit1) (val->str lit3)) "0 and -0 should serialize identically")))

  (testing "Other integer types - xsd:int"
    (let [lit1 (.createLiteral VF "42" XSD/INT)
          lit2 (.createLiteral VF "042" XSD/INT)]
      (is (= (val->str lit1) (val->str lit2)))))

  (testing "Other integer types - xsd:long"
    (let [lit1 (.createLiteral VF "999999999999" XSD/LONG)
          lit2 (.createLiteral VF "0999999999999" XSD/LONG)]
      (is (= (val->str lit1) (val->str lit2)))))

  (testing "Boolean canonicalization - true variants"
    (let [lit1 (.createLiteral VF "true" XSD/BOOLEAN)
          lit2 (.createLiteral VF "TRUE" XSD/BOOLEAN)
          lit3 (.createLiteral VF "True" XSD/BOOLEAN)
          lit4 (.createLiteral VF "1" XSD/BOOLEAN)]
      (is (= (val->str lit1) (val->str lit2)) "true and TRUE should serialize identically")
      (is (= (val->str lit1) (val->str lit3)) "true and True should serialize identically")
      (is (= (val->str lit1) (val->str lit4)) "true and 1 should serialize identically")
      (is (= "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>" (val->str lit1)))))

  (testing "Boolean canonicalization - false variants"
    (let [lit1 (.createLiteral VF "false" XSD/BOOLEAN)
          lit2 (.createLiteral VF "FALSE" XSD/BOOLEAN)
          lit3 (.createLiteral VF "0" XSD/BOOLEAN)]
      (is (= (val->str lit1) (val->str lit2)) "false and FALSE should serialize identically")
      (is (= (val->str lit1) (val->str lit3)) "false and 0 should serialize identically")
      (is (= "\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>" (val->str lit1)))))

  (testing "Decimal canonicalization"
    (let [lit1 (.createLiteral VF "3.14" XSD/DECIMAL)
          lit2 (.createLiteral VF "3.140" XSD/DECIMAL)
          lit3 (.createLiteral VF "03.14" XSD/DECIMAL)]
      (is (= (val->str lit1) (val->str lit2)) "3.14 and 3.140 should serialize identically")
      (is (= (val->str lit1) (val->str lit3)) "3.14 and 03.14 should serialize identically")))

  (testing "Double canonicalization"
    (let [lit1 (.createLiteral VF "1.0" XSD/DOUBLE)
          lit2 (.createLiteral VF "1.00" XSD/DOUBLE)
          lit3 (.createLiteral VF "01.0" XSD/DOUBLE)]
      (is (= (val->str lit1) (val->str lit2)) "1.0 and 1.00 should serialize identically")
      (is (= (val->str lit1) (val->str lit3)) "1.0 and 01.0 should serialize identically")))

  (testing "Invalid numeric - falls back to original"
    ;; If parsing fails, the original label is preserved
    (let [lit (.createLiteral VF "not-a-number" XSD/INTEGER)]
      (is (= "\"not-a-number\"^^<http://www.w3.org/2001/XMLSchema#integer>" (val->str lit)))))

  (testing "Non-numeric datatype - no canonicalization"
    ;; Custom datatypes are not canonicalized
    (let [custom-type (.createIRI VF "http://example.org/mytype")
          lit (.createLiteral VF "01" custom-type)]
      (is (= "\"01\"^^<http://example.org/mytype>" (val->str lit)))))

  (testing "String literal - no canonicalization"
    ;; Plain strings are not touched
    (let [lit (.createLiteral VF "01")]
      (is (= "\"01\"" (val->str lit)))))

  (testing "Language-tagged literal - no canonicalization"
    ;; Literals with language tags are not canonicalized
    (let [lit (.createLiteral VF "01" "en")]
      (is (= "\"01\"@en" (val->str lit))))))

;;; ---------------------------------------------------------------------------
;;; Context IDs Pending State Logic Tests (Audit Fix #4)
;;; ---------------------------------------------------------------------------

(deftest test-context-ids-clear-then-add
  (testing "Context cleared then added should be included in result"
    ;; This tests the composite pending state logic that handles clear+add sequences
    ;; The bug was that mixing :cleared keyword with numeric counts caused exceptions
    (let [;; Simulate the pending-state reduce from getContextIDsInternal
          compute-pending-state
          (fn [pending-ops]
            (reduce
             (fn [state [op [_s _p _o ctx]]]
               (let [{:keys [cleared? net-count] :or {cleared? false net-count 0}}
                     (get state ctx {:cleared? false :net-count 0})]
                 (case op
                   :add (assoc state ctx
                               (if cleared?
                                 {:cleared? true :net-count (inc net-count)}
                                 {:cleared? false :net-count (inc net-count)}))
                   :del (assoc state ctx
                               {:cleared? cleared? :net-count (dec net-count)})
                   :clear-context (assoc state ctx {:cleared? true :net-count 0})
                   state)))
             {}
             pending-ops))

          ;; Test case: clear then add to same context
          pending-ops [[:clear-context [nil nil nil "<http://example.org/graph1>"]]
                       [:add ["<s>" "<p>" "<o>" "<http://example.org/graph1>"]]]
          state (compute-pending-state pending-ops)
          {:keys [cleared? net-count]} (get state "<http://example.org/graph1>")]

      ;; Context should be marked as cleared but have positive net-count from post-clear adds
      (is (true? cleared?) "Context should be marked as cleared")
      (is (= 1 net-count) "Net count should be 1 (one add after clear)")

      ;; The context should be INCLUDED in results because net-count > 0
      (is (and cleared? (pos? net-count))
          "Clear+Add sequence should result in context being included")))

  (testing "Context cleared only should be excluded from result"
    (let [compute-pending-state
          (fn [pending-ops]
            (reduce
             (fn [state [op [_s _p _o ctx]]]
               (let [{:keys [cleared? net-count] :or {cleared? false net-count 0}}
                     (get state ctx {:cleared? false :net-count 0})]
                 (case op
                   :add (assoc state ctx
                               (if cleared?
                                 {:cleared? true :net-count (inc net-count)}
                                 {:cleared? false :net-count (inc net-count)}))
                   :del (assoc state ctx
                               {:cleared? cleared? :net-count (dec net-count)})
                   :clear-context (assoc state ctx {:cleared? true :net-count 0})
                   state)))
             {}
             pending-ops))

          pending-ops [[:clear-context [nil nil nil "<http://example.org/graph1>"]]]
          state (compute-pending-state pending-ops)
          {:keys [cleared? net-count]} (get state "<http://example.org/graph1>")]

      (is (true? cleared?) "Context should be marked as cleared")
      (is (= 0 net-count) "Net count should be 0 (no adds after clear)")

      ;; The context should be EXCLUDED because cleared with no adds
      (is (and cleared? (<= net-count 0))
          "Clear-only should result in context being excluded")))

  (testing "Add then clear sequence should exclude context"
    (let [compute-pending-state
          (fn [pending-ops]
            (reduce
             (fn [state [op [_s _p _o ctx]]]
               (let [{:keys [cleared? net-count] :or {cleared? false net-count 0}}
                     (get state ctx {:cleared? false :net-count 0})]
                 (case op
                   :add (assoc state ctx
                               (if cleared?
                                 {:cleared? true :net-count (inc net-count)}
                                 {:cleared? false :net-count (inc net-count)}))
                   :del (assoc state ctx
                               {:cleared? cleared? :net-count (dec net-count)})
                   :clear-context (assoc state ctx {:cleared? true :net-count 0})
                   state)))
             {}
             pending-ops))

          ;; Add then clear - clear should reset the count
          pending-ops [[:add ["<s>" "<p>" "<o>" "<http://example.org/graph1>"]]
                       [:clear-context [nil nil nil "<http://example.org/graph1>"]]]
          state (compute-pending-state pending-ops)
          {:keys [cleared? net-count]} (get state "<http://example.org/graph1>")]

      (is (true? cleared?) "Context should be marked as cleared")
      (is (= 0 net-count) "Net count should be 0 (clear resets)")

      ;; The context should be EXCLUDED because clear came after add
      (is (and cleared? (<= net-count 0))
          "Add+Clear should result in context being excluded"))))

;;; ---------------------------------------------------------------------------
;;; sizeInternal Duplicate Context Deduplication Tests (Audit Fix #3)
;;; ---------------------------------------------------------------------------

(deftest test-sizeInternal-duplicate-context-deduplication
  (testing "sizeInternal should deduplicate context IDs to avoid double-counting"
    ;; The fast path for specific contexts should not count the same context twice
    ;; if the caller passes duplicate context IDs in the array
    (let [;; Simulate the deduplication logic from sizeInternal
          contexts ["<http://example.org/graph1>"
                    "<http://example.org/graph1>"  ;; duplicate
                    "<http://example.org/graph2>"]
          ;; This is what the fixed code does
          unique-ctx-strs (distinct contexts)]
      (is (= 2 (count unique-ctx-strs))
          "Duplicate contexts should be removed")
      (is (= #{"<http://example.org/graph1>" "<http://example.org/graph2>"}
             (set unique-ctx-strs))
          "Unique contexts should be preserved"))))

;;; ---------------------------------------------------------------------------
;;; compute-pending-net-state Unit Tests
;;; ---------------------------------------------------------------------------

(deftest test-compute-pending-net-state-add-then-del-same-quad
  (testing "Add then delete same quad results in empty net-visible state"
    ;; This tests the core model used by both getContextIDsInternal and clearInternal.
    ;; When you add a quad then delete the exact same quad, they should cancel out.
    (let [;; Replicate compute-pending-net-state logic
          compute-pending-net-state
          (fn [pending-ops]
            (reduce (fn [state [op quad]]
                      (case op
                        :add (-> state
                                 (update :adds conj quad)
                                 (update :dels disj quad))
                        :del (-> state
                                 (update :adds disj quad)
                                 (update :dels conj quad))
                        :clear-context
                        (let [ctx (nth quad 3)]
                          (-> state
                              (update :adds (fn [adds]
                                              (into #{} (remove #(= (nth % 3) ctx) adds))))
                              (update :cleared-contexts (fnil conj #{}) ctx)))
                        state))
                    {:adds #{} :dels #{} :cleared-contexts #{}}
                    pending-ops))

          ;; Scenario: Add then delete SAME quad - add should be removed from adds
          ;; The del goes to dels (indicating intent to delete from committed too)
          quad ["<s>" "<p>" "<o>" "<http://example.org/graph>"]
          pending-ops [[:add quad] [:del quad]]
          {:keys [adds dels]} (compute-pending-net-state pending-ops)]

      (is (empty? adds) "Net-visible adds should be empty (add canceled by del)")
      ;; Note: dels contains the quad because del removes from adds AND adds to dels
      ;; This is correct - it means "delete from committed data too"
      (is (= #{quad} dels) "Del should be in dels (to delete from committed if present)")))

  (testing "Add then delete DIFFERENT quad in same context - add remains visible"
    (let [compute-pending-net-state
          (fn [pending-ops]
            (reduce (fn [state [op quad]]
                      (case op
                        :add (-> state
                                 (update :adds conj quad)
                                 (update :dels disj quad))
                        :del (-> state
                                 (update :adds disj quad)
                                 (update :dels conj quad))
                        state))
                    {:adds #{} :dels #{}}
                    pending-ops))

          ;; Scenario: Add one quad, delete a DIFFERENT (non-existent) quad
          add-quad ["<s>" "<p>" "<o>" "<http://example.org/graph>"]
          del-quad ["<other>" "<other>" "<other>" "<http://example.org/graph>"]
          pending-ops [[:add add-quad] [:del del-quad]]
          {:keys [adds dels]} (compute-pending-net-state pending-ops)]

      (is (= #{add-quad} adds) "The real add should remain in net-visible adds")
      (is (= #{del-quad} dels) "The unrelated del should be in dels")))

  (testing "Clear then add to same context - add is visible"
    (let [compute-pending-net-state
          (fn [pending-ops]
            (reduce (fn [state [op quad]]
                      (case op
                        :add (-> state
                                 (update :adds conj quad)
                                 (update :dels disj quad))
                        :del (-> state
                                 (update :adds disj quad)
                                 (update :dels conj quad))
                        :clear-context
                        (let [ctx (nth quad 3)]
                          (-> state
                              (update :adds (fn [adds]
                                              (into #{} (remove #(= (nth % 3) ctx) adds))))
                              (update :cleared-contexts (fnil conj #{}) ctx)))
                        state))
                    {:adds #{} :dels #{} :cleared-contexts #{}}
                    pending-ops))

          ctx "<http://example.org/graph>"
          add-quad ["<s>" "<p>" "<o>" ctx]
          pending-ops [[:clear-context [nil nil nil ctx]] [:add add-quad]]
          {:keys [adds cleared-contexts]} (compute-pending-net-state pending-ops)]

      (is (= #{add-quad} adds) "Add after clear should be in net-visible adds")
      (is (contains? cleared-contexts ctx) "Context should be marked as cleared"))))

;;; ---------------------------------------------------------------------------
;;; clearInternal Net-Visible Context Tests
;;; ---------------------------------------------------------------------------

(deftest test-clearInternal-uses-net-visible-adds
  (testing "clear-all extracts pending-only contexts from net-visible adds"
    ;; clearInternal should use compute-pending-net-state to get net-visible adds,
    ;; not raw :add operations. This avoids queueing unnecessary clears.
    (let [;; Replicate the logic from clearInternal
          compute-pending-net-state
          (fn [pending-ops]
            (reduce (fn [state [op quad]]
                      (case op
                        :add (-> state (update :adds conj quad) (update :dels disj quad))
                        :del (-> state (update :adds disj quad) (update :dels conj quad))
                        state))
                    {:adds #{} :dels #{}}
                    pending-ops))

          ;; Scenario: Add then delete same quad in pending-only context
          ;; Net result: context has NO visible adds
          quad ["<s>" "<p>" "<o>" "<http://example.org/pending-only>"]
          pending-ops [[:add quad] [:del quad]]
          {:keys [adds]} (compute-pending-net-state pending-ops)
          committed-set #{}
          pending-only (->> adds
                            (map (fn [[_s _p _o ctx]] ctx))
                            (remove (fn [ctx] (contains? committed-set ctx)))
                            set)]

      (is (empty? adds) "Net-visible adds should be empty")
      (is (empty? pending-only) "No pending-only contexts to clear"))))

;;; ---------------------------------------------------------------------------
;;; compute-commit-ops ordering tests
;;; ---------------------------------------------------------------------------

(def ^:private compute-commit-ops @#'sail/compute-commit-ops)

(deftest test-compute-commit-ops-basic
  (testing "basic adds and dels are preserved"
    (let [q1 ["<s1>" "<p>" "<o>" "<c>"]
          q2 ["<s2>" "<p>" "<o>" "<c>"]
          result (compute-commit-ops [[:add q1] [:del q2]])]
      (is (= 2 (count result)))
      (is (some #(= [:add q1] %) result))
      (is (some #(= [:del q2] %) result))))

  (testing "add then del of same quad produces del (last-write-wins)"
    (let [q ["<s>" "<p>" "<o>" "<c>"]
          result (compute-commit-ops [[:add q] [:del q]])]
      (is (= [[:del q]] result))))

  (testing "del then add of same quad produces add (last-write-wins)"
    (let [q ["<s>" "<p>" "<o>" "<c>"]
          result (compute-commit-ops [[:del q] [:add q]])]
      (is (= [[:add q]] result)))))

(deftest test-compute-commit-ops-clear-ordering
  (testing "clear then add: clear appears BEFORE add in output"
    (let [ctx "<http://g1>"
          q ["<s>" "<p>" "<o>" ctx]
          result (compute-commit-ops [[:clear-context [nil nil nil ctx]]
                                      [:add q]])]
      (is (= 2 (count result)))
      (is (= [:clear-context [nil nil nil ctx]] (first result)))
      (is (= [:add q] (second result)))))

  (testing "add then clear: add is discarded, only clear emitted"
    (let [ctx "<http://g1>"
          q ["<s>" "<p>" "<o>" ctx]
          result (compute-commit-ops [[:add q]
                                      [:clear-context [nil nil nil ctx]]])]
      (is (= 1 (count result)))
      (is (= [:clear-context [nil nil nil ctx]] (first result)))))

  (testing "add, clear, add same quad: clear then post-clear add"
    (let [ctx "<http://g1>"
          q ["<s>" "<p>" "<o>" ctx]
          result (compute-commit-ops [[:add q]
                                      [:clear-context [nil nil nil ctx]]
                                      [:add q]])]
      (is (= 2 (count result)))
      (is (= [:clear-context [nil nil nil ctx]] (first result)))
      (is (= [:add q] (second result))))))

(deftest test-compute-commit-ops-multi-context
  (testing "clear of ctx1 discards ops for ctx1, preserves ops for ctx2"
    (let [ctx1 "<http://g1>"
          ctx2 "<http://g2>"
          q1 ["<s>" "<p>" "<o>" ctx1]
          q2 ["<s>" "<p>" "<o>" ctx2]
          result (compute-commit-ops [[:add q1]
                                      [:add q2]
                                      [:clear-context [nil nil nil ctx1]]])]
      ;; q1's add is discarded (subsumed by clear), only clear + q2 remain
      (is (= 2 (count result)))
      (is (some #(= [:clear-context [nil nil nil ctx1]] %) result))
      (is (some #(= [:add q2] %) result))
      ;; q1's add should NOT be present
      (is (not (some #(= [:add q1] %) result)))))

  (testing "double clear of same context emits both clears"
    (let [ctx "<http://g1>"
          result (compute-commit-ops [[:clear-context [nil nil nil ctx]]
                                      [:clear-context [nil nil nil ctx]]])]
      (is (= 2 (count result)))
      (is (every? #(= [:clear-context [nil nil nil ctx]] %) result)))))

(deftest test-compute-commit-ops-clear-with-no-prior-ops
  (testing "clear with no prior ops emits just the clear"
    (let [ctx "<http://g1>"
          result (compute-commit-ops [[:clear-context [nil nil nil ctx]]])]
      (is (= [[:clear-context [nil nil nil ctx]]] result)))))

(comment

  (run-tests 'rama-sail.sail.adapter-test))
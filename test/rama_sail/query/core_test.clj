(ns rama-sail.query.core-test
  (:require [clojure.test :refer :all]
            [com.rpl.rama.aggs :as aggs]
            [com.rpl.rama.ops :as ops]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer :all]
            [rama-sail.query.expr :as expr])
  (:use [com.rpl.rama]
        [com.rpl.rama.path]))

(deftest test-filter-helpers
  (testing "parse-numeric helper"
    (is (= 30.0 (expr/parse-numeric "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"))
        "Should parse integer literals with datatype")
    (is (= 25.5 (expr/parse-numeric "\"25.5\""))
        "Should parse simple decimal strings in quotes")
    (is (= 10.0 (expr/parse-numeric "10"))
        "Should parse raw numeric strings")
    (is (nil? (expr/parse-numeric "\"Bob\""))
        "Should return nil for non-numeric strings"))

  (testing "evaluate-filter-cond logic"
    (let [eval-cond expr/evaluate-filter-cond
          bindings  {"?age" "\"30\"^^<int>"
                     "?name" "\"Bob\""
                     "?score" "\"10.5\""
                     "?g" "<http://ex/graph1>"}]

      (testing "Comparisons"
        (is (true? (eval-cond {:type :cmp :op :gt
                               :left {:type :var :name "?age"}
                               :right {:type :const :val "25"}} bindings))
            "30 > 25")

        (is (false? (eval-cond {:type :cmp :op :lt
                                :left {:type :var :name "?age"}
                                :right {:type :const :val "25"}} bindings))
            "30 < 25 should be false")

        (is (true? (eval-cond {:type :cmp :op :eq
                               :left {:type :var :name "?name"}
                               :right {:type :const :val "\"Bob\""}} bindings))
            "String equality")

				;; Tests for :le and :ge operators (critical bug fix)
        (is (true? (eval-cond {:type :cmp :op :le
                               :left {:type :var :name "?age"}
                               :right {:type :const :val "30"}} bindings))
            "30 <= 30 (numeric)")

        (is (true? (eval-cond {:type :cmp :op :le
                               :left {:type :var :name "?age"}
                               :right {:type :const :val "35"}} bindings))
            "30 <= 35 (numeric)")

        (is (false? (eval-cond {:type :cmp :op :le
                                :left {:type :var :name "?age"}
                                :right {:type :const :val "25"}} bindings))
            "30 <= 25 should be false (numeric)")

        (is (true? (eval-cond {:type :cmp :op :ge
                               :left {:type :var :name "?age"}
                               :right {:type :const :val "30"}} bindings))
            "30 >= 30 (numeric)")

        (is (true? (eval-cond {:type :cmp :op :ge
                               :left {:type :var :name "?age"}
                               :right {:type :const :val "25"}} bindings))
            "30 >= 25 (numeric)")

        (is (false? (eval-cond {:type :cmp :op :ge
                                :left {:type :var :name "?age"}
                                :right {:type :const :val "35"}} bindings))
            "30 >= 35 should be false (numeric)"))

      (testing "Context Variable Filtering"
        (is (true? (eval-cond {:type :cmp :op :eq
                               :left {:type :var :name "?g"}
                               :right {:type :const :val "<http://ex/graph1>"}} bindings))
            "Should be able to filter on graph variable"))

      (testing "Logic Operators"
        (is (true? (eval-cond {:type :logic :op :and
                               :left {:type :cmp :op :gt :left {:type :var :name "?age"} :right {:type :const :val "20"}}
                               :right {:type :cmp :op :eq :left {:type :var :name "?name"} :right {:type :const :val "\"Bob\""}}}
                              bindings))
            "AND logic")

        (is (true? (eval-cond {:type :logic :op :or
                               :left {:type :cmp :op :eq :left {:type :var :name "?name"} :right {:type :const :val "\"Alice\""}}
                               :right {:type :cmp :op :gt :left {:type :var :name "?age"} :right {:type :const :val "20"}}}
                              bindings))
            "OR logic (one side true)"))))

  ;; Tests for string comparison fallback (when numeric parsing fails)
  (testing "String comparison fallback for :le and :ge"
    (let [eval-cond expr/evaluate-filter-cond
          ;; Use IRIs which cannot be parsed as numbers
          bindings {"?uri1" "<http://example.org/a>"
                    "?uri2" "<http://example.org/b>"
                    "?uri3" "<http://example.org/a>"}]

      ;; :le string fallback
      (is (true? (eval-cond {:type :cmp :op :le
                             :left {:type :var :name "?uri1"}
                             :right {:type :var :name "?uri2"}} bindings))
          "IRI 'a' <= IRI 'b' (string comparison)")

      (is (true? (eval-cond {:type :cmp :op :le
                             :left {:type :var :name "?uri1"}
                             :right {:type :var :name "?uri3"}} bindings))
          "IRI 'a' <= IRI 'a' (equal strings)")

      (is (false? (eval-cond {:type :cmp :op :le
                              :left {:type :var :name "?uri2"}
                              :right {:type :var :name "?uri1"}} bindings))
          "IRI 'b' <= IRI 'a' should be false")

      ;; :ge string fallback
      (is (true? (eval-cond {:type :cmp :op :ge
                             :left {:type :var :name "?uri2"}
                             :right {:type :var :name "?uri1"}} bindings))
          "IRI 'b' >= IRI 'a' (string comparison)")

      (is (true? (eval-cond {:type :cmp :op :ge
                             :left {:type :var :name "?uri1"}
                             :right {:type :var :name "?uri3"}} bindings))
          "IRI 'a' >= IRI 'a' (equal strings)")

      (is (false? (eval-cond {:type :cmp :op :ge
                              :left {:type :var :name "?uri1"}
                              :right {:type :var :name "?uri2"}} bindings))
          "IRI 'a' >= IRI 'b' should be false"))))

;;; -----------------------------------------------------------------------------
;;; INTEGRATION TEST: Filter Query Topology
;;; -----------------------------------------------------------------------------

;; Define a dedicated test module to isolate the filter topology
;; We mock "execute-plan" to return static data so we don't need a real depot/PState
(defmodule FilterTestModule [setup topologies]
	;; 1. Register the filter topology we want to test
  (filter-query-topology topologies)

	;; 2. Mock 'execute-plan' to return a fixed set of bindings
  (<<query-topology topologies "execute-plan" [*plan :> *results]
		;; Return 3 rows: Alice (25), Bob (30), Charlie (20)
                    (identity [{"?name" "\"Alice\"" "?age" "\"25\""}
                               {"?name" "\"Bob\""   "?age" "\"30\""}
                               {"?name" "\"Charlie\"" "?age" "\"20\""}] :> *data)
                    (ops/explode *data :> *row)
                    (|origin)
                    (aggs/+set-agg *row :> *results)))

(deftest test-filter-topology-integration
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc FilterTestModule {:tasks 4 :threads 2})

    (let [module-name (get-module-name FilterTestModule)
          q-filter    (foreign-query ipc module-name "filter")]

      (testing "Filter Topology: Greater Than"
				;; Filter for age > 22
        (let [expr {:type :cmp :op :gt
                    :left {:type :var :name "?age"}
                    :right {:type :const :val "22"}}
							;; Pass a dummy sub-plan (ignored by our mock execute-plan)
              sub-plan {}
              results  (foreign-invoke-query q-filter sub-plan expr)]

          (is (= 2 (count results)))
          (let [names (set (map #(get % "?name") results))]
            (is (contains? names "\"Alice\""))
            (is (contains? names "\"Bob\""))
            (is (not (contains? names "\"Charlie\""))))))

      (testing "Filter Topology: Equality"
				;; Filter for name == "Charlie"
        (let [expr {:type :cmp :op :eq
                    :left {:type :var :name "?name"}
                    :right {:type :const :val "\"Charlie\""}}
              results (foreign-invoke-query q-filter {} expr)]

          (is (= 1 (count results)))
          (is (= "\"Charlie\"" (get (first results) "?name"))))))))

;;; -----------------------------------------------------------------------------
;;; INTEGRATION TEST: Slice Query Topology
;;; -----------------------------------------------------------------------------

;; Test module for slice topology with mocked execute-plan
(defmodule SliceTestModule [setup topologies]
  ;; Register the slice topology we want to test
  (slice-query-topology topologies)

  ;; Mock 'execute-plan' to return a fixed set of 5 bindings
  (<<query-topology topologies "execute-plan" [*plan :> *results]
                    (identity [{"?id" "1"} {"?id" "2"} {"?id" "3"} {"?id" "4"} {"?id" "5"}] :> *data)
                    (ops/explode *data :> *row)
                    (|origin)
                    (aggs/+set-agg *row :> *results)))

(deftest test-slice-topology-integration
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc SliceTestModule {:tasks 4 :threads 2})

    (let [module-name (get-module-name SliceTestModule)
          q-slice     (foreign-query ipc module-name "slice")]

      (testing "Slice: Normal offset and limit"
        ;; OFFSET 1, LIMIT 2 -> should get 2 results
        (let [results (foreign-invoke-query q-slice {} 1 2)]
          (is (= 2 (count results)) "Should return 2 results with OFFSET 1 LIMIT 2")))

      (testing "Slice: Zero offset"
        ;; OFFSET 0, LIMIT 3 -> first 3 results
        (let [results (foreign-invoke-query q-slice {} 0 3)]
          (is (= 3 (count results)) "Should return 3 results with OFFSET 0 LIMIT 3")))

      (testing "Slice: Offset exceeds total (per SPARQL spec, returns empty)"
        ;; OFFSET 10 (> 5 total), LIMIT 2 -> empty
        (let [results (foreign-invoke-query q-slice {} 10 2)]
          (is (empty? results) "Should return empty when offset exceeds total results")))

      (testing "Slice: Negative offset treated as 0 (per RDF4J spec)"
        ;; OFFSET -1 (not set), LIMIT 2 -> from start
        (let [results (foreign-invoke-query q-slice {} -1 2)]
          (is (= 2 (count results)) "Negative offset should be treated as 0")))

      (testing "Slice: Negative limit means no limit (per RDF4J spec)"
        ;; OFFSET 1, LIMIT -1 (not set) -> all from offset 1
        (let [results (foreign-invoke-query q-slice {} 1 -1)]
          (is (= 4 (count results)) "Negative limit should return all remaining results")))

      (testing "Slice: Limit exceeds remaining results"
        ;; OFFSET 3, LIMIT 10 -> only 2 remaining
        (let [results (foreign-invoke-query q-slice {} 3 10)]
          (is (= 2 (count results)) "Should return only remaining results when limit exceeds")))

      (testing "Slice: Zero limit returns empty"
        ;; OFFSET 0, LIMIT 0 -> empty
        (let [results (foreign-invoke-query q-slice {} 0 0)]
          (is (empty? results) "Zero limit should return empty set"))))))

;;; -----------------------------------------------------------------------------
;;; 2. INTEGRATION TEST: RdfStorageModule (Quads / Named Graphs)
;;; -----------------------------------------------------------------------------

(deftest test-rdf-storage-module-named-graphs
	;; Create an In-Process Cluster (IPC)
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})

    (let [module-name (get-module-name RdfStorageModule)

					;; --- Clients ---
          depot      (foreign-depot ipc module-name "*triple-depot")
          p-spoc     (foreign-pstate ipc module-name "$$spoc")
          p-posc     (foreign-pstate ipc module-name "$$posc")
          p-ospc     (foreign-pstate ipc module-name "$$ospc")
          p-cspo     (foreign-pstate ipc module-name "$$cspo")

          q-triples  (foreign-query ipc module-name "find-triples")
          q-bgp      (foreign-query ipc module-name "find-bgp")
          q-plan     (foreign-query ipc module-name "execute-plan")]

			;; ===================================================================
			;; 1. TEST: ETL & Quad PState Indexing
			;; ===================================================================
      (testing "ETL: Ingestion of Quads"
        (let [tx-time (System/currentTimeMillis)]
          ;; Default Graph Quad (Context = DEFAULT-CONTEXT-VAL)
          (foreign-append! depot [:add ["<alice>" "<knows>" "<bob>" DEFAULT-CONTEXT-VAL] tx-time])
          ;; Named Graph Quad (Context = <g1>)
          (foreign-append! depot [:add ["<bob>" "<age>" "30" "<g1>"] tx-time])
          ;; Named Graph Quad (Context = <g2>)
          (foreign-append! depot [:add ["<charlie>" "<knows>" "<dave>" "<g2>"] tx-time]))

				;; Wait for microbatch "indexer" to process 3 records
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3))

      (testing "PState Verification ($$spoc, $$posc, $$cspo)"
				;; 1. Verify $$spoc (S->P->O->Set<C>) for Alice (Default Graph)
        (is (= #{DEFAULT-CONTEXT-VAL}
               (set (foreign-select [(keypath "<alice>" "<knows>" "<bob>") ALL] p-spoc)))
            "Alice->knows->bob should be in the default context")

				;; 2. Verify $$spoc for Bob (Named Graph <g1>)
        (is (= #{"<g1>"}
               (set (foreign-select [(keypath "<bob>" "<age>" "30") ALL] p-spoc)))
            "Bob->age->30 should be in <g1>")

				;; 3. Verify $$cspo (C->S->P->Set<O>) - Graph-centric view
        (is (= #{"<dave>"}
               (set (foreign-select [(keypath "<g2>" "<charlie>" "<knows>") ALL] p-cspo)))
            "Graph <g2> should contain charlie->knows->dave")

				;; 4. Verify $$posc (P->O->S->Set<C>) - Predicate-centric view
        (is (= #{"<g1>"}
               (set (foreign-select [(keypath "<age>" "30" "<bob>") ALL] p-posc)))
            "Reverse lookup on Age 30 should point to <g1>"))

			;; ===================================================================
			;; 2. TEST: 'find-triples' (Low-level 4-pattern match)
			;; ===================================================================
      (testing "Query: find-triples (Quad matching)"
				;; Pattern: Exact Match (S P O C)
        (is (= [["<bob>" "<age>" "30" "<g1>"]]
               (foreign-invoke-query q-triples "<bob>" "<age>" "30" "<g1>")))

				;; Pattern: Wildcard Context (S P O ?)
				;; Should find Alice in default graph
        (is (= [["<alice>" "<knows>" "<bob>" DEFAULT-CONTEXT-VAL]]
               (foreign-invoke-query q-triples "<alice>" "<knows>" "<bob>" nil)))

				;; Pattern: Graph Query (? ? ? C)
				;; Find everything in <g2>
        (let [res (foreign-invoke-query q-triples nil nil nil "<g2>")]
          (is (= [["<charlie>" "<knows>" "<dave>" "<g2>"]] res))
          (is (= 1 (count res))))

				;; Pattern: Predicate in any graph (? P ? ?)
        (let [res (foreign-invoke-query q-triples nil "<knows>" nil nil)]
					;; Should find Alice (default) and Charlie (<g2>)
          (is (= 2 (count res)))
          (let [subjects (set (map first res))]
            (is (contains? subjects "<alice>"))
            (is (contains? subjects "<charlie>")))))

			;; ===================================================================
			;; 3. TEST: 'find-bgp' (Bindings & Context Variables)
			;; ===================================================================
      (testing "Query: find-bgp (Variable Bindings)"
				;; 1. Find who Alice knows and in what graph: {:s <alice> :p <knows> :o ?who :c ?g}
        (let [pattern {:s "<alice>" :p "<knows>" :o "?who" :c "?g"}
              res     (foreign-invoke-query q-bgp pattern)
              binding (first res)]
          (is (= 1 (count res)))
          (is (= "<bob>" (get binding "?who")))
          (is (= DEFAULT-CONTEXT-VAL (get binding "?g"))))

				;; 2. Find anything in graph <g1>: {:s ?s :p ?p :o ?o :c <g1>}
        (let [pattern {:s "?s" :p "?p" :o "?o" :c "<g1>"}
              res     (foreign-invoke-query q-bgp pattern)]
          (is (= 1 (count res)))
          (let [b (first res)]
            (is (= "<bob>" (get b "?s")))
            (is (= "30" (get b "?o")))
						;; Note: ?c wasn't requested in pattern keys (it was a constant), so it might not be bound.
						;; But 'find-bgp' logic binds wildcard vars. "?s", "?p", etc.
            )))

			;; ===================================================================
			;; 4. TEST: 'execute-plan' (Complex Plans with Graphs)
			;; ===================================================================
      (testing "Query: execute-plan (Filter on Graph Variable)"
				;; Plan: SELECT ?s ?g WHERE { GRAPH ?g { ?s <knows> ?o } } FILTER (?g != <urn:rama:default>)
				;; This should match Charlie (in g2) but NOT Alice (in default).

        (let [bgp-pattern {:s "?s" :p "<knows>" :o "?o" :c "?g"}
              filter-expr {:type :cmp :op :ne
                           :left {:type :var :name "?g"}
                           :right {:type :const :val DEFAULT-CONTEXT-VAL}}
              plan {:op :filter
                    :sub-plan {:op :bgp :pattern bgp-pattern}
                    :expr filter-expr}
              results (foreign-invoke-query q-plan plan)]

          (is (= 1 (count results)) "Should only return 1 result (Charlie)")
          (let [res (first results)]
            (is (= "<charlie>" (get res "?s")))
            (is (= "<g2>" (get res "?g"))))))

			;; ===================================================================
			;; 5. TEST: Deletion (Specific Quad)
			;; ===================================================================
      (testing "ETL: Deletion"
				;; Delete Bob from <g1> - now with tx-time for temporal tracking
        (foreign-append! depot [:del ["<bob>" "<age>" "30" "<g1>"] (System/currentTimeMillis)])
        (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4)

				;; Verify deletion - find-triples filters out tombstoned quads
        (let [res (foreign-invoke-query q-triples "<bob>" "<age>" "30" "<g1>")]
          (is (empty? res) "Quad should be deleted (filtered by tombstone)"))

				;; With soft delete, data remains in $$spoc but is tombstoned
        ;; find-triples correctly filters it out
        (is (not-empty (foreign-select [(keypath "<bob>" "<age>" "30") ALL] p-spoc))
            "Soft delete: data remains in $$spoc but is filtered by find-triples")))))

;(deftest test-rdf-storage-module
;	;; Create an In-Process Cluster (IPC)
;	(with-open [ipc (rtest/create-ipc)]
;		(rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
;
;		(let [module-name (get-module-name RdfStorageModule)
;
;					;; --- Clients ---
;					depot      (foreign-depot ipc module-name "*triple-depot")
;					p-spo      (foreign-pstate ipc module-name "$$spo")
;					p-pos      (foreign-pstate ipc module-name "$$pos")
;					p-osp      (foreign-pstate ipc module-name "$$osp")
;
;					q-triples  (foreign-query ipc module-name "find-triples")
;					q-bgp      (foreign-query ipc module-name "find-bgp")
;					q-plan     (foreign-query ipc module-name "execute-plan")]
;
;			;; ===================================================================
;			;; 1. TEST: ETL & Direct PState Access
;			;; ===================================================================
;			(testing "ETL: Ingestion and PState Indexing"
;				;; Append data: <alice> <knows> <bob>
;				(foreign-append! depot [:add ["<alice>" "<knows>" "<bob>"]])
;				(foreign-append! depot [:add ["<bob>" "<age>" "30"]])
;
;				;; Wait for microbatch "indexer" to process 2 records
;				(rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 2)
;
;				;; Verify $$spo (S->P->O)
;				(is (= #{"<bob>"}
;							 (set (foreign-select [(keypath "<alice>" "<knows>") ALL] p-spo)))
;						"SPO index should contain bob")
;
;				;; Verify $$pos (P->O->S)
;				(is (= #{"<bob>"}
;							 (set (foreign-select [(keypath "<age>" "30") ALL] p-pos)))
;						"POS index should contain bob (reverse lookup)")
;
;				;; Verify $$osp (O->S->P)
;				(is (= #{"<knows>"}
;							 (set (foreign-select [(keypath "<bob>" "<alice>") ALL] p-osp)))
;						"OSP index should contain knows"))
;
;			;; ===================================================================
;			;; 2. TEST: 'find-triples' (Low-level 8-pattern QT)
;			;; ===================================================================
;			(testing "Query: find-triples (The 8 patterns)"
;				;; Note: This topology expects `nil` for wildcards
;
;				;; Pattern: (S P O) - Exact Match
;				(is (= [["<alice>" "<knows>" "<bob>"]]
;							 (foreign-invoke-query q-triples "<alice>" "<knows>" "<bob>")))
;
;				;; Pattern: (S ? ?) - Find all Predicates/Objects for Subject
;				(let [res (foreign-invoke-query q-triples "<bob>" nil nil)]
;					(is (= [["<bob>" "<age>" "30"]] res)))
;
;				;; Pattern: (? P O) - Find Subject by Predicate+Object
;				(let [res (foreign-invoke-query q-triples nil "<age>" "30")]
;					(is (= [["<bob>" "<age>" "30"]] res))))
;
;			;; ===================================================================
;			;; 3. TEST: 'find-bgp' (Variable Binding)
;			;; ===================================================================
;			(testing "Query: find-bgp (Bindings)"
;				;; This topology expects strings starting with "?" as variables
;
;				;; Find who Alice knows: {:s <alice> :p <knows> :o ?who}
;				(let [pattern {:s "<alice>" :p "<knows>" :o "?who"}
;							res     (foreign-invoke-query q-bgp pattern)
;							binding (first res)]
;
;					(is (= 1 (count res)))
;					(is (= "<bob>" (get binding "?who"))))
;
;				;; Find subject and object for predicate <age>: {:s ?s :p <age> :o ?age}
;				(let [pattern {:s "?s" :p "<age>" :o "?age"}
;							res     (first (foreign-invoke-query q-bgp pattern))]
;
;					(is (= "<bob>" (get res "?s")))
;					(is (= "30"    (get res "?age")))))
;
;			;; ===================================================================
;			;; 4. TEST: 'execute-plan' (Joins & Projections)
;			;; ===================================================================
;			(testing "Query: execute-plan (Recursive Engine)"
;
;				;; --- 4a. Simple BGP via Plan ---
;				(let [bgp-plan {:op :bgp
;												:pattern {:s "<alice>" :p "<knows>" :o "?friend"}}
;							res      (first (foreign-invoke-query q-plan bgp-plan))]
;					(is (= "<bob>" (get res "?friend"))))
;
;				;; --- 4b. JOIN Operation ---
;				;; Query: Find ?person who <knows> ?friend AND ?friend has <age> ?age
;				;; SPARQL: SELECT ?person ?age WHERE { ?person <knows> ?friend . ?friend <age> ?age }
;				(let [join-plan {:op :join
;												 :join-vars ["?friend"] ;; Client calculates this
;												 :left {:op :bgp
;																:pattern {:s "?person" :p "<knows>" :o "?friend"}}
;												 :right {:op :bgp
;																 :pattern {:s "?friend" :p "<age>" :o "?age"}}}
;
;							results (foreign-invoke-query q-plan join-plan)
;							row     (first results)]
;
;					(is (= 1 (count results)))
;					(is (= "<alice>" (get row "?person")))
;					(is (= "<bob>"   (get row "?friend")))
;					(is (= "30"      (get row "?age"))))
;
;				;; --- 4c. PROJECT Operation ---
;				;; Query: Same as above, but only return ?person and ?age (drop ?friend)
;				(let [project-plan {:op :project
;														:vars ["?person" "?age"]
;														:sub-plan {:op :join
;																			 :join-vars ["?friend"]
;																			 :left {:op :bgp
;																							:pattern {:s "?person" :p "<knows>" :o "?friend"}}
;																			 :right {:op :bgp
;																							 :pattern {:s "?friend" :p "<age>" :o "?age"}}}}
;
;							results (foreign-invoke-query q-plan project-plan)
;							row     (first results)]
;
;					(is (= 1 (count results)))
;					(is (= "<alice>" (get row "?person")))
;					(is (= "30"      (get row "?age")))
;					(is (nil? (get row "?friend")) "Friend variable should be projected out"))
;
;				;; --- 4d. Invalid Plan (Default) ---
;				(let [bad-plan {:op :unknown-op}
;							res      (foreign-invoke-query q-plan bad-plan)]
;					(is (empty? res)))
;
;				;; --- 4e. LEFT JOIN Operation ---
;				;; append extra data: <charlie> <knows> <dave> (but no age for dave)
;				(foreign-append! depot [:add ["<charlie>" "<knows>" "<dave>"]])
;				(rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 3)
;				;; Query: Find all ?person who <knows> ?friend, and optionally get ?friend's ?age
;				;; SPARQL: SELECT ?person ?friend ?age WHERE { ?person <knows> ?friend . OPTIONAL { ?friend <age> ?age } }
;				(let [left-join-plan {:op :left-join
;															:join-vars ["?friend"]
;															:left {:op :bgp
;																		 :pattern {:s "?person" :p "<knows>" :o "?friend"}}
;															:right {:op :bgp
;																			:pattern {:s "?friend" :p "<age>" :o "?age"}}}
;							results (foreign-invoke-query q-plan left-join-plan)
;							;; alice -> bob (has age)
;							row1    (first results)
;							;; charlie -> dave (no age)
;							row2    (second results)]
;					(is (= 2 (count results)))
;					;; Verify alice -> bob
;
;					(is (= "<alice>" (get row1 "?person")))
;					(is (= "<bob>"   (get row1 "?friend")))
;					(is (= "30"      (get row1 "?age")))
;					;; Verify charlie -> dave (age should be nil)
;					(is (= "<charlie>" (get row2 "?person")))
;					(is (= "<dave>"    (get row2 "?friend")))
;					(is (nil? (get row2 "?age")) "Age should be nil for friend with no age"))
;
;
;
;				;; Additional data for Slice/Limit tests (using <knows-to> predicate to keep other tests clean)
;				(foreign-append! depot [:add ["<charlie>" "<knows-to>" "<dave>"]])
;				(foreign-append! depot [:add ["<eve>" "<knows-to>" "<frank>"]])
;				(foreign-append! depot [:add ["<george>" "<knows-to>" "<harry>"]])
;				(rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 6)
;
;				;; --- 4f. SLICE Operation ---
;				;; Query: SELECT ?person ?friend WHERE { ?person <knows-to> ?friend } OFFSET 1 LIMIT 2
;				(let [bgp-sub-plan {:op :bgp
;														:pattern {:s "?person" :p "<knows-to>" :o "?friend"}}
;							slice-plan   {:op :slice
;														:offset 1 ; Skip the first result
;														:limit 2  ; Take the next two
;														:sub-plan bgp-sub-plan}
;							;; Get all results first for deterministic assertion
;							all-results          (foreign-invoke-query q-plan bgp-sub-plan)
;							;; Sort by ?person to ensure a predictable slice order
;							sorted-all-results   (sort-by #(get % "?person") all-results)
;							;; Calculate expected results based on slice logic
;							expected-results     (->> sorted-all-results
;																				(drop 1) ; OFFSET 1
;																				(take 2) ; LIMIT 2
;																				set)
;							actual-results       (foreign-invoke-query q-plan slice-plan)]
;
;					(is (= 3 (count all-results)) "Total results before slicing should be 3")
;					(is (= 2 (count actual-results)) "Sliced results count should be 2")
;					;(is (= expected-results (set actual-results)) "Sliced results should contain correct rows")
;
;					;; Assert specific rows (charlie is dropped, eve/george remain)
;					;(let [eve-row (first (filter #(= "<eve>" (get % "?person")) actual-results))
;					;			george-row (first (filter #(= "<george>" (get % "?person")) actual-results))]
;					;	(is (some? eve-row) "Eve's row should be present")
;					;	(is (some? george-row) "George's row should be present"))
;					)
;
;				;; --- 4g. ASK Operation (New) ---
;				;; Query: ASK { <alice> <knows> <bob> } -> True
;				(let [ask-true-plan {:op :ask
;														 :sub-plan {:op :bgp
;																				:pattern {:s "<alice>" :p "<knows>" :o "<bob>"}}}
;							results (foreign-invoke-query q-plan ask-true-plan)
;							_ 		(println "ASK True results:" results)]
;					(is (= 1 (count results)) "ASK (True) should return one result")
;					(is (= {} (first results)) "ASK (True) should return an empty binding tuple"))
;
;				;; Query: ASK { <alice> <knows> <frank> } -> False (frank does not exist)
;				(let [ask-false-plan {:op :ask
;															:sub-plan {:op :bgp
;																				 :pattern {:s "<alice>" :p "<knows>" :o "<frank>"}}}
;							results (foreign-invoke-query q-plan ask-false-plan)
;							_ 		(println "ASK False results:" results)]
;					(is (empty? results) "ASK (False) should return no results"))
;
;				)
;
;			;; ===================================================================
;			;; 5. TEST: Deletion
;			;; ===================================================================
;			(testing "ETL: Deletion"
;				(foreign-append! depot [:del ["<alice>" "<knows>" "<bob>"]])
;				(rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 7)
;
;				(let [res (foreign-invoke-query q-triples "<alice>" "<knows>" "<bob>")]
;					(is (empty? res) "Triple should be deleted")))
;			)))

(comment

  (run-tests 'rama-sail.query.core-test)

  (test-filter-helpers)

  (test-filter-topology-integration)

  (test-rdf-storage-module-named-graphs))
(ns rama-sail.query.find-triples-test
  ;(:use [com.rpl.rama]
  ;      [com.rpl.rama.path])
  (:require [clojure.test :refer :all]
            [com.rpl.rama :as rama]
            [com.rpl.rama.path :as path]
            [com.rpl.rama.test :as rtest]
            [com.rpl.rama.ops :as ops]
            [rama-sail.core :as core]))

;; 1. Define a minimal module for isolation testing
(rama/defmodule FindTriplesTestModule [setup topologies]
  ;; Depot for ingesting test quads
  (rama/declare-depot setup *quad-depot (rama/hash-by core/depot-partition-key))

  ;; Declare the 4 Quad Indices required by find-triples
  (let [mb (rama/microbatch-topology topologies "indexer")]
    (rama/declare-pstate mb $$spoc {String (rama/map-schema String (rama/map-schema String (rama/set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})
    (rama/declare-pstate mb $$posc {String (rama/map-schema String (rama/map-schema String (rama/set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})
    (rama/declare-pstate mb $$ospc {String (rama/map-schema String (rama/map-schema String (rama/set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})
    (rama/declare-pstate mb $$cspo {String (rama/map-schema String (rama/map-schema String (rama/set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})

    ;; Simple ETL to populate indices (Add Only)
    (rama/<<sources mb
                    (rama/source> *quad-depot :> %microbatch)
                    (%microbatch :> [_ [*s *p *o *c]])
      ;; Write to all 4 indices
                    (rama/|hash *s)
                    (rama/local-transform> [(path/keypath *s *p *o) path/NIL->SET path/NONE-ELEM (path/termval *c)] $$spoc)
                    (rama/|hash *p)
                    (rama/local-transform> [(path/keypath *p *o *s) path/NIL->SET path/NONE-ELEM (path/termval *c)] $$posc)
                    (rama/|hash *o)
                    (rama/local-transform> [(path/keypath *o *s *p) path/NIL->SET path/NONE-ELEM (path/termval *c)] $$ospc)
                    (rama/|hash *c)
                    (rama/local-transform> [(path/keypath *c *s *p) path/NIL->SET path/NONE-ELEM (path/termval *o)] $$cspo)))

  ;; 2. Register the find-triples topology
  (core/find-triples-query-topology topologies))

(deftest test-find-triples-permutations
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc FindTriplesTestModule {:tasks 4 :threads 2})

    (let [module-name (rama/get-module-name FindTriplesTestModule)
          depot       (rama/foreign-depot ipc module-name "*quad-depot")
          find-triples (rama/foreign-query ipc module-name "find-triples")

          ;; Helper to invoke query
          query (fn [s p o c]
                  (let [res (rama/foreign-invoke-query find-triples s p o c)]
                    (set res)))]

      ;; --- DATA SETUP ---
      ;; <alice> <knows> <bob> <g1>
      (rama/foreign-append! depot [:add ["<alice>" "<knows>" "<bob>" "<g1>"]])
      ;; <alice> <age> 30 <g1>
      (rama/foreign-append! depot [:add ["<alice>" "<age>" "30" "<g1>"]])
      ;; <bob> <knows> <charlie> <g2>
      (rama/foreign-append! depot [:add ["<bob>" "<knows>" "<charlie>" "<g2>"]])
      ;; <dave> <knows> <alice> <g2>
      (rama/foreign-append! depot [:add ["<dave>" "<knows>" "<alice>" "<g2>"]])

      (rtest/wait-for-microbatch-processed-count ipc module-name "indexer" 4)

      (testing "Group 1: Subject Bound (Using $$spoc)"
        ;; 1. (S P O C) - Exact
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]}
               (query "<alice>" "<knows>" "<bob>" "<g1>")))

        ;; Negative Test
        (is (= #{}
               (query "<alice>" "<knows>" "<dave>" "<g1>")))

        ;; 2. (S P O ?)
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]}
               (query "<alice>" "<knows>" "<bob>" nil)))

        ;; 3. (S P ? C)
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]}
               (query "<alice>" "<knows>" nil "<g1>")))

        ;; 4. (S P ? ?)
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]}
               (query "<alice>" "<knows>" nil nil)))

        ;; 5. (S ? O C)
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]}
               (query "<alice>" nil "<bob>" "<g1>")))

        ;; 6. (S ? O ?)
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]}
               (query "<alice>" nil "<bob>" nil)))

        ;; 7. (S ? ? C)
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]
                 ["<alice>" "<age>" "30" "<g1>"]}
               (query "<alice>" nil nil "<g1>")))

        ;; 8. (S ? ? ?)
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]
                 ["<alice>" "<age>" "30" "<g1>"]}
               (query "<alice>" nil nil nil))))

      (testing "Group 2: Predicate Bound (Using $$posc)"
        ;; 9. (? P O C)
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]}
               (query nil "<knows>" "<bob>" "<g1>")))

        ;; 10. (? P O ?)
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]}
               (query nil "<knows>" "<bob>" nil)))

        ;; 11. (? P ? C) - Knows in <g1> -> Alice
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]}
               (query nil "<knows>" nil "<g1>")))

        ;; 12. (? P ? ?) - Knows -> Alice, Bob, Dave
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]
                 ["<bob>" "<knows>" "<charlie>" "<g2>"]
                 ["<dave>" "<knows>" "<alice>" "<g2>"]}
               (query nil "<knows>" nil nil))))

      (testing "Group 3: Object Bound (Using $$ospc)"
        ;; 13. (? ? O C) - Object <alice> in <g2>
        (is (= #{["<dave>" "<knows>" "<alice>" "<g2>"]}
               (query nil nil "<alice>" "<g2>")))

        ;; 14. (? ? O ?) - Object <alice> anywhere
        (is (= #{["<dave>" "<knows>" "<alice>" "<g2>"]}
               (query nil nil "<alice>" nil)))
        ;; Object <bob>
        (is (= #{["<alice>" "<knows>" "<bob>" "<g1>"]}
               (query nil nil "<bob>" nil))))

      (testing "Group 4: Context Bound (Using $$cspo)"
        ;; 15. (? ? ? C) - Everything in <g2>
        (is (= #{["<bob>" "<knows>" "<charlie>" "<g2>"]
                 ["<dave>" "<knows>" "<alice>" "<g2>"]}
               (query nil nil nil "<g2>"))))

      (testing "Group 5: Full Scan"
        ;; 16. (? ? ? ?)
        (let [all (query nil nil nil nil)]
          (is (= 4 (count all)))
          (is (contains? all ["<alice>" "<knows>" "<bob>" "<g1>"]))
          (is (contains? all ["<dave>" "<knows>" "<alice>" "<g2>"])))))))

(comment

  (run-tests 'rama-sail.query.find-triples-test))
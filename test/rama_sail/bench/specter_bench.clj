(ns rama-sail.bench.specter-bench
  "Benchmark comparing Specter-based pattern matching vs vanilla Clojure for loops.

   Tests the optimization of match-pattern-locally function used in co-located joins."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [com.rpl.rama.path :refer [ALL FIRST LAST MAP-VALS collect-one
                                       select filterer keypath submap]]))

;;; ---------------------------------------------------------------------------
;;; Test Data Generation
;;; ---------------------------------------------------------------------------

(defn generate-spoc-data
  "Generate test SPOC data: {subject -> {predicate -> {object -> #{contexts}}}}
   Returns n subjects, each with m predicates, each with k objects."
  [n-subjects n-predicates n-objects]
  (into {}
        (for [s (range n-subjects)]
          [(str "<subject" s ">")
           (into {}
                 (for [p (range n-predicates)]
                   [(str "<predicate" p ">")
                    (into {}
                          (for [o (range n-objects)]
                            [(str "\"object" o "\"")
                             #{"::rama-internal::default-graph"}]))]))])))

;;; ---------------------------------------------------------------------------
;;; Original Implementation (for loop with :when guards)
;;; ---------------------------------------------------------------------------

(defn match-pattern-locally-original
  "Original implementation using nested for loops with :when guards."
  [pattern spoc-data]
  (let [{:keys [s p o c]} pattern
        s-var? (and (string? s) (str/starts-with? s "?"))
        p-var? (and (string? p) (str/starts-with? p "?"))
        o-var? (and (string? o) (str/starts-with? o "?"))]
    (for [[subj pred-map] spoc-data
          :when (or s-var? (= s subj))
          [pred obj-map] pred-map
          :when (or p-var? (= p pred))
          [obj ctx-set] obj-map
          :when (or o-var? (= o obj))
          ctx ctx-set
          :when (or (nil? c) (= c ctx))]
      (cond-> {}
        s-var? (assoc s subj)
        p-var? (assoc p pred)
        o-var? (assoc o obj)))))

;;; ---------------------------------------------------------------------------
;;; Specter-Optimized Implementation
;;; ---------------------------------------------------------------------------

(defn- make-ctx-filter
  "Create filter for context set. nil context means match all."
  [c]
  (if (nil? c)
    (constantly true)
    (fn [ctx] (= ctx c))))

(defn match-pattern-locally-specter
  "Specter-optimized implementation using keypath for bound keys and ALL for variables.

   Key optimizations:
   1. keypath provides O(1) access for bound keys (vs O(n) scan)
   2. collect-one FIRST efficiently extracts keys during traversal
   3. Single select call instead of nested for loops"
  [pattern spoc-data]
  (let [{:keys [s p o c]} pattern
        s-var? (and (string? s) (str/starts-with? s "?"))
        p-var? (and (string? p) (str/starts-with? p "?"))
        o-var? (and (string? o) (str/starts-with? o "?"))
        c-filter (make-ctx-filter c)
        ;; Build navigation path dynamically based on bound/variable status
        ;; For bound keys: use keypath for O(1) direct access
        ;; For variable keys: use ALL + collect-one FIRST + LAST to scan and collect
        s-path (if s-var?
                 [ALL (collect-one FIRST) LAST]
                 [(keypath s)])
        p-path (if p-var?
                 [ALL (collect-one FIRST) LAST]
                 [(keypath p)])
        o-path (if o-var?
                 [ALL (collect-one FIRST) LAST]
                 [(keypath o)])
        ;; Context is always scanned (it's a set)
        c-path [ALL]
        ;; Combine paths - note: concat the vectors
        full-path (vec (concat s-path p-path o-path c-path))
        raw-results (select full-path spoc-data)]
    ;; Filter contexts and transform collected values into binding maps
    ;; The shape of raw-results depends on which vars are bound:
    ;; - All vars: [[subj pred obj ctx] ...]
    ;; - Subject bound: [[pred obj ctx] ...]
    ;; - S+P bound: [[obj ctx] ...]
    ;; - S+P+O bound: [ctx ...]
    (cond
      ;; All variables - collect-one captured [subj pred obj], ctx at end
      (and s-var? p-var? o-var?)
      (for [[subj pred obj ctx] raw-results
            :when (c-filter ctx)]
        {s subj, p pred, o obj})

      ;; Subject bound, P and O variable
      (and (not s-var?) p-var? o-var?)
      (for [[pred obj ctx] raw-results
            :when (c-filter ctx)]
        {p pred, o obj})

      ;; S and P bound, O variable
      (and (not s-var?) (not p-var?) o-var?)
      (for [[obj ctx] raw-results
            :when (c-filter ctx)]
        {o obj})

      ;; S and O variable, P bound
      (and s-var? (not p-var?) o-var?)
      (for [[subj obj ctx] raw-results
            :when (c-filter ctx)]
        {s subj, o obj})

      ;; S variable, P and O bound
      (and s-var? (not p-var?) (not o-var?))
      (for [[subj ctx] raw-results
            :when (c-filter ctx)]
        {s subj})

      ;; P variable, S and O bound
      (and (not s-var?) p-var? (not o-var?))
      (for [[pred ctx] raw-results
            :when (c-filter ctx)]
        {p pred})

      ;; O bound only, S and P variable
      (and s-var? p-var? (not o-var?))
      (for [[subj pred ctx] raw-results
            :when (c-filter ctx)]
        {s subj, p pred})

      ;; All bound - just check existence
      :else
      (for [ctx raw-results
            :when (c-filter ctx)]
        {}))))

;;; ---------------------------------------------------------------------------
;;; Alternative: Using submap for known keys
;;; ---------------------------------------------------------------------------

(defn match-pattern-locally-submap
  "Alternative optimization using submap for known keys.
   More efficient when subject/predicate/object are constants (not variables)."
  [pattern spoc-data]
  (let [{:keys [s p o c]} pattern
        s-var? (and (string? s) (str/starts-with? s "?"))
        p-var? (and (string? p) (str/starts-with? p "?"))
        o-var? (and (string? o) (str/starts-with? o "?"))
        c-filter (make-ctx-filter c)]
    (cond
      ;; All bound - direct lookup
      (and (not s-var?) (not p-var?) (not o-var?))
      (when-let [ctx-set (get-in spoc-data [s p o])]
        (for [ctx ctx-set :when (c-filter ctx)]
          {}))

      ;; Subject bound - use keypath
      (not s-var?)
      (when-let [pred-map (get spoc-data s)]
        (for [[pred obj-map] pred-map
              :when (or p-var? (= p pred))
              [obj ctx-set] obj-map
              :when (or o-var? (= o obj))
              ctx ctx-set
              :when (c-filter ctx)]
          (cond-> {}
            p-var? (assoc p pred)
            o-var? (assoc o obj))))

      ;; Subject is variable - must scan all
      :else
      (for [[subj pred-map] spoc-data
            [pred obj-map] pred-map
            :when (or p-var? (= p pred))
            [obj ctx-set] obj-map
            :when (or o-var? (= o obj))
            ctx ctx-set
            :when (c-filter ctx)]
        (cond-> {s subj}
          p-var? (assoc p pred)
          o-var? (assoc o obj))))))

;;; ---------------------------------------------------------------------------
;;; Benchmarking
;;; ---------------------------------------------------------------------------

(defn benchmark-fn
  "Run function n times and return timing statistics."
  [f n]
  (let [_ (dotimes [_ 3] (f)) ; warmup
        times (vec (for [_ (range n)]
                     (let [start (System/nanoTime)
                           result (doall (f))
                           end (System/nanoTime)]
                       {:time-ms (/ (- end start) 1000000.0)
                        :count (count result)})))
        timings (map :time-ms times)
        sorted (sort timings)]
    {:min (first sorted)
     :max (last sorted)
     :p50 (nth sorted (int (* 0.5 (count sorted))))
     :mean (/ (reduce + timings) (double (count timings)))
     :result-count (:count (first times))
     :n n}))

(defn format-stats [label stats]
  (format "%-20s | n=%d | min=%.3fms | p50=%.3fms | mean=%.3fms | results=%d"
          label (:n stats) (:min stats) (:p50 stats) (:mean stats) (:result-count stats)))

;;; ---------------------------------------------------------------------------
;;; Tests
;;; ---------------------------------------------------------------------------

(deftest test-correctness
  (testing "All implementations produce same results"
    (let [spoc-data (generate-spoc-data 10 5 3)
          ;; Test case 1: All variables (full scan)
          pattern1 {:s "?s" :p "?p" :o "?o" :c nil}
          ;; Test case 2: Subject bound
          pattern2 {:s "<subject5>" :p "?p" :o "?o" :c nil}
          ;; Test case 3: Subject and predicate bound
          pattern3 {:s "<subject5>" :p "<predicate2>" :o "?o" :c nil}]

      (doseq [[name pattern] [["all-vars" pattern1]
                              ["s-bound" pattern2]
                              ["sp-bound" pattern3]]]
        (let [orig-result (set (match-pattern-locally-original pattern spoc-data))
              specter-result (set (match-pattern-locally-specter pattern spoc-data))
              submap-result (set (match-pattern-locally-submap pattern spoc-data))]
          (is (= orig-result specter-result)
              (str "Specter should match original for " name))
          (is (= orig-result submap-result)
              (str "Submap should match original for " name)))))))

(deftest test-performance-small
  (testing "Performance comparison on small dataset (100 subjects)"
    (let [spoc-data (generate-spoc-data 100 10 5)
          ;; Pattern with all variables (worst case - full scan)
          pattern {:s "?s" :p "?p" :o "?o" :c nil}
          n 50]

      (println)
      (println "=== Specter Optimization Benchmark (Small: 100 subjects) ===")
      (println (format "Data size: %d subjects × 10 predicates × 5 objects = %d quads"
                       100 (* 100 10 5)))
      (println)

      (let [orig-stats (benchmark-fn #(match-pattern-locally-original pattern spoc-data) n)
            specter-stats (benchmark-fn #(match-pattern-locally-specter pattern spoc-data) n)
            submap-stats (benchmark-fn #(match-pattern-locally-submap pattern spoc-data) n)]

        (println (format-stats "Original (for loop)" orig-stats))
        (println (format-stats "Specter (filterer)" specter-stats))
        (println (format-stats "Submap (hybrid)" submap-stats))
        (println)
        (println (format "Specter speedup: %.2fx" (/ (:mean orig-stats) (:mean specter-stats))))
        (println (format "Submap speedup:  %.2fx" (/ (:mean orig-stats) (:mean submap-stats))))

        ;; Verify results match
        (is (= (:result-count orig-stats) (:result-count specter-stats)))
        (is (= (:result-count orig-stats) (:result-count submap-stats)))))))

(deftest test-performance-large
  (testing "Performance comparison on large dataset (1000 subjects)"
    (let [spoc-data (generate-spoc-data 1000 10 5)
          ;; Pattern with all variables (worst case - full scan)
          pattern {:s "?s" :p "?p" :o "?o" :c nil}
          n 20]

      (println)
      (println "=== Specter Optimization Benchmark (Large: 1000 subjects) ===")
      (println (format "Data size: %d subjects × 10 predicates × 5 objects = %d quads"
                       1000 (* 1000 10 5)))
      (println)

      (let [orig-stats (benchmark-fn #(match-pattern-locally-original pattern spoc-data) n)
            specter-stats (benchmark-fn #(match-pattern-locally-specter pattern spoc-data) n)
            submap-stats (benchmark-fn #(match-pattern-locally-submap pattern spoc-data) n)]

        (println (format-stats "Original (for loop)" orig-stats))
        (println (format-stats "Specter (filterer)" specter-stats))
        (println (format-stats "Submap (hybrid)" submap-stats))
        (println)
        (println (format "Specter speedup: %.2fx" (/ (:mean orig-stats) (:mean specter-stats))))
        (println (format "Submap speedup:  %.2fx" (/ (:mean orig-stats) (:mean submap-stats))))

        (is (= (:result-count orig-stats) (:result-count specter-stats)))
        (is (= (:result-count orig-stats) (:result-count submap-stats)))))))

(deftest test-performance-selective
  (testing "Performance with selective patterns (bound subject)"
    (let [spoc-data (generate-spoc-data 1000 10 5)
          ;; Pattern with subject bound (selective - only 1/1000 subjects)
          pattern {:s "<subject500>" :p "?p" :o "?o" :c nil}
          n 100]

      (println)
      (println "=== Specter Optimization Benchmark (Selective: 1 of 1000 subjects) ===")
      (println)

      (let [orig-stats (benchmark-fn #(match-pattern-locally-original pattern spoc-data) n)
            specter-stats (benchmark-fn #(match-pattern-locally-specter pattern spoc-data) n)
            submap-stats (benchmark-fn #(match-pattern-locally-submap pattern spoc-data) n)]

        (println (format-stats "Original (for loop)" orig-stats))
        (println (format-stats "Specter (filterer)" specter-stats))
        (println (format-stats "Submap (hybrid)" submap-stats))
        (println)
        (println (format "Specter speedup: %.2fx" (/ (:mean orig-stats) (:mean specter-stats))))
        (println (format "Submap speedup:  %.2fx" (/ (:mean orig-stats) (:mean submap-stats))))

        (is (= (:result-count orig-stats) (:result-count specter-stats)))
        (is (= (:result-count orig-stats) (:result-count submap-stats)))))))

(comment
  ;; Run benchmarks from REPL
  (require '[clojure.test :as t])
  (t/run-tests 'rama-sail.bench.specter-bench)

  ;; Run specific test
  (test-correctness)
  (test-performance-small)
  (test-performance-large)
  (test-performance-selective))

(ns rama-sail.ci-gate-test
  "Guards the CI gate: every test namespace runs in CI (project.clj :unit,
   :integration or :w3c selector) unless it is a benchmark, tagged ^:perf, or
   listed in `not-in-ci` with a reason. The :integration namespaces are listed
   once, in two halves (`integration-namespaces`), which CI runs in parallel."
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :refer [deftest is]]
            [clojure.walk :as walk])
  (:import (java.io PushbackReader)))

(def ^:private not-in-ci
  "Test namespaces deliberately kept out of CI."
  {"rama-sail.query.filter-test" "performance comparison of two filter topologies on its own module copy"})

(defn- read-forms [f]
  (with-open [r (PushbackReader. (io/reader f))]
    (binding [*read-eval* false]
      (doall (take-while #(not= ::eof %) (repeatedly #(read {:eof ::eof} r)))))))

(defn- read-first-form [f]
  (first (read-forms f)))

(defn- project-form [op]
  (some #(when (= op (first %)) %) (read-forms "project.clj")))

(defn- integration-halves
  "{half #{ns-name}} from `(def integration-namespaces ...)` in project.clj."
  []
  (let [form (some #(when (and (= 'def (first %)) (= 'integration-namespaces (second %))) %)
                   (read-forms "project.clj"))]
    (is (some? form) "project.clj has no (def integration-namespaces ...)")
    (last form)))

(defn- test-namespaces
  "{ns-name ns-symbol} for every test file that defines a deftest."
  []
  (into {}
        (for [^java.io.File f (file-seq (io/file "test"))
              :when (and (.isFile f)
                         (.endsWith (.getName f) ".clj")
                         (.contains ^String (slurp f) "(deftest"))
              :let [[op ns-sym] (read-first-form f)]
              :when (= 'ns op)]
          [(str ns-sym) ns-sym])))

(defn- ci-namespaces
  "Namespace names in the :unit and :w3c selectors and in `integration-namespaces`."
  []
  (let [selectors (->> (project-form 'defproject)
                       (drop 3)
                       (partition 2)
                       (some (fn [[k v]] (when (= :test-selectors k) v))))
        names (atom (reduce into #{} (vals (integration-halves))))]
    (doseq [k [:unit :integration :integration-module :integration-sail :w3c]]
      (is (contains? selectors k) (str "project.clj has no " k " test selector")))
    (doseq [k [:unit :w3c]]
      (walk/postwalk #(do (when (string? %) (swap! names conj %)) %) (get selectors k)))
    @names))

(deftest integration-halves-do-not-overlap
  (let [{:keys [module sail]} (integration-halves)]
    (is (seq module))
    (is (seq sail))
    (is (empty? (set/intersection module sail)) "a namespace in both halves runs twice in CI")))

(deftest every-test-namespace-runs-in-ci
  (let [tests (test-namespaces)
        ci (ci-namespaces)
        exempt? (fn [n] (or (re-find #"bench" n)
                            (:perf (meta (get tests n)))
                            (contains? not-in-ci n)))]
    (is (empty? (remove exempt? (set/difference (set (keys tests)) ci)))
        "add these namespaces to a CI selector in project.clj, or exempt them in not-in-ci")
    (is (empty? (set/difference ci (set (keys tests))))
        "CI selectors name namespaces that no longer exist")))

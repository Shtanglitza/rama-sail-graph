(ns rama-sail.ci-gate-test
  "Guards the CI gate: every test namespace runs in CI (project.clj :unit,
   :integration or :w3c selector) unless it is a benchmark, tagged ^:perf, or
   listed in `not-in-ci` with a reason."
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :refer [deftest is]]
            [clojure.walk :as walk])
  (:import (java.io PushbackReader)))

(def ^:private not-in-ci
  "Test namespaces deliberately kept out of CI."
  {"rama-sail.query.filter-test" "performance comparison of two filter topologies on its own module copy"})

(defn- read-first-form [f]
  (with-open [r (PushbackReader. (io/reader f))]
    (binding [*read-eval* false]
      (read r))))

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
  "Namespace names in the :unit, :integration and :w3c selectors of project.clj."
  []
  (let [selectors (->> (read-first-form "project.clj")
                       (drop 3)
                       (partition 2)
                       (some (fn [[k v]] (when (= :test-selectors k) v))))
        names (atom #{})]
    (doseq [k [:unit :integration :w3c]]
      (is (contains? selectors k) (str "project.clj has no " k " test selector"))
      (walk/postwalk #(do (when (string? %) (swap! names conj %)) %) (get selectors k)))
    @names))

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

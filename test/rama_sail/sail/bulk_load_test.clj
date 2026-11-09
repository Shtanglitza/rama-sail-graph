(ns rama-sail.sail.bulk-load-test
  "Unit tests (no IPC) for the bench loader's delivery guarantees: acknowledged
   appends, retries, the in-flight window, and waiting for the indexer."
  (:require [clojure.test :refer [deftest is testing]]
            [com.rpl.rama :as rama]
            [rama-sail.bench.bsbm.infra.ntriples-loader :as loader]
            [rama-sail.bench.infra.cluster-config :as cc])
  (:import (java.io File)
           (java.util.concurrent CompletableFuture Executor TimeUnit)))

(defn- nt-file
  "Temp N-Triples file with n distinct triples, deleted on JVM exit."
  ^File [n]
  (let [f (doto (File/createTempFile "bulk-load-test" ".nt") (.deleteOnExit))]
    (spit f (apply str (for [i (range n)] (str "<http://ex/s" i "> <http://ex/p> \"v\" .\n"))))
    f))

(defn- failed [] (CompletableFuture/failedFuture (ex-info "Depot append buffer full" {})))
(defn- ok [] (CompletableFuture/completedFuture nil))

(deftest test-failed-appends-are-retried
  (let [attempts (atom {})
        acks (atom #{})
        stats (with-redefs [rama/foreign-append-async!
                            (fn [_ op ack]
                              (swap! acks conj ack)
                              (if (= 1 (get (swap! attempts update op (fnil inc 0)) op)) (failed) (ok)))]
                (loader/load-ntriples-async! ::depot (str (nt-file 50)) :retry-backoff-ms 1))]
    (is (= 50 (:count stats)))
    (is (= #{:append-ack} @acks) "appends must be acknowledged")
    (is (= 50 (count @attempts)))
    (is (every? #(= 2 %) (vals @attempts)) "each append is retried until accepted")))

(deftest test-appends-that-keep-failing-throw
  (let [e (try
            (with-redefs [rama/foreign-append-async! (fn [_ _ _] (failed))]
              (loader/load-ntriples-async! ::depot (str (nt-file 10)) :retries 1 :retry-backoff-ms 1))
            nil
            (catch clojure.lang.ExceptionInfo e e))]
    (is (some? e) "a load that loses data must not return normally")
    (is (= {:failed 10 :total 10} (ex-data e)))))

(deftest test-window-bounds-in-flight-appends
  (let [in-flight (atom 0)
        peak (atom 0)
        later ^Executor (CompletableFuture/delayedExecutor 1 TimeUnit/MILLISECONDS)
        stats (with-redefs [rama/foreign-append-async!
                            (fn [_ _ _]
                              (let [f (CompletableFuture.)]
                                (swap! peak max (swap! in-flight inc))
                                (.execute later ^Runnable (fn []
                                                            (swap! in-flight dec)
                                                            (.complete f nil)))
                                f))]
                (loader/load-ntriples-async! ::depot (str (nt-file 40)) :window 3))]
    (is (= 40 (:count stats)))
    (is (<= 1 @peak 3))
    (is (zero? @in-flight) "returns only after every append is acknowledged")))

(defn- with-out-str-result
  "Result of f with its stdout discarded."
  [f]
  (binding [*out* (java.io.StringWriter.)] (f)))

(defn- counts
  "A read-total fn returning each of xs in turn, then repeating the last."
  [& xs]
  (let [remaining (atom xs)]
    (fn []
      (let [x (first @remaining)]
        (when (next @remaining) (swap! remaining rest))
        x))))

(def ^:private fast {:poll-ms 1 :settle-ms 5 :stall-ms 200 :max-wait-ms 2000})

(deftest test-await-indexed-triples
  (testing "waits for the target, treating a nil count as zero"
    (is (= 10 (cc/await-indexed-triples! (counts nil 0 5 10) 10 fast))))
  (testing "a store that already held data keeps waiting until the count stops moving"
    (is (= 200 (cc/await-indexed-triples! (counts 100 150 200) 10 fast))))
  (testing "a count that stops short returns instead of waiting forever"
    (is (= 7 (with-out-str-result #(cc/await-indexed-triples! (counts 7) 10 (assoc fast :stall-ms 10))))))
  (testing "an indexer still moving at the deadline is an error"
    (let [n (atom 0)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (cc/await-indexed-triples! #(swap! n inc) 1000000000
                                              (assoc fast :max-wait-ms 20)))))))

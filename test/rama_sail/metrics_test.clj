(ns rama-sail.metrics-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [rama-sail.metrics :as metrics]
            [rama-sail.server.sparql :as sparql]))

(deftest metrics-text-includes-recorded-values
  (testing "exported Prometheus text reflects recorded metrics"
    (metrics/reset-metrics!)
    (metrics/inc-query-count "success")
    (metrics/record-query-latency 0.125)
    (metrics/inc-connections)
    (metrics/record-result-size 3)
    (metrics/inc-transaction-count "commit")
    (metrics/record-transaction-ops :add 2)
    (let [text (metrics/metrics-text)]
      (is (str/includes? text "ramasail_queries_total{status=\"success\",} 1.0"))
      (is (str/includes? text "ramasail_active_connections 1.0"))
      (is (str/includes? text "ramasail_transactions_total{status=\"commit\",} 1.0"))
      (is (str/includes? text "ramasail_transaction_ops_total{type=\"add\",} 2.0"))
      (is (str/includes? text "ramasail_query_latency_seconds_bucket"))
      (is (str/includes? text "ramasail_query_result_size")))))

(deftest sparql-app-exposes-metrics-and-health
  (testing "the SPARQL Ring app serves /metrics and /health directly"
    (metrics/reset-metrics!)
    (metrics/inc-query-count "success")
    (let [app (sparql/build-app nil)
          metrics-response (app {:request-method :get
                                 :uri "/metrics"
                                 :headers {}
                                 :params {}})
          health-response (app {:request-method :get
                                :uri "/health"
                                :headers {}
                                :params {}})]
      (is (= 200 (:status metrics-response)))
      (is (str/includes? (get-in metrics-response [:headers "Content-Type"]) "text/plain"))
      (is (str/includes? (:body metrics-response) "ramasail_queries_total"))
      (is (= 200 (:status health-response)))
      (is (= "OK" (:body health-response))))))

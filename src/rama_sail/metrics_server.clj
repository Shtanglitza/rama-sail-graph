(ns rama-sail.metrics-server
  "Simple HTTP server for Prometheus metrics scraping.

   Exposes a /metrics endpoint that returns metrics in Prometheus text format.
   This is optional - you can also integrate metrics into your existing HTTP server.

   Usage:
     ;; Start metrics server on port 9091
     (def server (start-metrics-server 9091))

     ;; Stop when done
     (.stop server 0)

   Then configure Prometheus to scrape:
     - job_name: 'ramasail'
       static_configs:
         - targets: ['localhost:9091']"
  (:require [rama-sail.metrics :as metrics]
            [clojure.tools.logging :as log])
  (:import [com.sun.net.httpserver HttpServer HttpHandler HttpExchange]
           [java.net InetSocketAddress]))

(defn- metrics-handler
  "HTTP handler that returns Prometheus metrics."
  []
  (reify HttpHandler
    (^void handle [_ ^HttpExchange exchange]
      (try
        (let [^String response (metrics/metrics-text)
              ^bytes bytes (.getBytes response "UTF-8")]
          ;; Set content type for Prometheus
          (-> exchange .getResponseHeaders (.set "Content-Type" "text/plain; version=0.0.4; charset=utf-8"))
          (.sendResponseHeaders exchange 200 (count bytes))
          (with-open [os (.getResponseBody exchange)]
            (.write os bytes)))
        (catch Exception e
          (log/error e "Error serving metrics")
          (.sendResponseHeaders exchange 500 -1))
        (finally
          (.close exchange))))))

(defn- health-handler
  "HTTP handler for health checks."
  []
  (reify HttpHandler
    (^void handle [_ ^HttpExchange exchange]
      (try
        (let [^String response "OK"
              ^bytes bytes (.getBytes response "UTF-8")]
          (.sendResponseHeaders exchange 200 (count bytes))
          (with-open [os (.getResponseBody exchange)]
            (.write os bytes)))
        (finally
          (.close exchange))))))

(defn start-metrics-server
  "Start a simple HTTP server exposing /metrics and /health endpoints.

   Args:
     port - Port to listen on (default 9091)

   Returns:
     HttpServer instance. Call (.stop server 0) to shutdown."
  ([]
   (start-metrics-server 9091))
  ([port]
   (let [server (HttpServer/create (InetSocketAddress. port) 0)]
     (.createContext server "/metrics" (metrics-handler))
     (.createContext server "/health" (health-handler))
     (.setExecutor server nil)
     (.start server)
     (log/info "Metrics server started on port" port)
     (log/info "  /metrics - Prometheus metrics endpoint")
     (log/info "  /health  - Health check endpoint")
     server)))

(defn stop-metrics-server
  "Stop the metrics server gracefully.

   Args:
     server - HttpServer instance from start-metrics-server
     delay  - Seconds to wait for pending requests (default 1)"
  ([server]
   (stop-metrics-server server 1))
  ([^HttpServer server delay-seconds]
   (when server
     (.stop server delay-seconds)
     (log/info "Metrics server stopped"))))

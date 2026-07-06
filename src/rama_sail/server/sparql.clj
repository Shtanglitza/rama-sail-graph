(ns rama-sail.server.sparql
  "W3C SPARQL Protocol HTTP endpoint for Rama Sail.

   Exposes a SPARQL query endpoint compatible with gdotv and other
   SPARQL clients connecting as 'Eclipse RDF4J' data sources.

   Usage:
     ;; IPC mode (dev/test)
     lein run -m rama-sail.server.sparql -- --port 7200 --mode ipc

     ;; Cluster mode (production)
     lein run -m rama-sail.server.sparql -- --port 7200 --mode cluster --host localhost --rama-port 1973"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [rama-sail.metrics :as metrics]
            [rama-sail.server.connection :as conn]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :refer [wrap-params]])
  (:import [java.io ByteArrayOutputStream]
           [org.eclipse.rdf4j.query QueryLanguage
            TupleQuery GraphQuery BooleanQuery Update]
           [org.eclipse.rdf4j.query.resultio QueryResultIO
            TupleQueryResultFormat BooleanQueryResultFormat]
           [org.eclipse.rdf4j.rio RDFFormat Rio]
           [org.eclipse.rdf4j.repository.sail SailRepository])
  (:gen-class))

;; ---------------------------------------------------------------------------
;; Content Negotiation
;; ---------------------------------------------------------------------------

(def ^:private tuple-formats
  "Supported formats for SELECT/ASK tuple results, ordered by preference."
  [{:media-type "application/sparql-results+json" :format TupleQueryResultFormat/JSON}
   {:media-type "application/sparql-results+xml"  :format TupleQueryResultFormat/SPARQL}
   {:media-type "text/csv"                        :format TupleQueryResultFormat/CSV}
   {:media-type "text/tab-separated-values"       :format TupleQueryResultFormat/TSV}])

(def ^:private boolean-formats
  "Supported formats for ASK boolean results."
  [{:media-type "application/sparql-results+json" :format BooleanQueryResultFormat/JSON}
   {:media-type "application/sparql-results+xml"  :format BooleanQueryResultFormat/SPARQL}])

(def ^:private rdf-formats
  "Supported formats for CONSTRUCT/DESCRIBE graph results."
  [{:media-type "text/turtle"         :format RDFFormat/TURTLE}
   {:media-type "application/rdf+xml"  :format RDFFormat/RDFXML}
   {:media-type "application/n-triples" :format RDFFormat/NTRIPLES}])

(defn- parse-accept
  "Parse Accept header into ordered list of media types, most preferred first.
   Stable sort: entries with equal q-values preserve their original order."
  [accept-header]
  (if (str/blank? accept-header)
    ["*/*"]
    (->> (str/split accept-header #",")
         (map str/trim)
         (map-indexed (fn [idx entry]
                        (let [[media-type & params] (str/split entry #";")
                              q (or (some->> params
                                             (some #(when (str/starts-with? (str/trim %) "q=")
                                                      (str/trim %)))
                                             (re-find #"q=([\d.]+)")
                                             second
                                             parse-double)
                                    1.0)]
                          {:media-type (str/trim media-type) :q q :idx idx})))
         ;; Stable sort: by q descending, then by original order ascending
         (sort-by (juxt (comp - :q) :idx))
         (map :media-type))))

(defn- media-range-matches?
  "Check if a media range matches a specific media type.
   Supports exact match, type/* ranges, and */* wildcard."
  [range-str media-type-str]
  (cond
    (= range-str "*/*") true
    (str/ends-with? range-str "/*")
    (let [range-type (subs range-str 0 (str/index-of range-str "/"))]
      (str/starts-with? media-type-str (str range-type "/")))
    :else (= range-str media-type-str)))

(defn- negotiate-tuple-format
  "Select best tuple result format based on Accept header.
   Supports exact media types, type/* ranges, and */* wildcard."
  [accept-header]
  (let [accepted (parse-accept accept-header)]
    (or (some (fn [media]
                (some (fn [{:keys [media-type format]}]
                        (when (media-range-matches? media media-type) format))
                      tuple-formats))
              accepted)
        TupleQueryResultFormat/JSON)))

(defn- negotiate-boolean-format
  "Select best boolean result format based on Accept header.
   Supports exact media types, type/* ranges, and */* wildcard."
  [accept-header]
  (let [accepted (parse-accept accept-header)]
    (or (some (fn [media]
                (some (fn [{:keys [media-type format]}]
                        (when (media-range-matches? media media-type) format))
                      boolean-formats))
              accepted)
        BooleanQueryResultFormat/JSON)))

(defn- negotiate-rdf-format
  "Select best RDF format based on Accept header.
   Supports exact media types, type/* ranges, and */* wildcard."
  [accept-header]
  (let [accepted (parse-accept accept-header)]
    (or (some (fn [media]
                (some (fn [{:keys [media-type format]}]
                        (when (media-range-matches? media media-type) format))
                      rdf-formats))
              accepted)
        RDFFormat/TURTLE)))

(defn- format->content-type
  "Get the primary MIME type string for an RDF4J format object."
  [fmt]
  (cond
    (instance? TupleQueryResultFormat fmt)
    (-> ^TupleQueryResultFormat fmt .getDefaultMIMEType)

    (instance? BooleanQueryResultFormat fmt)
    (-> ^BooleanQueryResultFormat fmt .getDefaultMIMEType)

    (instance? RDFFormat fmt)
    (-> ^RDFFormat fmt .getDefaultMIMEType)

    :else "application/octet-stream"))

;; ---------------------------------------------------------------------------
;; Query Extraction
;; ---------------------------------------------------------------------------

(defn- extract-query
  "Extract SPARQL query string from a Ring request.
   Supports:
     - GET with ?query= parameter
     - POST with application/x-www-form-urlencoded (query parameter)
     - POST with application/sparql-query body
     - POST with application/sparql-update body"
  [request]
  (let [method (:request-method request)
        content-type (get-in request [:headers "content-type"] "")
        params (:params request)]
    (cond
      ;; GET with query parameter
      (= method :get)
      (or (get params "query")
          (get params :query))

      ;; POST with URL-encoded form (query or update parameter)
      (and (= method :post)
           (str/starts-with? content-type "application/x-www-form-urlencoded"))
      (or (get params "query")
          (get params "update"))

      ;; POST with SPARQL query body
      (and (= method :post)
           (str/starts-with? content-type "application/sparql-query"))
      (slurp (:body request))

      ;; POST with SPARQL update body
      (and (= method :post)
           (str/starts-with? content-type "application/sparql-update"))
      (slurp (:body request))

      ;; POST with no content type — try params then body
      (= method :post)
      (or (get params "query")
          (get params "update")
          (when (:body request) (slurp (:body request)))))))

(defn- update-request?
  "Check if the request is a SPARQL Update (not query)."
  [request]
  (let [content-type (get-in request [:headers "content-type"] "")
        params (:params request)]
    (or (str/starts-with? content-type "application/sparql-update")
        (and (get params "update")
             (not (get params "query"))))))

;; ---------------------------------------------------------------------------
;; Query Execution & Serialization
;; ---------------------------------------------------------------------------

(defn- execute-tuple-query
  "Execute a SELECT query and serialize results."
  [^TupleQuery query accept-header]
  (let [fmt (negotiate-tuple-format accept-header)
        baos (ByteArrayOutputStream.)
        writer (QueryResultIO/createWriter fmt baos)]
    ;; evaluate(handler) streams results through the writer
    (.evaluate query writer)
    {:status 200
     :headers {"Content-Type" (str (format->content-type fmt) ";charset=utf-8")}
     :body (.toByteArray baos)}))

(defn- execute-graph-query
  "Execute a CONSTRUCT/DESCRIBE query and serialize results."
  [^GraphQuery query accept-header]
  (let [fmt (negotiate-rdf-format accept-header)
        baos (ByteArrayOutputStream.)
        writer (Rio/createWriter ^RDFFormat fmt baos)]
    ;; evaluate(handler) streams statements through the RDF writer
    (.evaluate query writer)
    {:status 200
     :headers {"Content-Type" (str (format->content-type fmt) ";charset=utf-8")}
     :body (.toByteArray baos)}))

(defn- execute-boolean-query
  "Execute an ASK query and serialize results."
  [^BooleanQuery query accept-header]
  (let [fmt (negotiate-boolean-format accept-header)
        result (.evaluate query)
        baos (ByteArrayOutputStream.)
        writer (QueryResultIO/createBooleanWriter fmt baos)]
    (.handleBoolean writer result)
    {:status 200
     :headers {"Content-Type" (str (format->content-type fmt) ";charset=utf-8")}
     :body (.toByteArray baos)}))

(defn- execute-update
  "Execute a SPARQL Update operation."
  [^Update update]
  (.execute update)
  {:status 200
   :headers {"Content-Type" "text/plain;charset=utf-8"}
   :body "Update successful"})

;; ---------------------------------------------------------------------------
;; Ring Handlers
;; ---------------------------------------------------------------------------

(def default-server-options
  "Default security posture for the SPARQL endpoint.
   Read-only unless updates are explicitly enabled — a form-encoded POST is a
   CORS 'simple request', so any web page could otherwise fire a destructive
   `DELETE WHERE { ?s ?p ?o }` at a reachable server (finding C4)."
  {:allow-updates false   ; SPARQL Update rejected with 403 unless true
   :auth-token    nil     ; when set, updates require `Authorization: Bearer <token>`
   :cors-origin   "*"})   ; Access-Control-Allow-Origin value

(defn- authorized-update?
  "An update is authorized only when updates are enabled and, if an auth token
   is configured, the request carries a matching Bearer token. A configured
   token is a non-simple request header, so a cross-site simple POST cannot
   supply it — closing the CSRF hole even when updates are enabled."
  [request {:keys [allow-updates auth-token]}]
  (and allow-updates
       (or (nil? auth-token)
           (= (get-in request [:headers "authorization"])
              (str "Bearer " auth-token)))))

(defn make-sparql-handler
  "Create the SPARQL Protocol Ring handler.
   Takes a SailRepository that must already be initialized and a server-options
   map (see `default-server-options`)."
  ([^SailRepository repo] (make-sparql-handler repo default-server-options))
  ([^SailRepository repo options]
   (let [{:keys [allow-updates] :as opts} (merge default-server-options options)]
     (fn [request]
       (let [query-str (extract-query request)
             accept (get-in request [:headers "accept"] "*/*")]
         (cond
           (str/blank? query-str)
           {:status 400
            :headers {"Content-Type" "text/plain;charset=utf-8"}
            :body "Missing query parameter. Use ?query= or POST with application/sparql-query body."}

           ;; SPARQL Update requested but updates are disabled (read-only default)
           (and (update-request? request) (not allow-updates))
           {:status 403
            :headers {"Content-Type" "text/plain;charset=utf-8"}
            :body "SPARQL Update is disabled. Server is read-only; start with --allow-updates to enable."}

           ;; Updates enabled but this request is not authorized (missing/wrong token)
           (and (update-request? request) (not (authorized-update? request opts)))
           {:status 401
            :headers {"Content-Type" "text/plain;charset=utf-8"
                      "WWW-Authenticate" "Bearer"}
            :body "Unauthorized. SPARQL Update requires a valid Bearer token."}

           :else
           (let [conn (.getConnection repo)]
             (try
               (if (update-request? request)
                 ;; SPARQL Update (authorized above)
                 (let [update (.prepareUpdate conn QueryLanguage/SPARQL query-str)]
                   (execute-update update))
                 ;; SPARQL Query — detect type by preparing it
                 (let [query (.prepareQuery conn QueryLanguage/SPARQL query-str)]
                   (cond
                     (instance? TupleQuery query)
                     (execute-tuple-query query accept)

                     (instance? GraphQuery query)
                     (execute-graph-query query accept)

                     (instance? BooleanQuery query)
                     (execute-boolean-query query accept)

                     :else
                     {:status 400
                      :headers {"Content-Type" "text/plain;charset=utf-8"}
                      :body "Unsupported query type."})))
               (catch org.eclipse.rdf4j.query.MalformedQueryException e
                 (log/warn "Malformed SPARQL query:" (.getMessage e))
                 {:status 400
                  :headers {"Content-Type" "text/plain;charset=utf-8"}
                  :body (str "Malformed query: " (.getMessage e))})
               (catch org.eclipse.rdf4j.query.QueryInterruptedException e
                 (log/warn "Query timeout:" (.getMessage e))
                 ;; 503, not 408: the client's request was not itself slow to
                 ;; arrive; the server gave up evaluating it (server-side condition).
                 {:status 503
                  :headers {"Content-Type" "text/plain;charset=utf-8"}
                  :body "Query execution timed out."})
               (catch Exception e
                 (log/error e "SPARQL query execution error")
                 {:status 500
                  :headers {"Content-Type" "text/plain;charset=utf-8"}
                  :body (str "Internal error: " (.getMessage e))})
               (finally
                 (.close conn))))))))))

;; ---------------------------------------------------------------------------
;; Middleware
;; ---------------------------------------------------------------------------

(defn wrap-cors
  "Add CORS headers to allow cross-origin requests from gdotv and browsers.
   `cors-origin` is the Access-Control-Allow-Origin value (default \"*\"). Set a
   specific origin when SPARQL Update is enabled so a wildcard does not expose
   the mutating endpoint to arbitrary web pages (finding C4)."
  ([handler] (wrap-cors handler "*"))
  ([handler cors-origin]
   (let [cors-headers {"Access-Control-Allow-Origin"  cors-origin
                       "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
                       "Access-Control-Allow-Headers" "Content-Type, Accept, Authorization"}]
     (fn [request]
       (if (= (:request-method request) :options)
         ;; Preflight
         {:status 204
          :headers (assoc cors-headers "Access-Control-Max-Age" "86400")}
         ;; Normal request
         (update (handler request) :headers merge cors-headers))))))

(defn wrap-request-logging
  "Log incoming SPARQL requests."
  [handler]
  (fn [request]
    (let [start (System/currentTimeMillis)
          response (handler request)
          elapsed (- (System/currentTimeMillis) start)]
      (log/info (str (:request-method request) " " (:uri request)
                     " → " (:status response) " (" elapsed "ms)"))
      response)))

;; ---------------------------------------------------------------------------
;; Server Lifecycle
;; ---------------------------------------------------------------------------

(defn build-app
  "Build the full Ring application with middleware.
   `options` (see `default-server-options`) controls update authorization and
   the CORS origin; defaults to a read-only, wildcard-CORS posture."
  ([^SailRepository repo] (build-app repo default-server-options))
  ([^SailRepository repo options]
   (let [opts (merge default-server-options options)
         sparql-handler (make-sparql-handler repo opts)]
     (-> (fn [request]
           (case (:uri request)
             "/sparql"  (sparql-handler request)
             "/health"  {:status 200
                         :headers {"Content-Type" "text/plain;charset=utf-8"}
                         :body "OK"}
             "/metrics" (try
                          {:status 200
                           :headers {"Content-Type" "text/plain; version=0.0.4; charset=utf-8"}
                           :body (metrics/metrics-text)}
                          (catch Exception e
                            (log/error e "Error serving metrics")
                            {:status 500
                             :headers {"Content-Type" "text/plain;charset=utf-8"}
                             :body "Internal error"}))
             "/"        {:status 200
                         :headers {"Content-Type" "text/plain;charset=utf-8"}
                         :body "Rama Sail SPARQL Endpoint. Query at /sparql\n\nEndpoints:\n  /sparql  - SPARQL Protocol\n  /health  - Health check\n  /metrics - Prometheus metrics"}
             {:status 404
              :headers {"Content-Type" "text/plain;charset=utf-8"}
              :body "Not found. SPARQL endpoint is at /sparql"}))
         wrap-params
         (wrap-cors (:cors-origin opts))
         wrap-request-logging))))

(defn start-sparql-server!
  "Start the SPARQL Protocol HTTP server.
   Takes a SailRepository, port number, and optional server-options map
   (see `default-server-options`).
   Returns the Jetty server instance."
  ([^SailRepository repo port] (start-sparql-server! repo port default-server-options))
  ([^SailRepository repo port options]
   (let [app (build-app repo options)]
     (log/info (str "Starting SPARQL endpoint on port " port "..."))
     (jetty/run-jetty app {:port port :join? false}))))

(defn stop-sparql-server!
  "Stop the Jetty server."
  [^org.eclipse.jetty.server.Server server]
  (when server
    (.stop server)
    (log/info "SPARQL endpoint stopped.")))

;; ---------------------------------------------------------------------------
;; CLI Entry Point
;; ---------------------------------------------------------------------------

(defn- parse-long-arg
  "parse-long but fail fast with a clear message instead of NPE/nil on bad input."
  [flag val]
  (or (and val (parse-long val))
      (throw (ex-info (str "Flag " flag " requires an integer value, got: " (pr-str val)) {}))))

(defn- parse-args
  "Parse CLI arguments into a config map.
   Value flags consume the next arg; `--allow-updates` is a boolean flag."
  [args]
  (loop [args (seq args)
         config {:mode "ipc" :port 7200 :rama-port 1973
                 :allow-updates false :auth-token nil :cors-origin "*"}]
    (if-not args
      config
      (let [[flag val & more] args]
        (case flag
          "--port"          (recur more (assoc config :port (parse-long-arg flag val)))
          "--mode"          (recur more (assoc config :mode val))
          "--host"          (recur more (assoc config :host val))
          "--rama-port"     (recur more (assoc config :rama-port (parse-long-arg flag val)))
          "--allow-updates" (recur (next args) (assoc config :allow-updates true))
          "--auth-token"    (recur more (assoc config :auth-token val))
          "--cors-origin"   (recur more (assoc config :cors-origin val))
          ;; skip unknown flags (advance by one)
          (recur (next args) config))))))

(defn -main [& args]
  (let [config (parse-args args)
        _ (println (str "Rama Sail SPARQL Endpoint"))
        _ (println (str "Mode: " (:mode config) ", Port: " (:port config)))
        conn-state (case (:mode config)
                     "ipc" (do
                             (println "Starting IPC cluster...")
                             (conn/start-ipc!))
                     "cluster" (do
                                 (println (str "Connecting to cluster at "
                                               (or (:host config) "localhost") ":"
                                               (:rama-port config) "..."))
                                 (conn/start-cluster!
                                  {:host (or (:host config) "localhost")
                                   :port (:rama-port config)})))
        repo (:repo conn-state)
        server-options {:allow-updates (:allow-updates config)
                        :auth-token    (:auth-token config)
                        :cors-origin   (:cors-origin config)}
        server (start-sparql-server! repo (:port config) server-options)]

    ;; Surface the security posture and warn on the risky combination:
    ;; writable + wildcard CORS + no token = any web page can mutate the store.
    (println (str "Updates: " (if (:allow-updates config) "ENABLED" "disabled (read-only)")))
    (when (:allow-updates config)
      (if (:auth-token config)
        (println "Update auth: Bearer token required")
        (println "Update auth: NONE — anyone who can reach this port can modify data"))
      (when (and (= "*" (:cors-origin config)) (not (:auth-token config)))
        (println "WARNING: updates enabled with wildcard CORS and no --auth-token; any web")
        (println "         page can issue mutating requests. Set --auth-token and/or --cors-origin.")))
    (println (str "SPARQL endpoint ready at http://localhost:" (:port config) "/sparql"))
    (println (str "Health check at http://localhost:" (:port config) "/health"))
    (println (str "Prometheus metrics at http://localhost:" (:port config) "/metrics"))
    (println "Connect gdotv as 'Eclipse RDF4J' → localhost:" (:port config))

    ;; Shutdown hook
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. ^Runnable
      (fn []
        (println "Shutting down...")
        (stop-sparql-server! server)
        (conn/stop! conn-state))))

    ;; Block main thread
    (.join ^org.eclipse.jetty.server.Server server)))

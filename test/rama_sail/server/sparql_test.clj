(ns rama-sail.server.sparql-test
  "Integration tests for the SPARQL Protocol HTTP endpoint.

   Uses a shared IPC fixture. Tests HTTP handler directly (no Jetty needed)
   and optionally starts a real server for full HTTP round-trip tests."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [com.rpl.rama :as rama]
            [com.rpl.rama.test :as rtest]
            [rama-sail.core :refer [RdfStorageModule]]
            [rama-sail.sail.adapter :as rsail]
            [rama-sail.server.sparql :as sparql])
  (:import [org.eclipse.rdf4j.repository.sail SailRepository]
           [java.io ByteArrayInputStream]))

(def module-name (rama/get-module-name RdfStorageModule))

;; ---------------------------------------------------------------------------
;; Shared IPC Fixture
;; ---------------------------------------------------------------------------

(def ^:dynamic *repo* nil)
(def ^:dynamic *ipc* nil)
(def ^:dynamic *depot* nil)
(def ^:dynamic *handler* nil)

(defn sparql-fixture [f]
  (with-open [ipc (rtest/create-ipc)]
    (rtest/launch-module! ipc RdfStorageModule {:tasks 4 :threads 2})
    (let [depot (rama/foreign-depot ipc module-name "*triple-depot")
          sail  (rsail/create-rama-sail ipc module-name)
          repo  (SailRepository. sail)]
      (.init repo)
      (try
        (binding [*repo*    repo
                  *ipc*     ipc
                  *depot*   depot
                  *handler* (sparql/build-app repo)]
          (f))
        (finally (.shutDown repo))))))

(use-fixtures :once sparql-fixture)

(defn wait-mb!
  "Barrier: wait until the indexer has processed every depot record appended so
   far, deriving the exact target from the depot partitions' end-offsets. Robust
   to out-of-band appends (e.g. via SPARQL UPDATE) that a manually incremented
   counter would miss — see CLAUDE.md."
  []
  (let [num-partitions (:num-partitions (rama/foreign-object-info *depot*))
        total (reduce + (for [part (range num-partitions)]
                          (:end-offset (rama/foreign-depot-partition-info *depot* part))))]
    (rtest/wait-for-microbatch-processed-count *ipc* module-name "indexer" total)))

(defn load-triples!
  "Load triples into the store via depot append."
  [triples]
  (let [now (System/currentTimeMillis)]
    (doseq [[s p o] triples]
      (rama/foreign-append! *depot* [:add [s p o "<http://default>"] now]))
    (wait-mb!)))

;; ---------------------------------------------------------------------------
;; Helper: simulate Ring request
;; ---------------------------------------------------------------------------

(defn- get-request
  "Simulate a GET request to /sparql with query parameter."
  [query-str & {:keys [accept] :or {accept "application/sparql-results+json"}}]
  (*handler* {:request-method :get
              :uri "/sparql"
              :query-string (str "query=" (java.net.URLEncoder/encode query-str "UTF-8"))
              :headers {"accept" accept}}))

(defn- post-request-query
  "Simulate a POST request with application/sparql-query body."
  [query-str & {:keys [accept] :or {accept "application/sparql-results+json"}}]
  (*handler* {:request-method :post
              :uri "/sparql"
              :headers {"content-type" "application/sparql-query"
                        "accept" accept}
              :body (ByteArrayInputStream. (.getBytes query-str "UTF-8"))}))

(defn- post-request-form
  "Simulate a POST request with form-encoded query parameter."
  [query-str & {:keys [accept] :or {accept "application/sparql-results+json"}}]
  (*handler* {:request-method :post
              :uri "/sparql"
              :headers {"content-type" "application/x-www-form-urlencoded"
                        "accept" accept}
              :body (ByteArrayInputStream.
                     (.getBytes (str "query=" (java.net.URLEncoder/encode query-str "UTF-8"))
                                "UTF-8"))}))

(defn- parse-json-body
  "Parse response body bytes as JSON."
  [response]
  (json/read-str (String. ^bytes (:body response) "UTF-8")))

;; ---------------------------------------------------------------------------
;; Tests
;; ---------------------------------------------------------------------------

(deftest test-missing-query
  (testing "GET /sparql without query returns 400"
    (let [resp (*handler* {:request-method :get
                           :uri "/sparql"
                           :headers {"accept" "*/*"}})]
      (is (= 400 (:status resp))))))

(defn- body-str
  "Extract response body as string, handling both String and byte[] bodies."
  [response]
  (let [b (:body response)]
    (if (string? b) b (String. ^bytes b "UTF-8"))))

(deftest test-malformed-query
  (testing "Malformed SPARQL returns 400"
    (let [resp (get-request "NOT VALID SPARQL")]
      (is (= 400 (:status resp)))
      (is (str/includes? (body-str resp) "Malformed query")))))

(deftest test-select-json
  (testing "SELECT query returns SPARQL Results JSON"
    (load-triples! [["<http://ex.org/alice>" "<http://ex.org/name>" "\"Alice\""]])
    (let [resp (get-request "SELECT ?s ?o WHERE { ?s <http://ex.org/name> ?o }")
          body (parse-json-body resp)]
      (is (= 200 (:status resp)))
      (is (str/includes? (get-in resp [:headers "Content-Type"])
                         "application/sparql-results+json"))
      ;; SPARQL JSON structure
      (is (contains? body "head"))
      (is (contains? body "results"))
      (is (= ["s" "o"] (get-in body ["head" "vars"])))
      (let [bindings (get-in body ["results" "bindings"])]
        (is (pos? (count bindings)))
        (is (= "http://ex.org/alice"
               (get-in (first bindings) ["s" "value"])))))))

(deftest test-select-xml
  (testing "SELECT with Accept: application/sparql-results+xml returns XML"
    (load-triples! [["<http://ex.org/bob>" "<http://ex.org/age>" "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"]])
    (let [resp (get-request "SELECT ?s WHERE { ?s <http://ex.org/age> ?o }"
                            :accept "application/sparql-results+xml")]
      (is (= 200 (:status resp)))
      (is (str/includes? (get-in resp [:headers "Content-Type"])
                         "application/sparql-results+xml"))
      (let [body-str (String. ^bytes (:body resp) "UTF-8")]
        (is (str/includes? body-str "sparql"))))))

(deftest test-ask-query
  (testing "ASK query returns boolean result"
    (load-triples! [["<http://ex.org/carol>" "<http://ex.org/type>" "<http://ex.org/Person>"]])
    (let [resp (get-request "ASK { <http://ex.org/carol> <http://ex.org/type> <http://ex.org/Person> }")
          body (parse-json-body resp)]
      (is (= 200 (:status resp)))
      (is (contains? body "boolean"))
      (is (true? (get body "boolean"))))))

(deftest test-construct-query
  (testing "CONSTRUCT query returns RDF"
    (load-triples! [["<http://ex.org/dan>" "<http://ex.org/knows>" "<http://ex.org/eve>"]])
    (let [resp (get-request
                "CONSTRUCT { ?s <http://ex.org/knows> ?o } WHERE { ?s <http://ex.org/knows> ?o }"
                :accept "text/turtle")]
      (is (= 200 (:status resp)))
      (is (str/includes? (get-in resp [:headers "Content-Type"]) "text/turtle"))
      (let [body-str (String. ^bytes (:body resp) "UTF-8")]
        (is (str/includes? body-str "ex.org/dan"))))))

(deftest test-post-sparql-query-content-type
  (testing "POST with application/sparql-query body works"
    (let [resp (post-request-query "SELECT ?s WHERE { ?s ?p ?o } LIMIT 1")]
      (is (= 200 (:status resp))))))

(deftest test-post-form-encoded
  (testing "POST with form-encoded query parameter works"
    (let [resp (post-request-form "SELECT ?s WHERE { ?s ?p ?o } LIMIT 1")]
      (is (= 200 (:status resp))))))

(deftest test-cors-headers
  (testing "Response includes CORS headers"
    (let [resp (get-request "SELECT ?s WHERE { ?s ?p ?o } LIMIT 1")]
      (is (= "*" (get-in resp [:headers "Access-Control-Allow-Origin"])))))

  (testing "OPTIONS preflight returns 204"
    (let [resp (*handler* {:request-method :options
                           :uri "/sparql"
                           :headers {}})]
      (is (= 204 (:status resp)))
      (is (= "*" (get-in resp [:headers "Access-Control-Allow-Origin"]))))))

(deftest test-cors-origin-configurable
  (testing "A configured CORS origin is reflected instead of the wildcard (C4)"
    (let [app (sparql/build-app *repo* {:cors-origin "https://app.example"})
          resp (app {:request-method :get :uri "/sparql"
                     :query-string "query=SELECT%20*%20WHERE%20%7B%20%3Fs%20%3Fp%20%3Fo%20%7D%20LIMIT%201"
                     :headers {"accept" "application/sparql-results+json"}})]
      (is (= "https://app.example"
             (get-in resp [:headers "Access-Control-Allow-Origin"]))))))

;; ---------------------------------------------------------------------------
;; Security: SPARQL Update gating (finding C4)
;; ---------------------------------------------------------------------------

(defn- post-update
  "POST a SPARQL Update. Defaults to application/sparql-update body; pass
   :form? true to use the form-encoded `update=` CSRF simple-request vector."
  [handler update-str & {:keys [auth form?]}]
  (let [headers (cond-> {"accept" "*/*"}
                  (not form?) (assoc "content-type" "application/sparql-update")
                  form?       (assoc "content-type" "application/x-www-form-urlencoded")
                  auth        (assoc "authorization" auth))
        body (if form?
               (str "update=" (java.net.URLEncoder/encode update-str "UTF-8"))
               update-str)]
    (handler {:request-method :post :uri "/sparql"
              :headers headers
              :body (ByteArrayInputStream. (.getBytes ^String body "UTF-8"))})))

(deftest test-update-rejected-in-read-only-mode
  (testing "Default (read-only) handler rejects a SPARQL Update body with 403"
    (let [resp (post-update *handler* "DELETE WHERE { ?s ?p ?o }")]
      (is (= 403 (:status resp)))
      (is (str/includes? (body-str resp) "read-only"))))

  (testing "The form-encoded CSRF vector is also rejected with 403"
    ;; A form POST is a CORS 'simple request'; this is the exact drive-by hole.
    (let [resp (post-update *handler* "DROP ALL" :form? true)]
      (is (= 403 (:status resp))))))

(deftest test-update-requires-valid-token
  (let [app (sparql/build-app *repo* {:allow-updates true :auth-token "s3cret"})]
    (testing "Enabled updates without a token are 401"
      (let [resp (post-update app "INSERT DATA { <http://ex/a> <http://ex/b> <http://ex/c> }")]
        (is (= 401 (:status resp)))
        (is (= "Bearer" (get-in resp [:headers "WWW-Authenticate"])))))

    (testing "A wrong token is 401"
      (let [resp (post-update app "INSERT DATA { <http://ex/a> <http://ex/b> <http://ex/c> }"
                              :auth "Bearer nope")]
        (is (= 401 (:status resp)))))))

(deftest test-update-authorized-round-trips
  (testing "An authorized INSERT DATA writes data that a later SELECT sees"
    (let [s (str "<http://ex.org/upd-" (System/nanoTime) ">")
          app (sparql/build-app *repo* {:allow-updates true :auth-token "s3cret"})
          upd (post-update app (str "INSERT DATA { " s " <http://ex.org/p> \"v\" }")
                           :auth "Bearer s3cret")]
      (is (= 200 (:status upd)))
      (wait-mb!) ; ensure the out-of-band UPDATE write is indexed before reading
      (let [resp (get-request (str "SELECT ?o WHERE { " s " <http://ex.org/p> ?o }"))
            bindings (get-in (parse-json-body resp) ["results" "bindings"])]
        (is (= 200 (:status resp)))
        (is (= "v" (get-in (first bindings) ["o" "value"])))))))

(deftest test-root-endpoint
  (testing "GET / returns welcome message"
    (let [resp (*handler* {:request-method :get
                           :uri "/"
                           :headers {}})]
      (is (= 200 (:status resp))))))

(deftest test-404
  (testing "Unknown path returns 404"
    (let [resp (*handler* {:request-method :get
                           :uri "/unknown"
                           :headers {}})]
      (is (= 404 (:status resp))))))

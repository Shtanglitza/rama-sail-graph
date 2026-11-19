(def integration-namespaces
  "The :integration selector's test namespaces, in two halves that CI runs as parallel jobs
   (:integration-module, :integration-sail). Split by layer; on GitHub's runners (Nov 16, 2026)
   the halves took about 250 s and 240 s of a 490 s run. Keep them balanced when adding one."
  {:module #{"rama-sail.module.indexer-test"
             "rama-sail.query.aggregate-test"
             "rama-sail.query.core-test"
             "rama-sail.query.find-bgp-test"
             "rama-sail.query.find-triples-test"
             "rama-sail.query.left-join-test"
             "rama-sail.query.transaction-test"
             "rama-sail.query.union-test"
             "rama-sail.bulk-test"
             "rama-sail.migrate-test"}
   :sail #{"rama-sail.sail.e2e-test"
           "rama-sail.sail.compliance-test"
           "rama-sail.sail.memorystore-parity-test"
           "rama-sail.sail.optimizer-equivalence-test"
           "rama-sail.sail.property-path-test"
           "rama-sail.sail.random-query-parity-test"
           "rama-sail.sail.reified-triples-test"
           "rama-sail.sail.triple-term-join-test"
           "rama-sail.server.sparql-test"}})

(defn- namespace-selector
  "A test selector form: true for a test var in one of the namespaces `names`.
   (defproject evaluates ~ at a map value, not inside a quoted fn form.)"
  [names]
  (list 'fn '[m] (list 'contains? names '(str (:ns m)))))

(defproject rama-sail-graph "0.2.0-SNAPSHOT"
  :description "Rama module that integrates with RDF4J SAIL API for RDF quad storage and SPARQL query evaluation"
  :url "https://github.com/Shtanglitza/rama-sail-graph"
  :license {:name "Apache-2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :jvm-opts ["-Xss6m"
             "-Xms6g"
             "-Xmx6g"]
  :dependencies [[com.rpl/rama-helpers "0.10.0" :exclusions [org.clojure/clojure]]
                 [org.eclipse.rdf4j/rdf4j-sail-api "6.1.0"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "6.1.0"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 [org.eclipse.rdf4j/rdf4j-repository-sail "6.1.0"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 ;; Prometheus metrics for observability
                 [io.prometheus/simpleclient "0.16.0"]
                 [io.prometheus/simpleclient_hotspot "0.16.0"]
                 [io.prometheus/simpleclient_common "0.16.0"]
                 ;; ring-jetty-adapter for SPARQL endpoint. Keep Ring at the version Rama
                 ;; bundles (Rama 1.10.0: Ring 1.15.5 on Jetty 12): an older adapter puts a
                 ;; Jetty 9.4 jetty-server next to Rama's Jetty 12 jetty-http/io/util.
                 ;; jetty-server itself comes from Rama (provided), so the whole Jetty
                 ;; stack is the one Rama ships.
                 [ring/ring-jetty-adapter "1.15.5" :exclusions [org.eclipse.jetty/jetty-server]]
                 ;; RDF4J result serializers for SPARQL Protocol HTTP endpoint. RDF4J 6 uses
                 ;; Jackson 3 (tools.jackson packages), separate from Rama's Jackson 2.
                 [org.eclipse.rdf4j/rdf4j-queryresultio-sparqljson "6.1.0"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 [org.eclipse.rdf4j/rdf4j-queryresultio-sparqlxml "6.1.0"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 [org.eclipse.rdf4j/rdf4j-queryresultio-text "6.1.0"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 ;; RDF writers for CONSTRUCT/DESCRIBE results
                 [org.eclipse.rdf4j/rdf4j-rio-turtle "6.1.0"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 [org.eclipse.rdf4j/rdf4j-rio-rdfxml "6.1.0"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 ;; N-Quads for bulk/load-file! and the SPARQL server's writers (TriG and
                 ;; N-Triples come with rdf4j-rio-turtle)
                 [org.eclipse.rdf4j/rdf4j-rio-nquads "6.1.0"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 ;; Ring utilities for request parsing
                 [ring/ring-core "1.15.5"]]
  :plugins [[lein-codox "0.10.8"]]
  :codox {:output-path "target/doc"
          :source-uri "https://github.com/Shtanglitza/rama-sail-graph/blob/main/{filepath}#L{line}"
          :metadata {:doc/format :markdown}
          :namespaces [#"^rama-sail\.core"
                       #"^rama-sail\.sail\.adapter"
                       #"^rama-sail\.server\."
                       #"^rama-sail\.metrics"
                       #"^rama-sail\.module\."]}
  ;; commons-io: RDF4J and Ring ask for older versions (2.18, 2.21) than Rama 1.10
  ;; bundles; nearer dependencies would otherwise win over Rama's.
  :managed-dependencies [[commons-io "2.22.0"]]
  :global-vars {*warn-on-reflection* true}
  :repositories
  [["releases" {:id  "maven-releases"
                :url "https://nexus.redplanetlabs.com/repository/maven-public-releases"}]]
  ;; :unit, :integration and :w3c are the CI gate (.github/workflows/ci.yml) and the
  ;; release gate (release.yml). Add a new test namespace to one of these sets, or to a
  ;; half of `integration-namespaces` above.
  :test-selectors {:unit (fn [m] (contains? #{"rama-sail.sail.optimizer-test"
                                              "rama-sail.sail.adapter-test"
                                              "rama-sail.sail.triple-refs-test"
                                              "rama-sail.sail.serialization-golden-test"
                                              "rama-sail.query.helpers-test"
                                              "rama-sail.query.expr-test"
                                              "rama-sail.query.term-order-test"
                                              "rama-sail.sail.bulk-load-test"
                                              "rama-sail.sail.serialization-roundtrip-test"
                                              "rama-sail.metrics-test"
                                              "rama-sail.server.connection-test"
                                              "rama-sail.ci-gate-test"}
                                            (str (:ns m))))
                   ;; :integration is both halves of `integration-namespaces`; CI runs the
                   ;; halves as parallel jobs.
                   :integration ~(namespace-selector (reduce into #{} (vals integration-namespaces)))
                   :integration-module ~(namespace-selector (:module integration-namespaces))
                   :integration-sail ~(namespace-selector (:sail integration-namespaces))
                   :w3c (fn [m] (contains? #{"rama-sail.sail.w3c.sparql11-property-path-test"
                                             "rama-sail.sail.w3c.sparql12-test"
                                             "rama-sail.sail.w3c.rdf12-test"}
                                           (str (:ns m))))
                   :default (fn [m] (not (or (:perf m) (:scale m)
                                             (and (:ns m) (re-find #"bench" (str (:ns m)))))))
                   :perf :perf
                   :scale :scale
                   :bench (fn [m] (and (:ns m) (re-find #"bench" (str (:ns m)))))
                   :all (constantly true)}
  :aot [rama-sail.core]
  :profiles {:dev      {:resource-paths ["test/resources/"]
                        :java-source-paths ["test-java"]
                        :dependencies [[org.eclipse.rdf4j/rdf4j-sail-testsuite "6.1.0"
                                        :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic]]
                                       ;; Reference store for differential (oracle) tests
                                       [org.eclipse.rdf4j/rdf4j-sail-memory "6.1.0"]
                                       [org.junit.platform/junit-platform-console-standalone "6.1.1"]]}
             :provided {:dependencies [[com.rpl/rama "1.10.0"]
                                       [org.clojure/clojure "1.12.6"]]}
             :scale    {:jvm-opts ["-Xss8m" "-Xms12g" "-Xmx12g"]}
             :sparql-uberjar {:aot [rama-sail.server.sparql]
                              :main rama-sail.server.sparql
                              :uberjar-name "rama-sail-sparql.jar"}}
	;:repl-options {:init-ns rama-sail.core}
  )

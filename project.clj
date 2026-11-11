(defproject rama-sail-graph "0.2.0-SNAPSHOT"
  :description "Rama module that integrates with RDF4J SAIL API for RDF quad storage and SPARQL query evaluation"
  :url "https://github.com/Shtanglitza/rama-sail-graph"
  :license {:name "Apache-2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :jvm-opts ["-Xss6m"
             "-Xms6g"
             "-Xmx6g"]
  :dependencies [[com.rpl/rama-helpers "0.10.0" :exclusions [org.clojure/clojure]]
                 [org.eclipse.rdf4j/rdf4j-sail-api "5.2.2"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 [org.eclipse.rdf4j/rdf4j-queryparser-sparql "5.2.2"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 [org.eclipse.rdf4j/rdf4j-repository-sail "5.2.2"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 ;; Prometheus metrics for observability
                 [io.prometheus/simpleclient "0.16.0"]
                 [io.prometheus/simpleclient_hotspot "0.16.0"]
                 [io.prometheus/simpleclient_common "0.16.0"]
                 ;; ring-jetty-adapter for SPARQL endpoint
                 ;; 1.10.0 uses Jetty 9.4.x matching Rama's bundled version
                 [ring/ring-jetty-adapter "1.10.0"]
                 ;; RDF4J result serializers for SPARQL Protocol HTTP endpoint
                 [org.eclipse.rdf4j/rdf4j-queryresultio-sparqljson "5.2.2"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 [org.eclipse.rdf4j/rdf4j-queryresultio-sparqlxml "5.2.2"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 [org.eclipse.rdf4j/rdf4j-queryresultio-text "5.2.2"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 ;; RDF writers for CONSTRUCT/DESCRIBE results
                 [org.eclipse.rdf4j/rdf4j-rio-turtle "5.2.2"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 [org.eclipse.rdf4j/rdf4j-rio-rdfxml "5.2.2"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic org.slf4j/slf4j-simple]]
                 ;; Ring utilities for request parsing
                 [ring/ring-core "1.10.0"]]
  :plugins [[lein-codox "0.10.8"]]
  :codox {:output-path "target/doc"
          :source-uri "https://github.com/Shtanglitza/rama-sail-graph/blob/main/{filepath}#L{line}"
          :metadata {:doc/format :markdown}
          :namespaces [#"^rama-sail\.core"
                       #"^rama-sail\.sail\.adapter"
                       #"^rama-sail\.server\."
                       #"^rama-sail\.metrics"
                       #"^rama-sail\.module\."]}
  :global-vars {*warn-on-reflection* true}
  :repositories
  [["releases" {:id  "maven-releases"
                :url "https://nexus.redplanetlabs.com/repository/maven-public-releases"}]]
  ;; :unit, :integration and :w3c are the CI gate (.github/workflows/ci.yml) and the
  ;; release gate (release.yml). Add a new test namespace to one of these sets.
  :test-selectors {:unit (fn [m] (contains? #{"rama-sail.sail.optimizer-test"
                                              "rama-sail.sail.adapter-test"
                                              "rama-sail.query.helpers-test"
                                              "rama-sail.query.expr-test"
                                              "rama-sail.query.term-order-test"
                                              "rama-sail.sail.bulk-load-test"
                                              "rama-sail.sail.serialization-roundtrip-test"
                                              "rama-sail.metrics-test"
                                              "rama-sail.ci-gate-test"}
                                            (str (:ns m))))
                   :integration (fn [m] (contains? #{"rama-sail.sail.e2e-test"
                                                     "rama-sail.query.transaction-test"
                                                     "rama-sail.query.find-triples-test"
                                                     "rama-sail.query.find-bgp-test"
                                                     "rama-sail.module.indexer-test"
                                                     "rama-sail.query.core-test"
                                                     "rama-sail.query.aggregate-test"
                                                     "rama-sail.query.left-join-test"
                                                     "rama-sail.query.union-test"
                                                     "rama-sail.sail.optimizer-equivalence-test"
                                                     "rama-sail.sail.memorystore-parity-test"
                                                     "rama-sail.sail.random-query-parity-test"
                                                     "rama-sail.sail.compliance-test"
                                                     "rama-sail.sail.property-path-test"
                                                     "rama-sail.sail.rdf-star-test"
                                                     "rama-sail.server.sparql-test"}
                                                   (str (:ns m))))
                   :w3c (fn [m] (contains? #{"rama-sail.sail.w3c.sparql11-property-path-test"
                                             "rama-sail.sail.w3c.sparql12-test"}
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
                        :dependencies [[org.eclipse.rdf4j/rdf4j-sail-testsuite "5.2.2"
                                        :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic]]
                                       ;; Reference store for differential (oracle) tests
                                       [org.eclipse.rdf4j/rdf4j-sail-memory "5.2.2"]
                                       [org.junit.platform/junit-platform-console-standalone "1.10.0"]]}
             :provided {:dependencies [[com.rpl/rama "1.9.0"]
                                       [org.clojure/clojure "1.12.4"]]}
             :scale    {:jvm-opts ["-Xss8m" "-Xms12g" "-Xmx12g"]}
             :sparql-uberjar {:aot [rama-sail.server.sparql]
                              :main rama-sail.server.sparql
                              :uberjar-name "rama-sail-sparql.jar"}}
	;:repl-options {:init-ns rama-sail.core}
  )

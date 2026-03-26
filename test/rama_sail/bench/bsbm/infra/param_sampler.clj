(ns rama-sail.bench.bsbm.infra.param-sampler
  "Parameter sampling for BSBM query templates.

   Discovers valid parameter values from loaded data and substitutes
   them into query templates."
  (:require [clojure.string :as str])
  (:import [org.eclipse.rdf4j.repository.sail SailRepository]
           [org.eclipse.rdf4j.model Resource]))

;;; ---------------------------------------------------------------------------
;;; BSBM URIs
;;; ---------------------------------------------------------------------------

(def bsbm-vocab "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/")
(def bsbm-inst "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/")

;;; ---------------------------------------------------------------------------
;;; Query Helpers
;;; ---------------------------------------------------------------------------

(defn- parse-rdf-numeric
  "Extract numeric value from RDF literal string.
   Handles formats: \"123\"^^<xsd:type>, \"123\", plain numbers"
  [v]
  (try
    (cond
      (nil? v) nil
      (number? v) (double v)
      ;; RDF literal: "value"^^<type> or "value"
      (str/starts-with? v "\"")
      (let [end-quote (str/index-of v "\"" 1)]
        (when end-quote
          (Double/parseDouble (subs v 1 end-quote))))
      ;; Plain string number
      :else (Double/parseDouble v))
    (catch Exception _ nil)))

(defn- execute-select
  "Execute a SPARQL SELECT query and return results as a vector of maps."
  [conn sparql]
  (try
    (let [query (.prepareTupleQuery conn sparql)]
      (with-open [iter (.evaluate query)]
        (vec (for [binding (iterator-seq iter)]
               (into {}
                     (for [name (.getBindingNames binding)
                           :let [value (.getValue binding name)]
                           :when value]
                       [name (str value)]))))))
    (catch Exception e
      (println "Query failed:" (.getMessage e))
      [])))

(defn- sample-n
  "Randomly sample up to n items from a collection."
  [coll n]
  (if (<= (count coll) n)
    (vec coll)
    (vec (take n (shuffle coll)))))

;;; ---------------------------------------------------------------------------
;;; Parameter Discovery
;;; ---------------------------------------------------------------------------

(defn sample-products
  "Sample product URIs from the store.
   Finds products that have BSBM product properties."
  [conn max-count]
  (let [results (execute-select conn
                                (str "SELECT DISTINCT ?product WHERE { "
                                     "?product <" bsbm-vocab "productPropertyNumeric1> ?v "
                                     "} LIMIT " (* max-count 2)))]
    (sample-n (map #(get % "product") results) max-count)))

(defn sample-product-types
  "Sample product type URIs from the store.
   Finds types that products belong to."
  [conn max-count]
  (let [results (execute-select conn
                                (str "SELECT DISTINCT ?type WHERE { "
                                     "?product a ?type . "
                                     "?product <" bsbm-vocab "productPropertyNumeric1> ?v "
                                     "} LIMIT " (* max-count 2)))]
    (sample-n (map #(get % "type") results) max-count)))

(defn sample-product-features
  "Sample product feature URIs from the store."
  [conn max-count]
  (let [results (execute-select conn
                                (str "SELECT DISTINCT ?feature WHERE { "
                                     "?product <" bsbm-vocab "productFeature> ?feature "
                                     "} LIMIT " (* max-count 2)))]
    (sample-n (map #(get % "feature") results) max-count)))

(defn sample-offers
  "Sample offer URIs from the store."
  [conn max-count]
  (let [results (execute-select conn
                                (str "SELECT DISTINCT ?offer WHERE { "
                                     "?offer <" bsbm-vocab "product> ?product "
                                     "} LIMIT " (* max-count 2)))]
    (sample-n (map #(get % "offer") results) max-count)))

(defn sample-numeric-thresholds
  "Sample numeric property values for FILTER thresholds.
   Returns percentile values that can be used for > and < filters."
  [conn property-name max-count]
  (let [property-uri (str bsbm-vocab property-name)
        results (execute-select conn
                                (str "SELECT ?value WHERE { "
                                     "?product <" property-uri "> ?value "
                                     "} LIMIT 1000"))]
    (when (seq results)
      (let [values (->> results
                        (map #(get % "value"))
                        (map parse-rdf-numeric)
                        (filter some?)
                        sort
                        vec)]
        (when (seq values)
          ;; Return percentile thresholds: 10%, 25%, 50%, 75%, 90%
          (let [n (count values)]
            {:p10 (nth values (int (* 0.10 n)))
             :p25 (nth values (int (* 0.25 n)))
             :p50 (nth values (int (* 0.50 n)))
             :p75 (nth values (int (* 0.75 n)))
             :p90 (nth values (int (* 0.90 n)))
             :min (first values)
             :max (last values)
             :samples (sample-n values max-count)}))))))

;;; ---------------------------------------------------------------------------
;;; Q3 Valid Parameter Discovery
;;; ---------------------------------------------------------------------------

(defn sample-q3-valid-params
  "Find products that have two different features and numeric properties.
   Returns vector of {:product :type :feature1 :feature2 :p1 :p3} maps.

   Q3 requires: product with type, two features, and numeric1/numeric3 values.
   This pre-validates that combinations will actually return results."
  [conn max-count]
  (let [results (execute-select conn
                                (str "SELECT DISTINCT ?product ?type ?f1 ?f2 ?p1 ?p3 WHERE { "
                                     "?product a ?type . "
                                     "?product <" bsbm-vocab "productFeature> ?f1 . "
                                     "?product <" bsbm-vocab "productFeature> ?f2 . "
                                     "?product <" bsbm-vocab "productPropertyNumeric1> ?p1 . "
                                     "?product <" bsbm-vocab "productPropertyNumeric3> ?p3 . "
                                     "FILTER (?f1 < ?f2) "  ;; Ensure f1 != f2 and avoid duplicates
                                     "} LIMIT " (* max-count 3)))]
    (let [valid-params (->> results
                            (map (fn [r]
                                   {:product (get r "product")
                                    :type (get r "type")
                                    :feature1 (get r "f1")
                                    :feature2 (get r "f2")
                                    :p1 (parse-rdf-numeric (get r "p1"))
                                    :p3 (parse-rdf-numeric (get r "p3"))}))
                            (filter #(and (:p1 %) (:p3 %))))]
      (sample-n valid-params max-count))))

;;; ---------------------------------------------------------------------------
;;; Parameter Pool
;;; ---------------------------------------------------------------------------

(defn build-param-pool
  "Build a pool of parameter values by querying the store.

   Returns:
   {:products [uri...]
    :product-types [uri...]
    :features [uri...]
    :offers [uri...]
    :numeric-1 {:p10 ... :p90 ... :samples [...]}
    :numeric-3 {:p10 ... :p90 ... :samples [...]}
    :q3-valid-params [{:product :type :feature1 :feature2 :p1 :p3}...]
    :current-date \"...\"}"
  [conn & {:keys [products product-types features offers]
           :or {products 100
                product-types 20
                features 50
                offers 100}}]
  {:products (sample-products conn products)
   :product-types (sample-product-types conn product-types)
   :features (sample-product-features conn features)
   :offers (sample-offers conn offers)
   :numeric-1 (sample-numeric-thresholds conn "productPropertyNumeric1" 20)
   :numeric-3 (sample-numeric-thresholds conn "productPropertyNumeric3" 20)
   ;; Pre-validated Q3 parameters for guaranteed non-empty results
   :q3-valid-params (sample-q3-valid-params conn 50)
   ;; Current date for Q7 - use a future date to find valid offers
   :current-date "\"2008-06-20\"^^<http://www.w3.org/2001/XMLSchema#date>"})

(defn pool-summary
  "Return a summary of the parameter pool."
  [pool]
  {:products (count (:products pool))
   :product-types (count (:product-types pool))
   :features (count (:features pool))
   :offers (count (:offers pool))
   :has-numeric-1 (some? (:numeric-1 pool))
   :has-numeric-3 (some? (:numeric-3 pool))
   :q3-valid-params (count (:q3-valid-params pool))})

;;; ---------------------------------------------------------------------------
;;; Parameter Selection
;;; ---------------------------------------------------------------------------

(defn select-param
  "Select a random parameter from the pool.
   key is :products, :product-types, :features, or :offers"
  [pool key]
  (when-let [values (get pool key)]
    (when (seq values)
      (rand-nth values))))

(defn select-numeric
  "Select a numeric threshold from the pool.
   percentile is :p10, :p25, :p50, :p75, :p90, or :samples"
  [pool property-key percentile]
  (get-in pool [property-key percentile]))

(defn select-numeric-sample
  "Select a random numeric sample value."
  [pool property-key]
  (when-let [samples (get-in pool [property-key :samples])]
    (when (seq samples)
      (rand-nth samples))))

;;; ---------------------------------------------------------------------------
;;; Parameter Substitution
;;; ---------------------------------------------------------------------------

(defn- format-uri
  "Format a URI for SPARQL - ensure it has angle brackets."
  [uri]
  (if (str/starts-with? uri "<")
    uri
    (str "<" uri ">")))

(defn- format-literal
  "Format a literal value for SPARQL."
  [value]
  (if (or (str/starts-with? value "\"")
          (str/starts-with? value "'"))
    value
    (str "\"" value "\"")))

(defn substitute-params
  "Substitute parameter placeholders in a query template.

   Placeholders:
   - %ProductXYZ% -> product URI
   - %ProductType% -> product type URI
   - %ProductFeature1%, %ProductFeature2% -> feature URIs
   - %x%, %y% -> numeric thresholds
   - %OfferXYZ% -> offer URI
   - %currentDate% -> date literal

   Returns the substituted query string."
  [query-template param-pool]
  (let [product (select-param param-pool :products)
        product-type (select-param param-pool :product-types)
        features (take 2 (shuffle (:features param-pool)))
        feature1 (first features)
        feature2 (or (second features) feature1)
        offer (select-param param-pool :offers)
        ;; Use percentiles for numeric filters
        x (or (select-numeric param-pool :numeric-1 :p25)
              (select-numeric-sample param-pool :numeric-1)
              10)
        y (or (select-numeric param-pool :numeric-3 :p75)
              (select-numeric-sample param-pool :numeric-3)
              500)
        current-date (:current-date param-pool)]
    (-> query-template
        (str/replace "%ProductXYZ%" (if product (format-uri product) "<http://example.org/product1>"))
        (str/replace "%ProductType%" (if product-type (format-uri product-type) "<http://example.org/ProductType1>"))
        (str/replace "%ProductFeature1%" (if feature1 (format-uri feature1) "<http://example.org/Feature1>"))
        (str/replace "%ProductFeature2%" (if feature2 (format-uri feature2) "<http://example.org/Feature2>"))
        (str/replace "%x%" (str (long x)))
        (str/replace "%y%" (str (long y)))
        (str/replace "%OfferXYZ%" (if offer (format-uri offer) "<http://example.org/offer1>"))
        (str/replace "%currentDate%" (or current-date "\"2008-06-20\"^^<http://www.w3.org/2001/XMLSchema#date>")))))

(defn substitute-params-q3
  "Substitute parameters for Q3 using pre-validated combinations.

   Q3 requires products with TWO features and specific numeric constraints.
   This uses pre-validated parameters to guarantee non-empty results."
  [query-template param-pool]
  (if-let [q3-params (seq (:q3-valid-params param-pool))]
    ;; Use pre-validated Q3 parameters
    (let [{:keys [type feature1 feature2 p1 p3]} (rand-nth q3-params)
          ;; Set thresholds to ensure the product matches:
          ;; p1 > x means x should be less than p1
          ;; p3 < y means y should be greater than p3
          x (max 0 (- p1 10))   ;; x < p1, so product passes FILTER(?p1 > x)
          y (+ p3 10)]          ;; y > p3, so product passes FILTER(?p3 < y)
      (-> query-template
          (str/replace "%ProductType%" (format-uri type))
          (str/replace "%ProductFeature1%" (format-uri feature1))
          (str/replace "%ProductFeature2%" (format-uri feature2))
          (str/replace "%x%" (str (long x)))
          (str/replace "%y%" (str (long y)))))
    ;; Fallback to generic substitution if no Q3 params available
    (substitute-params query-template param-pool)))

(defn instantiate-query
  "Instantiate a query template with fresh parameters.

   template-def is a map with :template key containing the SPARQL string.
   For Q3, uses pre-validated parameters to ensure non-empty results.
   Returns the substituted query string."
  [template-def param-pool]
  (let [query-name (:name template-def)]
    (if (= query-name "Q3")
      (substitute-params-q3 (:template template-def) param-pool)
      (substitute-params (:template template-def) param-pool))))

;;; ---------------------------------------------------------------------------
;;; Batch Generation
;;; ---------------------------------------------------------------------------

(defn generate-query-batch
  "Generate n instantiated queries from a template."
  [template-def param-pool n]
  (repeatedly n #(instantiate-query template-def param-pool)))

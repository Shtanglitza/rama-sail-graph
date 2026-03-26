(ns rama-sail.bench.bsbm.infra.data-generator
  "Generate synthetic BSBM-compatible datasets for benchmarking."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

;;; ---------------------------------------------------------------------------
;;; BSBM Namespaces
;;; ---------------------------------------------------------------------------

(def bsbm "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/")
(def bsbm-inst "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/")
(def rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
(def rdfs "http://www.w3.org/2000/01/rdf-schema#")
(def dc "http://purl.org/dc/elements/1.1/")
(def rev "http://purl.org/stuff/rev#")
(def foaf "http://xmlns.com/foaf/0.1/")
(def xsd "http://www.w3.org/2001/XMLSchema#")

;;; ---------------------------------------------------------------------------
;;; N-Triples Formatting
;;; ---------------------------------------------------------------------------

(defn iri [uri] (str "<" uri ">"))
(defn lit [s] (str "\"" s "\""))
(defn lit-lang [s lang] (str "\"" s "\"@" lang))
(defn lit-type [s type] (str "\"" s "\"^^<" xsd type ">"))
(defn lit-int [n] (lit-type n "integer"))
(defn lit-double [n] (lit-type (format "%.2f" (double n)) "double"))
(defn lit-date [s] (lit-type s "date"))

(defn triple [s p o]
  (str s " " p " " o " ."))

;;; ---------------------------------------------------------------------------
;;; Entity URIs
;;; ---------------------------------------------------------------------------

(defn product-uri [n] (iri (str bsbm-inst "Product" n)))
(defn product-type-uri [n] (iri (str bsbm "ProductType" n)))
(defn feature-uri [n] (iri (str bsbm-inst "ProductFeature" n)))
(defn producer-uri [n] (iri (str bsbm-inst "Producer" n)))
(defn vendor-uri [n] (iri (str bsbm-inst "Vendor" n)))
(defn offer-uri [n] (iri (str bsbm-inst "Offer" n)))
(defn review-uri [n] (iri (str bsbm-inst "Review" n)))
(defn reviewer-uri [n] (iri (str bsbm-inst "Reviewer" n)))
(defn country-uri [code] (iri (str "http://downlode.org/rdf/iso-3166/countries#" code)))

;;; ---------------------------------------------------------------------------
;;; Predicate URIs
;;; ---------------------------------------------------------------------------

(def rdf-type (iri (str rdf "type")))
(def rdfs-label (iri (str rdfs "label")))
(def rdfs-comment (iri (str rdfs "comment")))

(def bsbm-producer (iri (str bsbm "producer")))
(def bsbm-product-feature (iri (str bsbm "productFeature")))
(def bsbm-product-prop-text1 (iri (str bsbm "productPropertyTextual1")))
(def bsbm-product-prop-text2 (iri (str bsbm "productPropertyTextual2")))
(def bsbm-product-prop-text3 (iri (str bsbm "productPropertyTextual3")))
(def bsbm-product-prop-text4 (iri (str bsbm "productPropertyTextual4")))
(def bsbm-product-prop-text5 (iri (str bsbm "productPropertyTextual5")))
(def bsbm-product-prop-num1 (iri (str bsbm "productPropertyNumeric1")))
(def bsbm-product-prop-num2 (iri (str bsbm "productPropertyNumeric2")))
(def bsbm-product-prop-num3 (iri (str bsbm "productPropertyNumeric3")))
(def bsbm-product-prop-num4 (iri (str bsbm "productPropertyNumeric4")))

(def bsbm-product (iri (str bsbm "product")))
(def bsbm-vendor (iri (str bsbm "vendor")))
(def bsbm-price (iri (str bsbm "price")))
(def bsbm-valid-to (iri (str bsbm "validTo")))
(def bsbm-country (iri (str bsbm "country")))

(def bsbm-review-for (iri (str bsbm "reviewFor")))
(def bsbm-rating1 (iri (str bsbm "rating1")))
(def bsbm-rating2 (iri (str bsbm "rating2")))
(def rev-reviewer (iri (str rev "reviewer")))
(def dc-title (iri (str dc "title")))
(def foaf-name (iri (str foaf "name")))

;;; ---------------------------------------------------------------------------
;;; Data Generation
;;; ---------------------------------------------------------------------------

(defn generate-product-types
  "Generate product type triples."
  [num-types]
  (for [i (range 1 (inc num-types))]
    [(triple (product-type-uri i) rdf-type (iri (str bsbm "ProductType")))
     (triple (product-type-uri i) rdfs-label (lit (str "Product Type " i)))]))

(defn generate-features
  "Generate product feature triples."
  [num-features]
  (for [i (range 1 (inc num-features))]
    [(triple (feature-uri i) rdf-type (iri (str bsbm "ProductFeature")))
     (triple (feature-uri i) rdfs-label (lit (str "Feature " i)))]))

(defn generate-producers
  "Generate producer triples."
  [num-producers]
  (for [i (range 1 (inc num-producers))]
    [(triple (producer-uri i) rdf-type (iri (str bsbm "Producer")))
     (triple (producer-uri i) rdfs-label (lit (str "Producer " i)))
     (triple (producer-uri i) bsbm-country (country-uri "US"))]))

(defn generate-vendors
  "Generate vendor triples."
  [num-vendors]
  (let [countries ["US" "GB" "DE" "FR" "JP"]]
    (for [i (range 1 (inc num-vendors))]
      [(triple (vendor-uri i) rdf-type (iri (str bsbm "Vendor")))
       (triple (vendor-uri i) rdfs-label (lit (str "Vendor " i)))
       (triple (vendor-uri i) bsbm-country (country-uri (nth countries (mod i (count countries)))))])))

(defn generate-reviewers
  "Generate reviewer triples."
  [num-reviewers]
  (let [first-names ["John" "Jane" "Bob" "Alice" "Charlie" "Diana" "Eve" "Frank"]
        last-names ["Smith" "Johnson" "Williams" "Brown" "Jones" "Davis" "Miller" "Wilson"]]
    (for [i (range 1 (inc num-reviewers))]
      [(triple (reviewer-uri i) rdf-type (iri (str bsbm "Reviewer")))
       (triple (reviewer-uri i) foaf-name
               (lit (str (nth first-names (mod i (count first-names))) " "
                         (nth last-names (mod (quot i 8) (count last-names))))))])))

(defn generate-product
  "Generate triples for a single product."
  [product-id num-types num-features num-producers]
  (let [type-id (inc (mod product-id num-types))
        producer-id (inc (mod product-id num-producers))
        ;; Each product gets 2-4 features
        num-product-features (+ 2 (mod product-id 3))
        feature-ids (take num-product-features
                          (map #(inc (mod % num-features))
                               (iterate #(+ % 7) product-id)))
        ;; Numeric properties with good distribution for filtering
        num1 (+ 10 (* 10 (mod product-id 100)))   ;; 10-1000
        num2 (+ 50 (* 5 (mod (* product-id 3) 200))) ;; 50-1050
        num3 (+ 100 (* 20 (mod (* product-id 7) 50))) ;; 100-1100
        ;; Some products have optional properties
        has-text4 (zero? (mod product-id 3))
        has-text5 (zero? (mod product-id 5))
        has-num4 (zero? (mod product-id 4))]
    (concat
     [(triple (product-uri product-id) rdf-type (product-type-uri type-id))
      (triple (product-uri product-id) rdfs-label (lit (str "Product " product-id)))
      (triple (product-uri product-id) rdfs-comment (lit (str "This is the description of product " product-id ". It has many great features.")))
      (triple (product-uri product-id) bsbm-producer (producer-uri producer-id))
      (triple (product-uri product-id) bsbm-product-prop-text1 (lit (str "Text1-Product" product-id)))
      (triple (product-uri product-id) bsbm-product-prop-text2 (lit (str "Text2-Product" product-id)))
      (triple (product-uri product-id) bsbm-product-prop-text3 (lit (str "Text3-Product" product-id)))
      (triple (product-uri product-id) bsbm-product-prop-num1 (lit-int num1))
      (triple (product-uri product-id) bsbm-product-prop-num2 (lit-int num2))
      (triple (product-uri product-id) bsbm-product-prop-num3 (lit-int num3))]
     ;; Features
     (for [fid feature-ids]
       (triple (product-uri product-id) bsbm-product-feature (feature-uri fid)))
     ;; Optional properties
     (when has-text4
       [(triple (product-uri product-id) bsbm-product-prop-text4 (lit (str "OptText4-" product-id)))])
     (when has-text5
       [(triple (product-uri product-id) bsbm-product-prop-text5 (lit (str "OptText5-" product-id)))])
     (when has-num4
       [(triple (product-uri product-id) bsbm-product-prop-num4 (lit-int (* product-id 11)))]))))

(defn generate-offer
  "Generate triples for a single offer."
  [offer-id num-products num-vendors]
  (let [product-id (inc (mod offer-id num-products))
        vendor-id (inc (mod (quot offer-id 2) num-vendors))
        price (+ 10.0 (* 0.99 (mod (* offer-id 17) 500)))
        ;; Offers valid until various dates in 2025
        month (inc (mod offer-id 12))
        day (inc (mod (* offer-id 3) 28))]
    [(triple (offer-uri offer-id) rdf-type (iri (str bsbm "Offer")))
     (triple (offer-uri offer-id) bsbm-product (product-uri product-id))
     (triple (offer-uri offer-id) bsbm-vendor (vendor-uri vendor-id))
     (triple (offer-uri offer-id) bsbm-price (lit-double price))
     (triple (offer-uri offer-id) bsbm-valid-to (lit-date (format "2025-%02d-%02d" month day)))]))

(defn generate-review
  "Generate triples for a single review."
  [review-id num-products num-reviewers]
  (let [product-id (inc (mod review-id num-products))
        reviewer-id (inc (mod (quot review-id 3) num-reviewers))
        rating1 (inc (mod review-id 5))
        rating2 (inc (mod (* review-id 3) 5))
        titles ["Great product!" "Good value" "Decent quality" "Could be better" "Excellent!"
                "Highly recommend" "Not bad" "Amazing!" "Just okay" "Perfect for me"]]
    [(triple (review-uri review-id) rdf-type (iri (str bsbm "Review")))
     (triple (review-uri review-id) bsbm-review-for (product-uri product-id))
     (triple (review-uri review-id) rev-reviewer (reviewer-uri reviewer-id))
     (triple (review-uri review-id) dc-title (lit (nth titles (mod review-id (count titles)))))
     (triple (review-uri review-id) bsbm-rating1 (lit-int rating1))
     (triple (review-uri review-id) bsbm-rating2 (lit-int rating2))]))

(defn generate-dataset
  "Generate a complete BSBM dataset.

   Options:
   - :num-products - Number of products (default 100)
   - :num-types - Number of product types (default 10)
   - :num-features - Number of features (default 25)
   - :num-producers - Number of producers (default 10)
   - :num-vendors - Number of vendors (default 5)
   - :num-reviewers - Number of reviewers (default 20)
   - :offers-per-product - Average offers per product (default 3)
   - :reviews-per-product - Average reviews per product (default 5)"
  [& {:keys [num-products num-types num-features num-producers
             num-vendors num-reviewers offers-per-product reviews-per-product]
      :or {num-products 100
           num-types 10
           num-features 25
           num-producers 10
           num-vendors 5
           num-reviewers 20
           offers-per-product 3
           reviews-per-product 5}}]
  (let [num-offers (* num-products offers-per-product)
        num-reviews (* num-products reviews-per-product)]
    (flatten
     (concat
      ;; Header comment
      [["# BSBM Synthetic Dataset"
        (str "# Products: " num-products)
        (str "# Types: " num-types)
        (str "# Features: " num-features)
        (str "# Producers: " num-producers)
        (str "# Vendors: " num-vendors)
        (str "# Offers: " num-offers)
        (str "# Reviews: " num-reviews)
        "#"]]
      ;; Types
      (generate-product-types num-types)
      ;; Features
      (generate-features num-features)
      ;; Producers
      (generate-producers num-producers)
      ;; Vendors
      (generate-vendors num-vendors)
      ;; Reviewers
      (generate-reviewers num-reviewers)
      ;; Products
      (for [i (range 1 (inc num-products))]
        (generate-product i num-types num-features num-producers))
      ;; Offers
      (for [i (range 1 (inc num-offers))]
        (generate-offer i num-products num-vendors))
      ;; Reviews
      (for [i (range 1 (inc num-reviews))]
        (generate-review i num-products num-reviewers))))))

(defn write-dataset
  "Write dataset to a file."
  [file-path & opts]
  (let [triples (apply generate-dataset opts)]
    (with-open [w (io/writer file-path)]
      (doseq [line triples]
        (.write w (str line "\n"))))
    (count (filter #(str/ends-with? % " .") triples))))

(defn generate-standard-datasets!
  "Generate standard BSBM datasets of various sizes."
  []
  (println "Generating BSBM datasets...")

  ;; 100 products (~2K triples)
  (let [path "test/resources/bsbm/dataset_100.nt"
        n (write-dataset path :num-products 100)]
    (println (str "  " path ": " n " triples")))

  ;; 500 products (~10K triples)
  (let [path "test/resources/bsbm/dataset_500.nt"
        n (write-dataset path :num-products 500
                         :num-types 20
                         :num-features 50
                         :num-producers 20
                         :num-vendors 10
                         :num-reviewers 50)]
    (println (str "  " path ": " n " triples")))

  ;; 1000 products (~20K triples)
  (let [path "test/resources/bsbm/dataset_1000.nt"
        n (write-dataset path :num-products 1000
                         :num-types 30
                         :num-features 75
                         :num-producers 30
                         :num-vendors 15
                         :num-reviewers 100)]
    (println (str "  " path ": " n " triples")))

  (println "Done!"))

(comment
  ;; Generate all standard datasets
  (generate-standard-datasets!)

  ;; Generate custom dataset
  (write-dataset "test/resources/bsbm/dataset_custom.nt"
                 :num-products 200
                 :num-types 15)

  ;; Preview generated data
  (take 20 (generate-dataset :num-products 5)))

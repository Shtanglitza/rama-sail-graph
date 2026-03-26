(ns rama-sail.bench.bsbm.infra.query-templates
  "BSBM (Berlin SPARQL Benchmark) query templates.

   These queries test different SPARQL operators:
   - Q1:  BGP, FILTER, DISTINCT, ORDER BY, LIMIT
   - Q2:  BGP, OPTIONAL
   - Q3:  BGP, OPTIONAL, FILTER, ORDER BY, LIMIT
   - Q4:  BGP, UNION, FILTER, DISTINCT, ORDER BY, LIMIT
   - Q5:  BGP, complex FILTER (numeric ranges), DISTINCT, ORDER BY, LIMIT
   - Q7:  BGP, nested OPTIONAL, FILTER
   - Q8:  BGP, OPTIONAL, ORDER BY DESC, LIMIT
   - Q10: BGP, FILTER (multiple conditions), DISTINCT, ORDER BY, LIMIT
   - Q11: BGP, UNION

   Placeholders use %Name% format for parameter substitution.")

;;; ---------------------------------------------------------------------------
;;; BSBM Namespace Prefixes
;;; ---------------------------------------------------------------------------

(def bsbm-prefixes
  "Common BSBM prefixes for SPARQL queries."
  {"bsbm"      "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/"
   "bsbm-inst" "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/"
   "rev"       "http://purl.org/stuff/rev#"
   "dc"        "http://purl.org/dc/elements/1.1/"
   "foaf"      "http://xmlns.com/foaf/0.1/"
   "rdfs"      "http://www.w3.org/2000/01/rdf-schema#"
   "rdf"       "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "xsd"       "http://www.w3.org/2001/XMLSchema#"})

(defn prefix-declarations
  "Generate SPARQL PREFIX declarations string."
  []
  (apply str
         (for [[prefix uri] bsbm-prefixes]
           (str "PREFIX " prefix ": <" uri ">\n"))))

;;; ---------------------------------------------------------------------------
;;; Query Templates
;;; ---------------------------------------------------------------------------

(def q1-template
  "Q1: Find products for given product type and features.

   Tests: BGP, FILTER, DISTINCT, ORDER BY, LIMIT
   Parameters: %ProductType%, %ProductFeature1%, %ProductFeature2%, %x%"
  {:name "Q1"
   :description "Find products for given type and features"
   :operators #{:bgp :filter :distinct :order-by :limit}
   :params [:product-type :feature1 :feature2 :numeric-x]
   :template
   (str (prefix-declarations)
        "SELECT DISTINCT ?product ?label
WHERE {
  ?product rdfs:label ?label .
  ?product rdf:type %ProductType% .
  ?product bsbm:productFeature %ProductFeature1% .
  ?product bsbm:productFeature %ProductFeature2% .
  ?product bsbm:productPropertyNumeric1 ?value1 .
  FILTER (?value1 > %x%)
}
ORDER BY ?label
LIMIT 10")})

(def q2-template
  "Q2: Retrieve basic information about a specific product.

   Tests: BGP, OPTIONAL
   Parameters: %ProductXYZ%"
  {:name "Q2"
   :description "Retrieve basic information about a specific product"
   :operators #{:bgp :optional}
   :params [:product]
   :template
   (str (prefix-declarations)
        "SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1
        ?propertyTextual2 ?propertyTextual3 ?propertyNumeric1 ?propertyNumeric2
        ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4
WHERE {
  %ProductXYZ% rdfs:label ?label .
  %ProductXYZ% rdfs:comment ?comment .
  %ProductXYZ% bsbm:producer ?producer .
  %ProductXYZ% bsbm:productFeature ?productFeature .
  %ProductXYZ% bsbm:productPropertyTextual1 ?propertyTextual1 .
  %ProductXYZ% bsbm:productPropertyTextual2 ?propertyTextual2 .
  %ProductXYZ% bsbm:productPropertyTextual3 ?propertyTextual3 .
  %ProductXYZ% bsbm:productPropertyNumeric1 ?propertyNumeric1 .
  %ProductXYZ% bsbm:productPropertyNumeric2 ?propertyNumeric2 .
  OPTIONAL { %ProductXYZ% bsbm:productPropertyTextual4 ?propertyTextual4 }
  OPTIONAL { %ProductXYZ% bsbm:productPropertyTextual5 ?propertyTextual5 }
  OPTIONAL { %ProductXYZ% bsbm:productPropertyNumeric4 ?propertyNumeric4 }
}")})

(def q3-template
  "Q3: Find products with features and numeric constraints.

   Tests: BGP, OPTIONAL, FILTER, ORDER BY, LIMIT
   Parameters: %ProductType%, %ProductFeature1%, %ProductFeature2%, %x%, %y%"
  {:name "Q3"
   :description "Find products with features and numeric constraints"
   :operators #{:bgp :optional :filter :order-by :limit}
   :params [:product-type :feature1 :feature2 :numeric-x :numeric-y]
   :template
   (str (prefix-declarations)
        "SELECT ?product ?label
WHERE {
  ?product rdfs:label ?label .
  ?product rdf:type %ProductType% .
  ?product bsbm:productFeature %ProductFeature1% .
  ?product bsbm:productFeature %ProductFeature2% .
  ?product bsbm:productPropertyNumeric1 ?p1 .
  FILTER ( ?p1 > %x% )
  ?product bsbm:productPropertyNumeric3 ?p3 .
  FILTER ( ?p3 < %y% )
  OPTIONAL {
    ?product bsbm:productPropertyTextual1 ?propertyTextual1
  }
}
ORDER BY ?label
LIMIT 10")})

(def q4-template
  "Q4: Find products matching two different feature sets (UNION).

   Tests: BGP, UNION, FILTER, DISTINCT, ORDER BY, LIMIT
   Parameters: %ProductType%, %ProductFeature1%, %ProductFeature2%, %x%, %y%"
  {:name "Q4"
   :description "Find products matching two different feature sets"
   :operators #{:bgp :union :filter :distinct :order-by :limit}
   :params [:product-type :feature1 :feature2 :numeric-x :numeric-y]
   :template
   (str (prefix-declarations)
        "SELECT DISTINCT ?product ?label
WHERE {
  {
    ?product rdfs:label ?label .
    ?product rdf:type %ProductType% .
    ?product bsbm:productFeature %ProductFeature1% .
    ?product bsbm:productPropertyNumeric1 ?p1 .
    FILTER ( ?p1 > %x% )
  } UNION {
    ?product rdfs:label ?label .
    ?product rdf:type %ProductType% .
    ?product bsbm:productFeature %ProductFeature2% .
    ?product bsbm:productPropertyNumeric2 ?p2 .
    FILTER ( ?p2 > %y% )
  }
}
ORDER BY ?label
LIMIT 10")})

(def q5-template
  "Q5: Find products similar to a given product.

   Tests: BGP, FILTER (complex numeric range + inequality), DISTINCT, ORDER BY, LIMIT
   Parameters: %ProductXYZ%"
  {:name "Q5"
   :description "Find products similar to a given product"
   :operators #{:bgp :filter :distinct :order-by :limit}
   :params [:product]
   :template
   (str (prefix-declarations)
        "SELECT DISTINCT ?product ?productLabel
WHERE {
  ?product rdfs:label ?productLabel .
  FILTER (%ProductXYZ% != ?product)
  %ProductXYZ% bsbm:productFeature ?prodFeature .
  ?product bsbm:productFeature ?prodFeature .
  %ProductXYZ% bsbm:productPropertyNumeric1 ?origProperty1 .
  ?product bsbm:productPropertyNumeric1 ?simProperty1 .
  FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))
  %ProductXYZ% bsbm:productPropertyNumeric2 ?origProperty2 .
  ?product bsbm:productPropertyNumeric2 ?simProperty2 .
  FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170))
}
ORDER BY ?productLabel
LIMIT 5")})

(def q7-template
  "Q7: Product reviews with reviewer details.

   Tests: BGP, nested OPTIONAL, FILTER
   Parameters: %ProductXYZ%, %currentDate%"
  {:name "Q7"
   :description "Product reviews with reviewer details"
   :operators #{:bgp :optional :filter}
   :params [:product :current-date]
   :template
   (str (prefix-declarations)
        "SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review
        ?revTitle ?reviewer ?revName ?rating1 ?rating2
WHERE {
  %ProductXYZ% rdfs:label ?productLabel .
  OPTIONAL {
    ?offer bsbm:product %ProductXYZ% .
    ?offer bsbm:price ?price .
    ?offer bsbm:vendor ?vendor .
    ?vendor rdfs:label ?vendorTitle .
    ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
    ?offer bsbm:validTo ?date .
    FILTER ( ?date > %currentDate% )
  }
  OPTIONAL {
    ?review bsbm:reviewFor %ProductXYZ% .
    ?review rev:reviewer ?reviewer .
    ?reviewer foaf:name ?revName .
    ?review dc:title ?revTitle .
    OPTIONAL { ?review bsbm:rating1 ?rating1 }
    OPTIONAL { ?review bsbm:rating2 ?rating2 }
  }
}")})

(def q8-template
  "Q8: Get recent reviews for a product.

   Tests: BGP, OPTIONAL, ORDER BY DESC, LIMIT
   Parameters: %ProductXYZ%
   Note: Original BSBM Q8 uses langMatches which is not yet supported.
   This simplified version omits the language filter."
  {:name "Q8"
   :description "Get recent reviews for a product"
   :operators #{:bgp :optional :order-by :limit}
   :params [:product]
   :template
   (str (prefix-declarations)
        "SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 ?rating2 ?rating3 ?rating4
WHERE {
  ?review bsbm:reviewFor %ProductXYZ% .
  ?review dc:title ?title .
  ?review rev:text ?text .
  ?review bsbm:reviewDate ?reviewDate .
  ?review rev:reviewer ?reviewer .
  ?reviewer foaf:name ?reviewerName .
  OPTIONAL { ?review bsbm:rating1 ?rating1 }
  OPTIONAL { ?review bsbm:rating2 ?rating2 }
  OPTIONAL { ?review bsbm:rating3 ?rating3 }
  OPTIONAL { ?review bsbm:rating4 ?rating4 }
}
ORDER BY DESC(?reviewDate)
LIMIT 20")})

(def q10-template
  "Q10: Get cheap offers with fast delivery from US vendors.

   Tests: BGP, FILTER (multiple conditions), DISTINCT, ORDER BY, LIMIT
   Parameters: %ProductXYZ%, %currentDate%"
  {:name "Q10"
   :description "Get cheap offers with fast delivery"
   :operators #{:bgp :filter :distinct :order-by :limit}
   :params [:product :current-date]
   :template
   (str (prefix-declarations)
        "SELECT DISTINCT ?offer ?price
WHERE {
  ?offer bsbm:product %ProductXYZ% .
  ?offer bsbm:vendor ?vendor .
  ?offer dc:publisher ?vendor .
  ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
  ?offer bsbm:deliveryDays ?deliveryDays .
  FILTER (?deliveryDays <= 3)
  ?offer bsbm:price ?price .
  ?offer bsbm:validTo ?date .
  FILTER (?date > %currentDate%)
}
ORDER BY ?price
LIMIT 10")})

(def q11-template
  "Q11: Get offer details with union of two retrieval patterns.

   Tests: BGP, UNION
   Parameters: %OfferXYZ%"
  {:name "Q11"
   :description "Get offer details with union of retrieval patterns"
   :operators #{:bgp :union}
   :params [:offer]
   :template
   (str (prefix-declarations)
        "SELECT ?property ?hasValue ?isValueOf
WHERE {
  {
    %OfferXYZ% ?property ?hasValue
  } UNION {
    ?isValueOf ?property %OfferXYZ%
  }
}")})

;;; ---------------------------------------------------------------------------
;;; Additional Join-Focused Queries
;;; ---------------------------------------------------------------------------

(def qj1-template
  "QJ1: 3-way join - Products with their producers and vendors offering them.

   Tests: BGP, JOIN (3-way)
   No bound constants - pure join performance test."
  {:name "QJ1"
   :description "3-way join: products → producers → offers → vendors"
   :operators #{:bgp :join}
   :params []
   :template
   (str (prefix-declarations)
        "SELECT ?product ?productLabel ?producer ?producerLabel ?offer ?vendor ?vendorLabel ?price
WHERE {
  ?product rdfs:label ?productLabel .
  ?product bsbm:producer ?producer .
  ?producer rdfs:label ?producerLabel .
  ?offer bsbm:product ?product .
  ?offer bsbm:vendor ?vendor .
  ?offer bsbm:price ?price .
  ?vendor rdfs:label ?vendorLabel .
}
LIMIT 100")})

(def qj2-template
  "QJ2: Star join - All properties of products with a specific feature.

   Tests: BGP, JOIN (star pattern)
   Parameters: %ProductFeature1%"
  {:name "QJ2"
   :description "Star join: products with feature and all their properties"
   :operators #{:bgp :join :filter}
   :params [:feature1]
   :template
   (str (prefix-declarations)
        "SELECT ?product ?label ?producer ?num1 ?num2 ?num3
WHERE {
  ?product bsbm:productFeature %ProductFeature1% .
  ?product rdfs:label ?label .
  ?product bsbm:producer ?producer .
  ?product bsbm:productPropertyNumeric1 ?num1 .
  ?product bsbm:productPropertyNumeric2 ?num2 .
  ?product bsbm:productPropertyNumeric3 ?num3 .
}
ORDER BY ?num1
LIMIT 50")})

(def qj3-template
  "QJ3: Chain join - Reviews with reviewer details and product info.

   Tests: BGP, JOIN (chain pattern)
   No bound constants."
  {:name "QJ3"
   :description "Chain join: reviews → reviewers → products → producers"
   :operators #{:bgp :join}
   :params []
   :template
   (str (prefix-declarations)
        "SELECT ?review ?title ?reviewerName ?product ?productLabel ?producer
WHERE {
  ?review dc:title ?title .
  ?review rev:reviewer ?reviewer .
  ?reviewer foaf:name ?reviewerName .
  ?review bsbm:reviewFor ?product .
  ?product rdfs:label ?productLabel .
  ?product bsbm:producer ?producer .
}
LIMIT 100")})

(def qj4-template
  "QJ4: Self-join - Products sharing the same producer.

   Tests: BGP, JOIN (self-join pattern)"
  {:name "QJ4"
   :description "Self-join: pairs of products from same producer"
   :operators #{:bgp :join :filter}
   :params []
   :template
   (str (prefix-declarations)
        "SELECT ?product1 ?label1 ?product2 ?label2 ?producer
WHERE {
  ?product1 bsbm:producer ?producer .
  ?product2 bsbm:producer ?producer .
  ?product1 rdfs:label ?label1 .
  ?product2 rdfs:label ?label2 .
  FILTER (?product1 < ?product2)
}
LIMIT 100")})

;;; ---------------------------------------------------------------------------
;;; Template Registry
;;; ---------------------------------------------------------------------------

(def all-templates
  "Map of template name to template definition."
  {:q1 q1-template
   :q2 q2-template
   :q3 q3-template
   :q4 q4-template
   :q5 q5-template
   :q7 q7-template
   :q8 q8-template
   :q10 q10-template
   :q11 q11-template
   :qj1 qj1-template
   :qj2 qj2-template
   :qj3 qj3-template
   :qj4 qj4-template})

(def benchmark-templates
  "Ordered list of templates for benchmarking.
   Includes original BSBM queries (Q1-Q11, excluding Q6/Q9/Q12)."
  [q1-template q2-template q3-template q4-template q5-template
   q7-template q8-template q10-template q11-template])

(def join-benchmark-templates
  "Join-focused query templates for testing join performance."
  [qj1-template qj2-template qj3-template qj4-template])

(def all-benchmark-templates
  "All templates including join-focused queries."
  (concat benchmark-templates join-benchmark-templates))

(defn get-template
  "Get a template by keyword name."
  [template-name]
  (get all-templates template-name))

(defn list-templates
  "List all available template names."
  []
  (keys all-templates))

;;; ---------------------------------------------------------------------------
;;; Template Info
;;; ---------------------------------------------------------------------------

(defn template-summary
  "Get a summary of a template."
  [template]
  {:name (:name template)
   :description (:description template)
   :operators (:operators template)
   :params (:params template)})

(defn print-template-info
  "Print information about all templates."
  []
  (println "BSBM Query Templates")
  (println "====================")
  (doseq [template benchmark-templates]
    (println)
    (println (str (:name template) ": " (:description template)))
    (println (str "  Operators: " (pr-str (:operators template))))
    (println (str "  Parameters: " (pr-str (:params template))))))

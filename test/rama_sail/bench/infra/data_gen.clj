(ns rama-sail.bench.infra.data-gen
  "Synthetic RDF data generators for benchmarking."
  (:require [rama-sail.core :as core]))

;;; ---------------------------------------------------------------------------
;;; URI/Literal Helpers
;;; ---------------------------------------------------------------------------

(defn entity-uri
  "Generate entity URI: <http://bench.example/entity/N>"
  [n]
  (str "<http://bench.example/entity/" n ">"))

(defn predicate-uri
  "Generate predicate URI: <http://bench.example/pred/NAME>"
  [name]
  (str "<http://bench.example/pred/" name ">"))

(defn type-uri
  "Generate type URI: <http://bench.example/type/NAME>"
  [name]
  (str "<http://bench.example/type/" name ">"))

(defn graph-uri
  "Generate named graph URI: <http://bench.example/graph/N>"
  [n]
  (str "<http://bench.example/graph/" n ">"))

(defn literal-string
  "Generate RDF literal: \"VALUE\""
  [value]
  (str "\"" value "\""))

(defn literal-int
  "Generate typed integer literal: \"N\"^^<http://www.w3.org/2001/XMLSchema#integer>"
  [n]
  (str "\"" n "\"^^<http://www.w3.org/2001/XMLSchema#integer>"))

(defn literal-double
  "Generate typed double literal."
  [n]
  (str "\"" n "\"^^<http://www.w3.org/2001/XMLSchema#double>"))

;;; ---------------------------------------------------------------------------
;;; Data Generators
;;; ---------------------------------------------------------------------------

(defn generate-linear-graph
  "Generate a linear chain graph: e0 -> e1 -> e2 -> ... -> eN-1
   Returns n-1 quads (edges) for n nodes.
   Good for testing join performance with chain traversals."
  [n]
  (let [pred (predicate-uri "next")]
    (vec (for [i (range (dec n))]
           [(entity-uri i)
            pred
            (entity-uri (inc i))
            nil]))))

(defn generate-star-graph
  "Generate a star graph: central node with n outgoing edges.
   Returns n quads.
   Good for testing subject-based lookups."
  [n]
  (let [center (entity-uri "center")
        pred (predicate-uri "pointsTo")]
    (vec (for [i (range n)]
           [center
            pred
            (entity-uri i)
            nil]))))

(defn generate-typed-entities
  "Generate n entities with 3 triples each:
   - rdf:type Person/Company/Product (cycles through 3 types)
   - name literal
   - age/value literal (numeric)
   Returns 3*n quads.
   Good for testing filters, ORDER BY, and aggregations."
  [n]
  (let [type-pred "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        name-pred (predicate-uri "name")
        age-pred (predicate-uri "age")
        types [(type-uri "Person")
               (type-uri "Company")
               (type-uri "Product")]]
    (vec (for [i (range n)
               quad [[(entity-uri i) type-pred (types (mod i 3)) nil]
                     [(entity-uri i) name-pred (literal-string (str "Entity-" i)) nil]
                     [(entity-uri i) age-pred (literal-int (+ 18 (mod (* i 7) 80))) nil]]]
           quad))))

(defn generate-multi-context
  "Generate n quads distributed across num-contexts named graphs.
   Each quad: entity -> pred -> value, in graph g(i mod num-contexts).
   Good for testing context filtering."
  [n num-contexts]
  (let [pred (predicate-uri "value")]
    (vec (for [i (range n)]
           [(entity-uri i)
            pred
            (literal-int i)
            (graph-uri (mod i num-contexts))]))))

(defn generate-dense-predicates
  "Generate n quads using k different predicates.
   Distribution: predicate i has n/k quads.
   Good for testing predicate-based scans."
  [n k]
  (vec (for [i (range n)]
         [(entity-uri i)
          (predicate-uri (str "p" (mod i k)))
          (literal-int i)
          nil])))

(defn generate-shared-objects
  "Generate n quads where m objects are shared (many-to-few pattern).
   Each entity points to one of m objects.
   Good for testing object-based lookups and joins."
  [n m]
  (let [pred (predicate-uri "refersTo")]
    (vec (for [i (range n)]
           [(entity-uri i)
            pred
            (entity-uri (str "shared-" (mod i m)))
            nil]))))

(defn generate-join-friendly
  "Generate data suitable for 2-way and 3-way join testing.
   Creates entities with relationships that form a diamond/chain pattern.
   Returns approx n*3 quads."
  [n]
  (let [knows (predicate-uri "knows")
        works-at (predicate-uri "worksAt")
        located-in (predicate-uri "locatedIn")]
    (vec (concat
          ;; People know each other (chain)
          (for [i (range (dec n))]
            [(entity-uri (str "person-" i))
             knows
             (entity-uri (str "person-" (inc i)))
             nil])
          ;; People work at companies
          (for [i (range n)]
            [(entity-uri (str "person-" i))
             works-at
             (entity-uri (str "company-" (mod i 10)))
             nil])
          ;; Companies located in cities
          (for [i (range 10)]
            [(entity-uri (str "company-" i))
             located-in
             (entity-uri (str "city-" (mod i 3)))
             nil])))))

;;; ---------------------------------------------------------------------------
;;; Scale Test Data
;;; ---------------------------------------------------------------------------

(defn generate-batch-for-scale
  "Generate a batch of n quads for scale testing.
   Uses a deterministic pattern based on offset for reproducibility."
  [n offset]
  (vec (for [i (range n)]
         (let [idx (+ offset i)]
           [(entity-uri idx)
            (predicate-uri (str "prop" (mod idx 100)))
            (literal-int idx)
            nil]))))

;;; ---------------------------------------------------------------------------
;;; Query Pattern Data
;;; ---------------------------------------------------------------------------

(defn sample-quad
  "Return a random sample quad from a quad collection."
  [quads]
  (rand-nth quads))

(defn sample-subject
  "Return a random subject from generated data range."
  [n]
  (entity-uri (rand-int n)))

(defn sample-predicate
  "Return a random predicate from k predicates."
  [k]
  (predicate-uri (str "prop" (rand-int k))))

(defn sample-context
  "Return a random context from num-contexts."
  [num-contexts]
  (graph-uri (rand-int num-contexts)))

;;; ---------------------------------------------------------------------------
;;; Pattern Templates for Query Testing
;;; ---------------------------------------------------------------------------

(defn bgp-pattern
  "Create a BGP pattern map with optional wildcards (nil)."
  [s p o]
  {:s s :p p :o o})

(defn var-pattern
  "Create a BGP pattern with variables (strings starting with ?)."
  [s p o]
  {:s s :p p :o o})

(def sample-patterns
  "Sample query patterns for benchmarking."
  {:exact-lookup (fn [quads]
                   (let [[s p o _] (sample-quad quads)]
                     {:s s :p p :o o}))

   :subject-scan (fn [quads]
                   (let [[s _ _ _] (sample-quad quads)]
                     {:s s :p "?p" :o "?o"}))

   :predicate-scan (fn [quads]
                     (let [[_ p _ _] (sample-quad quads)]
                       {:s "?s" :p p :o "?o"}))

   :object-scan (fn [quads]
                  (let [[_ _ o _] (sample-quad quads)]
                    {:s "?s" :p "?p" :o o}))

   :full-scan (fn [_]
                {:s "?s" :p "?p" :o "?o"})})

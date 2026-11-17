(ns ^:no-doc rama-sail.terms
  "Canonical term strings shared by the module and its clients. No RDF4J
   imports: the module loads this namespace, and keeping RDF4J out of the
   module means Rama workers do not need the Java version RDF4J requires.")

;; Internal marker for default graph context. Uses a non-IRI format that cannot
;; be created through normal RDF4J APIs, preventing user data collision.
;; The "::" prefix is invalid in IRIs, making this a safe internal-only value.
(def DEFAULT-CONTEXT-VAL "::rama-internal::default-graph")

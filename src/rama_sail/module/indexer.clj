(ns rama-sail.module.indexer
  "Composable building blocks for the RDF indexer microbatch topology.
   Declares PStates and ETL sources for quad storage, statistics, and materialized views.
   Use these functions from within a defmodule body to set up the core RDF indexing pipeline."
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [rama-sail.query.helpers :as qh]
            [rama-sail.sail.serialization :as ser]))

(def ^{:doc "RDF type predicate IRI"} RDF-TYPE-PREDICATE qh/RDF-TYPE-PREDICATE)

(defn dec-floor-zero
  "Decrement a value but never go below zero. Guards against negative statistics
   under edge cases like out-of-order delivery or double-delete."
  [n]
  (max 0 (dec (or n 0))))

(defn inc-or-init
  "Increment a possibly-nil counter (nil counts as 0). Used with termval to
   write a counter whose previous value was already read by a local-select> —
   (term inc) would re-read from RocksDB a value we already hold."
  [n]
  (inc (or n 0)))

(defn new-subject?
  "True when a quad becoming visible belongs to a subject with no prior triples."
  [becoming-visible subj-existed]
  (boolean (and becoming-visible (not subj-existed))))

(defn nil-or-zero?
  "True for nil or 0 — a predicate-stats :count in this state means the
   predicate is (re)appearing."
  [n]
  (or (nil? n) (zero? n)))

;; --- Empty Container Cleanup ---
;; After deleting an element from a nested map structure, empty containers
;; should be removed to prevent memory growth over time.

;; Helper: Generic traversal for indices structured as Level1 -> Level2 -> Level3 -> Set<Level4>
;; Used for $$spoc, $$posc, $$ospc.
(deframaop scan-3-levels-index [*k1 *k2 *k3 *k4 *pstate]
		;; 2. Conditional Logic: Traverse based on which keys are known (non-nil)
  (<<cond
		;; Case A: All keys known (Exact Quad check)
   (case> (and> *k2 *k3 *k4))
   (local-select> [(keypath *k1 *k2 *k3 *k4)] *pstate :> *exists?)
		;; Emit only if exists
   (filter> *exists?)
   (:> *k1 *k2 *k3 *k4)

		;; Case B: k4 is wildcard (Find all C for specific S,P,O)
   (case> (and> *k2 *k3 (nil? *k4)))
   (local-select> [(keypath *k1 *k2 *k3) ALL] *pstate {:allow-yield? true} :> *out4)
   (:> *k1 *k2 *k3 *out4)

		;; Case C: k3, k4 are wildcards (Find all O,C for specific S,P)
   (case> (and> *k2 (nil? *k3) (nil? *k4)))
   (local-select>
    [*k1 *k2 ALL (collect-one FIRST) LAST ALL] *pstate {:allow-yield? true}
    :> [*out3 *out4])
   (:> *k1 *k2 *out3 *out4)

		;; Case C1: k2 is wildcard, k4 known, k3 known (Find all P for specific S,O,C)
   (case> (and> (nil? *k2) *k3 *k4))
   (local-select>
    [(keypath *k1) ALL
     (selected? LAST *k3 ALL (pred= *k4)) FIRST] *pstate {:allow-yield? true}
    :> *out2)
   (:> *k1 *out2 *k3 *k4)

		;; Case C2: k3 is wildcard, k2 known, k4 known (Find all O for specific S,P,C)
   (case> (and> *k2 (nil? *k3) *k4))
   (local-select>
    [(keypath *k1 *k2) ALL
     (selected? LAST ALL (pred= *k4)) FIRST] *pstate {:allow-yield? true}
    :> *out3)
   (:> *k1 *k2 *out3 *k4)

		; Case C3: k2 and k4 are wildcards, k3 known (Find all P for specific S,O)
   (case> (and> (nil? *k2) *k3 (nil? *k4)))
   (local-select>
    [(keypath *k1) ALL (collect-one FIRST) LAST (keypath *k3) ALL] *pstate {:allow-yield? true}
    :> [*out2 *out4])
   (:> *k1 *out2 *k3 *out4)

		;; Case C4: k2 and k3 are wildcards, k4 known (Find all P,O for specific S,C)
   (case> (and> (nil? *k2) (nil? *k3) *k4))
   (local-select>
    [(keypath *k1) ALL
     (collect-one FIRST) LAST ALL (selected? LAST ALL #{*k4}) FIRST] *pstate {:allow-yield? true}
    :> [*out2 *out3])
   (:> *k1 *out2 *out3 *k4)

		;; Case D: k2, k3, k4 wildcards (Find all P,O,C for specific S)
   (default>)
   (local-select>
    [(keypath *k1) ALL
     (collect-one FIRST) LAST ALL (collect-one FIRST) LAST ALL] *pstate {:allow-yield? true}
    :> [*out2 *out3 *out4])
   (:> *k1 *out2 *out3 *out4)))

(defn declare-rdf-index-pstates
  "Declares all core RDF index and statistics PStates on microbatch topology `mb`.
   Call from within a defmodule body after creating the microbatch topology."
  [mb]
  ;; Quad indices: S->P->O->Set<C>, P->O->S->Set<C>, O->S->P->Set<C>, C->S->P->Set<O>
  (declare-pstate mb $$spoc {String (map-schema String (map-schema String (set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})
  (declare-pstate mb $$posc {String (map-schema String (map-schema String (set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})
  (declare-pstate mb $$ospc {String (map-schema String (map-schema String (set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})
  (declare-pstate mb $$cspo {String (map-schema String (map-schema String (set-schema String {:subindex? true}) {:subindex? true}) {:subindex? true})})
  ;; Per-predicate statistics for selectivity estimation
  (declare-pstate mb $$predicate-stats {String (fixed-keys-schema
                                                {:count Long
                                                 :distinct-subjects Long
                                                 :distinct-objects Long})})
  ;; Global statistics for overall cardinality estimation (single partition on task 0)
  (declare-pstate mb $$global-stats (fixed-keys-schema
                                     {:total-triples Long
                                      :total-predicates Long
                                      :total-subjects Long})
                  {:global? true})
  ;; Accurate distinct tracking: pred -> subject/object -> count
  (declare-pstate mb $$pred-subj-count {String (map-schema String Long {:subindex? true})})
  (declare-pstate mb $$pred-obj-count {String (map-schema String Long {:subindex? true})})
  ;; Materialized views for type-based queries
  (declare-pstate mb $$type-subjects {String (set-schema String {:subindex? true})})
  (declare-pstate mb $$subject-types {String (set-schema String {:subindex? true})})
  ;; Context-aware cardinality tracking for type views
  (declare-pstate mb $$type-subject-count {String (map-schema String Long {:subindex? true})})
  (declare-pstate mb $$subject-type-count {String (map-schema String Long {:subindex? true})})
  ;; Temporal tracking: s -> p -> o -> c -> tx-time
  (declare-pstate mb $$quad-tx-time {String (map-schema String (map-schema String (map-schema String Long {:subindex? true}) {:subindex? true}) {:subindex? true})}))

(defn rdf-indexer-sources
  "Wires up the core RDF indexer ETL on microbatch topology `mb`.
   Handles :add, :del, and :clear-context operations from *triple-depot.
   Requires all PStates from declare-rdf-index-pstates to be declared on `mb`."
  [mb]
  (<<sources mb
             (source> *triple-depot :> %microbatch)
             ;; Format: [op [s p o c] tx-time]
             (%microbatch :> *msg)
             (first *msg :> *op)

             (<<cond
              ;; --- ADD/DEL/CLEAR-CONTEXT: Quad operations ---
              (default>)
              ;; Format: [op [s p o c] tx-time]
              (identity (second *msg) :> [*s *p *o *c])
              (identity (nth *msg 2 nil) :> *raw-tx-time)
              (<<if (nil? *raw-tx-time)
                    (identity (System/currentTimeMillis) :> *tx-time)
                    (else>)
                    (identity *raw-tx-time :> *tx-time))

              (<<cond
               (case> (= *op :add))
               ;; Check if this quad already exists (for idempotent stats tracking)
               (|hash *s)
               (local-select> [(keypath *s *p *o *c)] $$quad-tx-time :> *existing-tx-time)
               ;; Quad is becoming visible iff it doesn't already exist
               (identity (nil? *existing-tx-time) :> *becoming-visible)

               ;; Claim visibility IMMEDIATELY, on the same task as the check with
               ;; no partitioner in between: ops between partitioners run atomically
               ;; per event, so a duplicate :add of the same quad later in this
               ;; microbatch observes the entry and is NOT "becoming visible".
               ;; (Previously the write happened 4 hops later, so concurrent
               ;; duplicates each saw nil and permanently double-counted stats.)
               (<<if *becoming-visible
                     (local-transform> [(keypath *s *p *o *c) (termval *tx-time)] $$quad-tx-time))

               ;; Subject visibility must be sampled BEFORE the $$spoc insert on
               ;; this same task: a brand-new subject means :total-subjects grows.
               (local-select> [(keypath *s) (view some?)] $$spoc :> *subj-existed)
               (new-subject? *becoming-visible *subj-existed :> *new-subject)

               ;; Update indices (idempotent - set semantics)
               (local-transform> [(keypath *s *p *o) NIL->SET NONE-ELEM (termval *c)] $$spoc)
               (|hash *p) (local-transform> [(keypath *p *o *s) NIL->SET NONE-ELEM (termval *c)] $$posc)
               (|hash *o) (local-transform> [(keypath *o *s *p) NIL->SET NONE-ELEM (termval *c)] $$ospc)
               (|hash *c) (local-transform> [(keypath *c *s *p) NIL->SET NONE-ELEM (termval *o)] $$cspo)

               ;; Only update statistics if quad is BECOMING VISIBLE (not already visible)
               ;; This ensures idempotent adds don't inflate counts
               (<<if *becoming-visible
                     ;; Update statistics with accurate distinct tracking.
                     ;; The :count read and increment are adjacent on the predicate
                     ;; task, so the 0->1 transition (new predicate) is race-free.
                     (|hash *p)
                     (local-select> [(keypath *p :count)] $$predicate-stats :> *prev-pred-count)
                     (nil-or-zero? *prev-pred-count :> *new-predicate)
                     ;; termval: value already read above — (term inc) would re-read
                     (inc-or-init *prev-pred-count :> *next-pred-count)
                     (local-transform> [(keypath *p :count) (termval *next-pred-count)] $$predicate-stats)

                     ;; Track distinct subjects: increment only when first triple for (pred, subject)
                     (local-select> [(keypath *p *s)] $$pred-subj-count :> *prev-subj-count)
                     (inc-or-init *prev-subj-count :> *next-subj-count)
                     (local-transform> [(keypath *p *s) (termval *next-subj-count)] $$pred-subj-count)
                     (<<if (nil? *prev-subj-count)
                           (local-transform> [(keypath *p :distinct-subjects) (nil->val 0) (term inc)] $$predicate-stats))

                     ;; Track distinct objects: increment only when first triple for (pred, object)
                     (local-select> [(keypath *p *o)] $$pred-obj-count :> *prev-obj-count)
                     (inc-or-init *prev-obj-count :> *next-obj-count)
                     (local-transform> [(keypath *p *o) (termval *next-obj-count)] $$pred-obj-count)
                     (<<if (nil? *prev-obj-count)
                           (local-transform> [(keypath *p :distinct-objects) (nil->val 0) (term inc)] $$predicate-stats))

                     ;; Update global statistics (single global partition)
                     (|global)
                     (local-transform> [(keypath :total-triples) (nil->val 0) (term inc)] $$global-stats)
                     (<<if *new-subject
                           (local-transform> [(keypath :total-subjects) (nil->val 0) (term inc)] $$global-stats))
                     (<<if *new-predicate
                           (local-transform> [(keypath :total-predicates) (nil->val 0) (term inc)] $$global-stats))

                     ;; --- Materialized View Maintenance (add) ---
                     ;; If this is an rdf:type triple, update type views with context-aware cardinality.
                     (<<if (= *p RDF-TYPE-PREDICATE)
                           ;; *o is the type, *s is the subject
                           ;; Track type->subject occurrence count
                           (|hash *o)
                           (local-select> [(keypath *o *s)] $$type-subject-count :> *prev-ts-count)
                           (inc-or-init *prev-ts-count :> *next-ts-count)
                           (local-transform> [(keypath *o *s) (termval *next-ts-count)] $$type-subject-count)
                           ;; Only add to type view if this is the FIRST occurrence
                           (<<if (nil? *prev-ts-count)
                                 (local-transform> [(keypath *o) NIL->SET NONE-ELEM (termval *s)] $$type-subjects))
                           ;; Track subject->type occurrence count (symmetric)
                           (|hash *s)
                           (local-select> [(keypath *s *o)] $$subject-type-count :> *prev-st-count)
                           (inc-or-init *prev-st-count :> *next-st-count)
                           (local-transform> [(keypath *s *o) (termval *next-st-count)] $$subject-type-count)
                           ;; Only add to subject-types view if this is the FIRST occurrence
                           (<<if (nil? *prev-st-count)
                                 (local-transform> [(keypath *s) NIL->SET NONE-ELEM (termval *o)] $$subject-types))))

               (case> (= *op :del))
               ;; Delete a quad: physically remove from all indices

               (|hash *s)
               ;; Check if quad ever existed
               (local-select> [(keypath *s *p *o *c)] $$quad-tx-time :> *quad-existed)

               (<<if (some? *quad-existed)
                     ;; Claim the delete IMMEDIATELY (same task, no partitioner in
                     ;; between the check and this removal): a duplicate :del of the
                     ;; same quad later in this microbatch sees the entry gone and
                     ;; does not double-decrement statistics.
                     (local-transform> [(keypath *s *p *o *c) NONE>] $$quad-tx-time)
                     (local-transform> [(keypath *s *p *o) (if-path (pred empty?) NONE>)] $$quad-tx-time)
                     (local-transform> [(keypath *s *p) (if-path (pred empty?) NONE>)] $$quad-tx-time)
                     (local-transform> [(keypath *s) (if-path (pred empty?) NONE>)] $$quad-tx-time)

                     ;; Remove from all 4 indices, pruning newly-empty containers so
                     ;; churn workloads don't grow the indices without bound.
                     (local-transform> [(keypath *s *p *o) (set-elem *c) NONE>] $$spoc)
                     (local-transform> [(keypath *s *p *o) (if-path (pred empty?) NONE>)] $$spoc)
                     (local-transform> [(keypath *s *p) (if-path (pred empty?) NONE>)] $$spoc)
                     (local-transform> [(keypath *s) (if-path (pred empty?) NONE>)] $$spoc)
                     ;; Did this delete remove the subject's last triple?
                     ;; (Sampled on the subject task before hopping away.)
                     (local-select> [(keypath *s) (view some?)] $$spoc :> *subj-still-exists)

                     (|hash *p)
                     (local-transform> [(keypath *p *o *s) (set-elem *c) NONE>] $$posc)
                     (local-transform> [(keypath *p *o *s) (if-path (pred empty?) NONE>)] $$posc)
                     (local-transform> [(keypath *p *o) (if-path (pred empty?) NONE>)] $$posc)
                     (local-transform> [(keypath *p) (if-path (pred empty?) NONE>)] $$posc)

                     (|hash *o)
                     (local-transform> [(keypath *o *s *p) (set-elem *c) NONE>] $$ospc)
                     (local-transform> [(keypath *o *s *p) (if-path (pred empty?) NONE>)] $$ospc)
                     (local-transform> [(keypath *o *s) (if-path (pred empty?) NONE>)] $$ospc)
                     (local-transform> [(keypath *o) (if-path (pred empty?) NONE>)] $$ospc)

                     (|hash *c)
                     (local-transform> [(keypath *c *s *p) (set-elem *o) NONE>] $$cspo)
                     (local-transform> [(keypath *c *s *p) (if-path (pred empty?) NONE>)] $$cspo)
                     (local-transform> [(keypath *c *s) (if-path (pred empty?) NONE>)] $$cspo)
                     (local-transform> [(keypath *c) (if-path (pred empty?) NONE>)] $$cspo)

                     ;; Update statistics - decrement counts for deleted triple.
                     ;; :count read and decrement are adjacent on the predicate task,
                     ;; so the 1->0 transition (predicate vanishing) is race-free.
                     (|hash *p)
                     (local-select> [(keypath *p :count)] $$predicate-stats :> *cur-pred-count)
                     (identity (= *cur-pred-count 1) :> *pred-vanished)
                     ;; termval: value already read above — (term ...) would re-read
                     (dec-floor-zero *cur-pred-count :> *next-pred-count)
                     (local-transform> [(keypath *p :count) (termval *next-pred-count)] $$predicate-stats)

                     ;; Decrement distinct subjects only when last triple for (pred, subject) removed
                     (local-select> [(keypath *p *s)] $$pred-subj-count :> *cur-subj-count)
                     (dec-floor-zero *cur-subj-count :> *next-subj-count)
                     (local-transform> [(keypath *p *s) (termval *next-subj-count)] $$pred-subj-count)
                     (<<if (= *cur-subj-count 1)
                           ;; Last occurrence - decrement distinct count AND cleanup the mapping
                           (local-transform> [(keypath *p :distinct-subjects) (nil->val 0) (term dec-floor-zero)] $$predicate-stats)
                           (local-transform> [(keypath *p *s) NONE>] $$pred-subj-count))

                     ;; Decrement distinct objects only when last triple for (pred, object) removed
                     (local-select> [(keypath *p *o)] $$pred-obj-count :> *cur-obj-count)
                     (dec-floor-zero *cur-obj-count :> *next-obj-count)
                     (local-transform> [(keypath *p *o) (termval *next-obj-count)] $$pred-obj-count)
                     (<<if (= *cur-obj-count 1)
                           ;; Last occurrence - decrement distinct count AND cleanup the mapping
                           (local-transform> [(keypath *p :distinct-objects) (nil->val 0) (term dec-floor-zero)] $$predicate-stats)
                           (local-transform> [(keypath *p *o) NONE>] $$pred-obj-count))

                     ;; Update global statistics
                     (|global)
                     (local-transform> [(keypath :total-triples) (nil->val 0) (term dec-floor-zero)] $$global-stats)
                     (<<if (not *subj-still-exists)
                           (local-transform> [(keypath :total-subjects) (nil->val 0) (term dec-floor-zero)] $$global-stats))
                     (<<if *pred-vanished
                           (local-transform> [(keypath :total-predicates) (nil->val 0) (term dec-floor-zero)] $$global-stats))

                     ;; --- Materialized View Maintenance (delete) ---
                     ;; If this is an rdf:type triple, update type views with context-aware cardinality.
                     ;; Only remove from view when this is the LAST context having this (type, subject) pair.
                     (<<if (= *p RDF-TYPE-PREDICATE)
                           ;; *o is the type, *s is the subject
                           ;; Decrement type->subject occurrence count
                           (|hash *o)
                           (local-select> [(keypath *o *s)] $$type-subject-count :> *cur-ts-count)
                           (dec-floor-zero *cur-ts-count :> *next-ts-count)
                           (local-transform> [(keypath *o *s) (termval *next-ts-count)] $$type-subject-count)
                           ;; Only remove from type view if this was the LAST occurrence
                           (<<if (= *cur-ts-count 1)
                                 (local-transform> [(keypath *o) (set-elem *s) NONE>] $$type-subjects)
                                 (local-transform> [(keypath *o) (if-path (pred empty?) NONE>)] $$type-subjects)
                                 (local-transform> [(keypath *o *s) NONE>] $$type-subject-count))
                           ;; Decrement subject->type occurrence count (symmetric)
                           (|hash *s)
                           (local-select> [(keypath *s *o)] $$subject-type-count :> *cur-st-count)
                           (dec-floor-zero *cur-st-count :> *next-st-count)
                           (local-transform> [(keypath *s *o) (termval *next-st-count)] $$subject-type-count)
                           ;; Only remove from subject-types view if this was the LAST occurrence
                           (<<if (= *cur-st-count 1)
                                 (local-transform> [(keypath *s) (set-elem *o) NONE>] $$subject-types)
                                 (local-transform> [(keypath *s) (if-path (pred empty?) NONE>)] $$subject-types)
                                 (local-transform> [(keypath *s *o) NONE>] $$subject-type-count))))

               (case> (= *op :clear-context))
               ;; Clear all quads in a context. Mode-dependent deletion.
               ;; Respects last-write-wins: only deletes quads whose creation time <= clear tx-time.
               ;; 1. Read all (S, P, O) from $$cspo[c]
               (local-select> [(keypath *c) ALL (collect-one FIRST)
                               LAST ALL (collect-one FIRST)
                               LAST ALL] $$cspo {:allow-yield? true}
                              :> [*s *p *o])
               ;; 2. Check existing state
               (|hash *s)
               (local-select> [(keypath *s *p *o *c)] $$quad-tx-time :> *quad-creation-time)
               ;; 3. Apply deletion if quad exists and clear is not older than creation
               (<<if (and> (some? *quad-creation-time)
                           (>= *tx-time *quad-creation-time))
                     ;; Claim the delete atomically with the check (same task): a
                     ;; duplicate :clear-context or concurrent :del of this quad in
                     ;; the same microbatch sees the entry gone.
                     (local-transform> [(keypath *s *p *o *c) NONE>] $$quad-tx-time)
                     (local-transform> [(keypath *s *p *o) (if-path (pred empty?) NONE>)] $$quad-tx-time)
                     (local-transform> [(keypath *s *p) (if-path (pred empty?) NONE>)] $$quad-tx-time)
                     (local-transform> [(keypath *s) (if-path (pred empty?) NONE>)] $$quad-tx-time)

                     ;; Remove from all indices, pruning newly-empty containers
                     (local-transform> [(keypath *s *p *o) (set-elem *c) NONE>] $$spoc)
                     (local-transform> [(keypath *s *p *o) (if-path (pred empty?) NONE>)] $$spoc)
                     (local-transform> [(keypath *s *p) (if-path (pred empty?) NONE>)] $$spoc)
                     (local-transform> [(keypath *s) (if-path (pred empty?) NONE>)] $$spoc)
                     (local-select> [(keypath *s) (view some?)] $$spoc :> *subj-still-exists)

                     (|hash *p)
                     (local-transform> [(keypath *p *o *s) (set-elem *c) NONE>] $$posc)
                     (local-transform> [(keypath *p *o *s) (if-path (pred empty?) NONE>)] $$posc)
                     (local-transform> [(keypath *p *o) (if-path (pred empty?) NONE>)] $$posc)
                     (local-transform> [(keypath *p) (if-path (pred empty?) NONE>)] $$posc)

                     (|hash *o)
                     (local-transform> [(keypath *o *s *p) (set-elem *c) NONE>] $$ospc)
                     (local-transform> [(keypath *o *s *p) (if-path (pred empty?) NONE>)] $$ospc)
                     (local-transform> [(keypath *o *s) (if-path (pred empty?) NONE>)] $$ospc)
                     (local-transform> [(keypath *o) (if-path (pred empty?) NONE>)] $$ospc)

                     (|hash *c)
                     (local-transform> [(keypath *c *s *p) (set-elem *o) NONE>] $$cspo)
                     (local-transform> [(keypath *c *s *p) (if-path (pred empty?) NONE>)] $$cspo)
                     (local-transform> [(keypath *c *s) (if-path (pred empty?) NONE>)] $$cspo)
                     (local-transform> [(keypath *c) (if-path (pred empty?) NONE>)] $$cspo)

                     (|hash *p)
                     (local-select> [(keypath *p :count)] $$predicate-stats :> *cur-pred-count)
                     (identity (= *cur-pred-count 1) :> *pred-vanished)
                     ;; termval: value already read above — (term ...) would re-read
                     (dec-floor-zero *cur-pred-count :> *next-pred-count)
                     (local-transform> [(keypath *p :count) (termval *next-pred-count)] $$predicate-stats)

                     ;; Decrement distinct subjects only when last triple for (pred, subject) removed
                     (local-select> [(keypath *p *s)] $$pred-subj-count :> *cur-subj-count)
                     (dec-floor-zero *cur-subj-count :> *next-subj-count)
                     (local-transform> [(keypath *p *s) (termval *next-subj-count)] $$pred-subj-count)
                     (<<if (= *cur-subj-count 1)
                           ;; Last occurrence - decrement distinct count AND cleanup the mapping
                           (local-transform> [(keypath *p :distinct-subjects) (nil->val 0) (term dec-floor-zero)] $$predicate-stats)
                           (local-transform> [(keypath *p *s) NONE>] $$pred-subj-count))

                     ;; Decrement distinct objects only when last triple for (pred, object) removed
                     (local-select> [(keypath *p *o)] $$pred-obj-count :> *cur-obj-count)
                     (dec-floor-zero *cur-obj-count :> *next-obj-count)
                     (local-transform> [(keypath *p *o) (termval *next-obj-count)] $$pred-obj-count)
                     (<<if (= *cur-obj-count 1)
                           ;; Last occurrence - decrement distinct count AND cleanup the mapping
                           (local-transform> [(keypath *p :distinct-objects) (nil->val 0) (term dec-floor-zero)] $$predicate-stats)
                           (local-transform> [(keypath *p *o) NONE>] $$pred-obj-count))

                     (|global)
                     (local-transform> [(keypath :total-triples) (nil->val 0) (term dec-floor-zero)] $$global-stats)
                     (<<if (not *subj-still-exists)
                           (local-transform> [(keypath :total-subjects) (nil->val 0) (term dec-floor-zero)] $$global-stats))
                     (<<if *pred-vanished
                           (local-transform> [(keypath :total-predicates) (nil->val 0) (term dec-floor-zero)] $$global-stats))
                     ;; Update type views if this is an rdf:type triple (context-aware cardinality)
                     (<<if (= *p RDF-TYPE-PREDICATE)
                           ;; Decrement type->subject occurrence count
                           (|hash *o)
                           (local-select> [(keypath *o *s)] $$type-subject-count :> *cur-ts-count)
                           (dec-floor-zero *cur-ts-count :> *next-ts-count)
                           (local-transform> [(keypath *o *s) (termval *next-ts-count)] $$type-subject-count)
                           ;; Only remove from type view if this was the LAST occurrence
                           (<<if (= *cur-ts-count 1)
                                 (local-transform> [(keypath *o) (set-elem *s) NONE>] $$type-subjects)
                                 (local-transform> [(keypath *o) (if-path (pred empty?) NONE>)] $$type-subjects)
                                 (local-transform> [(keypath *o *s) NONE>] $$type-subject-count))
                           ;; Decrement subject->type occurrence count (symmetric)
                           (|hash *s)
                           (local-select> [(keypath *s *o)] $$subject-type-count :> *cur-st-count)
                           (dec-floor-zero *cur-st-count :> *next-st-count)
                           (local-transform> [(keypath *s *o) (termval *next-st-count)] $$subject-type-count)
                           ;; Only remove from subject-types view if this was the LAST occurrence
                           (<<if (= *cur-st-count 1)
                                 (local-transform> [(keypath *s) (set-elem *o) NONE>] $$subject-types)
                                 (local-transform> [(keypath *s) (if-path (pred empty?) NONE>)] $$subject-types)
                                 (local-transform> [(keypath *s *o) NONE>] $$subject-type-count))))))))

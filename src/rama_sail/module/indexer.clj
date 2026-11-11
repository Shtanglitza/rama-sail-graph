(ns rama-sail.module.indexer
  "Composable building blocks for the RDF indexer microbatch topology.
   Declares PStates and ETL sources for quad storage, statistics, and materialized views.
   Use these functions from within a defmodule body to set up the core RDF indexing pipeline."
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require [clojure.tools.logging :as log]
            [com.rpl.rama.ops :as ops]
            [rama-sail.query.helpers :as qh]
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
  ;; Graph clears in progress: c -> {:tx clear tx-time, :cursor last subject cleared}.
  ;; Lives on the graph's task (where $$cspo holds the graph); empty when idle.
  (declare-pstate mb $$pending-clears {String (fixed-keys-schema {:tx Long :cursor String})})
  ;; Temporal tracking: s -> p -> o -> c -> tx-time
  (declare-pstate mb $$quad-tx-time {String (map-schema String (map-schema String (map-schema String Long {:subindex? true}) {:subindex? true}) {:subindex? true})}))

;; --- Depot record validation ---

(defn valid-depot-record?
  "True for a record the indexer can apply: `[op [s p o c]]` or
   `[op [s p o c] tx-time]` with op :add or :del and four string terms, or
   op :clear-context and a string graph in the fourth position; tx-time is an
   integer or nil.

   The depot accepts any object, and a record that throws in the topology
   fails its microbatch, which Rama retries from the start with the same data,
   forever. So the indexer checks every record with this and drops the rest.
   Direct depot writers can use it to check records before appending."
  [record]
  (and (sequential? record)
       (<= 2 (count record) 3)
       (let [[op quad tx-time] record]
         (and (sequential? quad)
              (= 4 (count quad))
              (or (nil? tx-time) (integer? tx-time))
              (case op
                (:add :del) (every? string? quad)
                :clear-context (string? (nth quad 3))
                false)))))

(defn indexable-record?
  "valid-depot-record?, logging what is dropped. A [:clear-continue ...] record
   is the indexer's own wake-up for pending clears and is skipped silently."
  [record]
  (cond
    (valid-depot-record? record) true
    (and (sequential? record) (= :clear-continue (first record))) false
    :else (do (log/warn "Indexer dropped a malformed *triple-depot record:" (pr-str record))
              false)))

;; --- Bounded graph clears ---

(def clear-quads-per-microbatch
  "Most quads one task deletes for pending graph clears in one microbatch. A
   microbatch reads at most depot.microbatch.max.records (default 1000) records
   per partition, so this keeps clear work in the same range as ordinary load."
  1000)

(def clear-subjects-per-page
  "Subjects of a graph read per $$cspo page while clearing."
  200)

(defn clear-quads-budget [] clear-quads-per-microbatch)
(defn clear-page-subjects [] clear-subjects-per-page)

(defn merge-pending-clear
  "Pending-clear entry for a graph after a clear at `tx`. A later clear restarts
   the scan, since quads created between the two stamps may lie behind the
   cursor; an older or equal one changes nothing."
  [pending tx]
  (if (and pending (>= (:tx pending) tx))
    pending
    {:tx tx :cursor ""}))

(defn clear-slice
  "Next slice of a graph clear. `page` holds the [s p o] triples of up to
   `page-subjects` subjects in subject order. Takes whole subjects while they
   fit in `budget` quads (always at least one, so a subject larger than the
   budget still makes progress). Returns the quads to delete, the cursor (last
   subject taken) and whether the graph has been scanned to its end."
  [page page-subjects budget]
  (let [by-subject (partition-by first page)
        taken (loop [taken [] n 0 groups by-subject]
                (if-let [g (first groups)]
                  (if (or (empty? taken) (<= (+ n (count g)) budget))
                    (recur (conj taken g) (+ n (count g)) (rest groups))
                    taken)
                  taken))]
    {:quads (into [] cat taken)
     :next-cursor (or (some-> (last taken) first first) "")
     :exhausted? (and (< (count by-subject) page-subjects)
                      (= (count taken) (count by-subject)))}))

(deframaop delete-quad
  "Physically removes quad [s p o c] from every index, statistic and type view,
   if it exists and was created at or before `*max-tx`. Emits nothing.
   Shared by :del (max-tx = Long/MAX_VALUE) and graph clears (max-tx = the
   clear's tx-time: last-write-wins against later adds)."
  [*s *p *o *c *max-tx]
  ;; Module PStates are not in scope in a deframaop: resolve them by name on
  ;; whichever task the code is running on.
  (<<with-substitutions
   [$$cspo (this-module-pobject-task-global "$$cspo")
    $$global-stats (this-module-pobject-task-global "$$global-stats")
    $$ospc (this-module-pobject-task-global "$$ospc")
    $$posc (this-module-pobject-task-global "$$posc")
    $$pred-obj-count (this-module-pobject-task-global "$$pred-obj-count")
    $$pred-subj-count (this-module-pobject-task-global "$$pred-subj-count")
    $$predicate-stats (this-module-pobject-task-global "$$predicate-stats")
    $$quad-tx-time (this-module-pobject-task-global "$$quad-tx-time")
    $$spoc (this-module-pobject-task-global "$$spoc")
    $$subject-type-count (this-module-pobject-task-global "$$subject-type-count")
    $$subject-types (this-module-pobject-task-global "$$subject-types")
    $$type-subject-count (this-module-pobject-task-global "$$type-subject-count")
    $$type-subjects (this-module-pobject-task-global "$$type-subjects")]
    (|hash *s)
    (local-select> [(keypath *s *p *o *c)] $$quad-tx-time :> *quad-creation-time)
    ;; Delete only a quad that exists and was created at or before *max-tx
    (<<if (and> (some? *quad-creation-time)
                (>= *max-tx *quad-creation-time))
          ;; Claim the delete atomically with the check (same task, no
          ;; partitioner in between): a duplicate delete of this quad later in
          ;; the microbatch sees the entry gone and does not double-decrement
          ;; statistics.
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
                      (local-transform> [(keypath *s *o) NONE>] $$subject-type-count))))))

(defn rdf-indexer-sources
  "Wires up the core RDF indexer ETL on microbatch topology `mb`.
   Handles :add, :del, and :clear-context operations from *triple-depot. A
   graph clear is registered in $$pending-clears and applied in bounded slices,
   one per microbatch.
   Requires all PStates from declare-rdf-index-pstates to be declared on `mb`."
  [mb]
  (<<sources mb
             (source> *triple-depot :> %microbatch)
             ;; --- Depot records ---
             ;; Format: [op [s p o c] tx-time]. A batch block so that the clear
             ;; slices below run after every record of this microbatch.
             (<<batch
             (%microbatch :> *msg)
             ;; Drops malformed records (they would throw and block the indexer
             ;; forever) and the [:clear-continue ...] wake-ups appended below.
             (filter> (indexable-record? *msg))
             (first *msg :> *op)

             (<<cond
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
               ;;
               ;; A re-add of an already-visible quad must still advance its
               ;; creation stamp to max(existing, tx): a :clear-context of the same
               ;; context (different partition, so unordered relative to this add
               ;; within a microbatch) deletes iff clear-tx >= creation-tx. Without
               ;; the refresh, "CLEAR GRAPH g; re-INSERT unchanged quads into g"
               ;; deleted those quads whenever the add happened to run first.
               ;; Existing stamp already read above -> termval is a pure write.
               (<<if (or> *becoming-visible (> *tx-time *existing-tx-time))
                     (local-transform> [(keypath *s *p *o *c) (termval *tx-time)] $$quad-tx-time)
                     (else>))

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
               (delete-quad *s *p *o *c Long/MAX_VALUE)

               (case> (= *op :clear-context))
               ;; Only REGISTER the clear here. Deleting every quad of the graph
               ;; in this microbatch is unbounded work: past the phase timeout
               ;; the microbatch fails and retries from the start with the same
               ;; data, forever, blocking the indexer. The batch block below
               ;; deletes a bounded slice per microbatch instead.
               ;; A single :clear-context record is already on c's partition
               ;; (depot-partition-key); the hop makes that explicit.
               (|hash *c)
               (local-select> [(keypath *c)] $$pending-clears :> *pending)
               (merge-pending-clear *pending *tx-time :> *merged)
               (local-transform> [(keypath *c) (termval *merged)] $$pending-clears))))

             ;; --- Bounded graph clears ---
             ;; Runs once per microbatch on every task, AFTER the batch block
             ;; above completed on all tasks, so a clear registered in this
             ;; microbatch starts here: a graph smaller than the budget is gone
             ;; by the time the microbatch that carried its clear commits.
             (<<batch
              (|all)
              ;; bound first: a call inside a path's options map is not evaluated
              (clear-page-subjects :> *page-size)
              (clear-quads-budget :> *task-budget)
              (loop<- [*budget *task-budget *after "" :> *gc *clear-tx *slice]
                      (local-select> [(sorted-map-range-from *after {:max-amt 1 :inclusive? false})
                                      (subselect ALL)]
                                     $$pending-clears :> *entries)
                      (<<if (seq *entries)
                            (identity (first *entries) :> [*pc {:keys [*tx *cursor]}])
                            (local-select> [(keypath *pc)
                                            (sorted-map-range-from *cursor {:max-amt *page-size
                                                                            :inclusive? false})
                                            (subselect ALL (collect-one FIRST)
                                                       LAST ALL (collect-one FIRST)
                                                       LAST ALL)]
                                           $$cspo :> *page)
                            (clear-slice *page *page-size *budget
                                         :> {:keys [*quads *next-cursor *exhausted?]})
                            (<<if *exhausted?
                                  (local-transform> [(keypath *pc) NONE>] $$pending-clears)
                                  (else>)
                                  (local-transform> [(keypath *pc :cursor) (termval *next-cursor)] $$pending-clears))
                            (:> *pc *tx *quads)
                            (- *budget (count *quads) :> *left)
                            (<<if (> *left 0)
                                  (continue> *left (ifexpr *exhausted? *pc *after))
                                  (else>)
                                  ;; Budget spent with clears possibly left: wake the
                                  ;; indexer for the next slice. With no new depot data
                                  ;; the next microbatch could be up to a minute away.
                                  ;; The record carries no work, so a duplicate from a
                                  ;; retried microbatch is harmless.
                                  (depot-partition-append! *triple-depot [:clear-continue [*pc]] :append-ack))
                            (else>)))
              (ops/explode *slice :> [*s *p *o])
              (delete-quad *s *p *o *gc *clear-tx))))

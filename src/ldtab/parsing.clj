(ns ldtab.parsing
  (:require [clojure.set :as set]
            [ldtab.thin2thick :as t2t])
  (:import [java.io FileInputStream]
           [org.apache.jena.graph Triple Node]
           [java.util Iterator]
           [org.apache.jena.riot RDFDataMgr Lang]))

(defn thin-triple?
  "Given a JENA triple, return true if the triple has no blank node dependencies."
  [^Triple triple]
  (let [s (.getSubject triple)
        o (.getObject triple)]
    (and (not (.isBlank s))
         (not (.isBlank o)))))

(defn map-on-hash-map-vals
  [f m]
  (zipmap (keys m) (map f (vals m))))

(defn get-triples
  "Given a stream of triples and a natural number n,
    return the next n triples from the stream."
  [^Iterator it ^long n]
  (loop [x 0
         triples '()]
    (if (and (.hasNext it)
             (< x n))
      (recur (inc x) (conj triples (.next it)))
      triples)))

(defn fetch-new-window
  "Given a stream of RDF triples, extract the next."
  [^Iterator it ^long windowsize]
  (let [new-triples (get-triples it windowsize) ;extract windowsize many new triples
        thin-triples (filter thin-triple? new-triples)
        thick-triples (remove thin-triple? new-triples)]
    [thin-triples thick-triples]))

(defn get-blanknode-dependency
  "Given a subject and a (direct) map from subjects to blank node dependencies,
  return the transitive closure of blank node dependencies for the subject."
  [^Node subject subject-2-blanknode]
  (let [direct (get subject-2-blanknode subject)
        ;TODO: this does not need to be recomputed recursively
        ;this map could be build up in a bottom-up fasion (however, this shouldn't speed things up too drastically)
        indirect (flatten (map (fn [^Node x] (get-blanknode-dependency x subject-2-blanknode)) direct))]
    (into () (remove nil? (set/union direct indirect)))))

;TODO write proper doc string 
(defn add-triples
  "Given a backlog map m1, and a map from subjects to triples m2,
    add triples associated with a subject from m2 to m1."
  [m1 m2]
  (loop [ks (keys m2)
         vs (vals m2)
         res m1]
    (if (empty? ks)
      res
      (recur (rest ks)
             (rest vs)
             (assoc-in res [(first ks) :triples] (first vs))))))

(defn is-resolved?
  "Given a subject and a backlog map,
    return true if all blank node dependencies are resolved,
    and false otherwise."
  [^Node subject backlog-map]
  (let [dependencies (get-in backlog-map [subject :dependencies])]
    (cond
      (not dependencies) false
      (empty? dependencies) true
      :else (every? (fn [^Node x] (is-resolved? x backlog-map)) dependencies))))

(defn updated?
  [^Node subject backlog-map]
  (let [dependencies (get-in backlog-map [subject :dependencies])]
    (if (get-in backlog-map [subject :updated])
      true
      (some (fn [^Node x] (updated? x backlog-map)) dependencies)))) ;NB: this can return nil

(defn build-backlog-map
  "Given a list of JENA triples,
    construct a map from subjects to a map containing information about 
    1. blank node dependencies
    2. whether all blank node dependencies can be resolved to non-blank nodes, and
    3. tripels associated with a subject:

    {subject-1 {:dependencies (d11, d12, ...)
                :updated true
                :resolved true/fase
                :triples ([subject1 p11 o11], [subject1 p12 o12], ...)},
     subject-2 {:dependencies (d21, d22, ...)
                :updated true
                :resolved true/fase
                :triples ([subject2 p21 o21], [subject2 p22 o22], ...)} 
     ...
     subject-n {:dependencies (dn1, dn2, ...)
                :updated true
                :resolved true/fase
                :triples ([subjectn pn1 on1], [subjectn pn2 on2], ...)}"
  [triples]
  (let [subject-map (group-by (fn [^Triple x] (.getSubject x)) triples)
        subject-2-object (map-on-hash-map-vals (fn [x] (map (fn [^Triple y] (.getObject y)) x)) subject-map)
        subject-2-blanknode (map-on-hash-map-vals (fn [x] (filter (fn [^Node y] (.isBlank y)) x)) subject-2-object)
        subjects (map (fn [^Triple x] (.getSubject x)) triples)
        dependency-map (map (fn [^Node x] (get-blanknode-dependency x subject-2-blanknode)) subjects)
        base (zipmap subjects dependency-map)
        dependencies (map-on-hash-map-vals #(assoc {} :dependencies %) base)
        ;assumption: when a backlog is created, the subject is assumed to be 'updated' 
        updated (map-on-hash-map-vals #(assoc % :updated true) dependencies)
        triples-added (add-triples updated subject-map)
        ;subjects without blank node dependencies are considered 'resolved'
        base-resolved (map-on-hash-map-vals #(if (empty? (:dependencies %))
                                               (assoc % :resolved true)
                                               %)
                                            triples-added)
        resolved (apply merge (map (fn [[k v]] (if (is-resolved? k base-resolved)
                                                 (hash-map k (assoc v :resolved true))
                                                 (hash-map k (assoc v :resolved false)))) base-resolved))]
    resolved))

(defn merge-updates
  "Given two backlog maps, m1 and m2, merge m2 into m1."
  [m1 m2]
  (loop [ks (keys m2)
         vs (vals m2)
         res m1]
    (if (empty? ks)
      res
      (recur (rest ks)
             (rest vs)
             (-> res
                 (assoc-in [(first ks) :updated] true)
                 (update-in [(first ks) :triples] #(concat % (:triples (first vs))))
                 (update-in [(first ks) :dependencies] #(distinct (concat % (:dependencies (first vs))))))))))


;TODO add proper doc string


(defn reset-key
  "Given a backlog map m and a (nested) key :updated or :resolved, set the key to false."
  [m k]
  (loop [ks (keys m)
         vs (vals m)
         res m]
    (if (empty? ks)
      res
      (recur (rest ks)
             (rest vs)
             (assoc-in res [(first ks) k] false)))))

;TODO add proper doc string
(defn update-key
  "Given a backlog map m, a key, and a function f,
    apply f to the value of the (nested) key k."
  [m k f]
  (loop [ks (keys m)
         vs (vals m)
         res m]
    (if (empty? ks)
      res
      (recur (rest ks)
             (rest vs)
             (assoc-in res [(first ks) k] (f (first ks) m))))))

;TODO add proper doc string
(defn set-update
  "Given a backlog map m, and a list of updated subjects,
    update m's :updated keys for all (dependent) subjects"
  [m updated]
  (loop [ks (keys m)
         vs (vals m)
         res m]
    (if (empty? ks)
      res
      (recur (rest ks)
             (rest vs)
             ;either the key itself is updated 
             ;or one of its dependencies
             (update-in res [(first ks) :updated] #(or %
                                                       (contains? updated (first ks))
                                                       (not-empty (set/intersection
                                                                   updated
                                                                   (set (get-in res [(first ks) :dependencies]))))))))))

;BUGFIX:
;updates for (blank) nodes need to be propagated
;both upwards and downwards blank node dependency chains.
;TODO: determine fixed-point for propagation
;(this is necessary if two subjects depend on the same blank node)
(defn trickle-down
  [m updated]
  (loop [to-update updated
         visited #{}
         res m]
    (if (empty? to-update)
      res
      (recur (concat (rest to-update)
                     (remove visited (:dependencies (get m (first to-update)))))
             (conj visited (first to-update))
             (assoc-in res [(first to-update) :updated] true)))))

;TODO add proper doc string
(defn update-backlog-map
  "Given a backlog map, and an update backlog map, return the corresponding updated backlog map."
  [old-backlog update-map]
  (let [reset-updates (reset-key old-backlog :updated)
        merged (merge-updates reset-updates update-map)
        reset-resolved (reset-key merged :resolved)
        ;updated-1 (update-key reset-resolved :updated updated?);TODO this is slow
        updated (set (map first (filter (fn [[k v]] (:updated v)) update-map)))
        updated-1 (set-update reset-resolved updated)
        updated (flatten (map first (filter (fn [[k v]] (:updated v)) updated-1)))
        updated-2 (trickle-down updated-1 updated)
        resolved (update-key updated-2 :resolved is-resolved?)]
    resolved))

(defn parse-window
  [^Iterator it ^long windowsize backlog]
  (if (empty? backlog)
    (let [new-triples (get-triples it windowsize)
          thin-triples (filter thin-triple? new-triples)
          thick-triples (remove thin-triple? new-triples)
          kept (build-backlog-map thick-triples)]
      [thin-triples kept '()])

    (let [[new-thin-triples new-thick-triples] (fetch-new-window it windowsize)

          ;setup data structures for previous window of triples
          new-map (build-backlog-map new-thick-triples)
          new-backlog (update-backlog-map backlog new-map)

          ;get resolved-no-updates (these will be returned)
          resolved-no-updates (filter (fn [[k v]] (and (:resolved v)
                                                       (not (:updated v)))) new-backlog)
          resolved-no-updates-triples (flatten (map #(:triples (second %)) resolved-no-updates))

          ;remove resolved-no-updates from backlog map
          not-kept (map first resolved-no-updates)
          kept (apply (partial dissoc new-backlog) not-kept)]
      [new-thin-triples kept resolved-no-updates-triples])))

(defn -main
  "Currently only used for manual testing."
  [& args]

  (let [^FileInputStream is (new FileInputStream (first args))
        ^Iterator it (RDFDataMgr/createIteratorTriples is Lang/RDFXML "base")
        windowsize 500]
    (time (loop [backlog '()]
            (when (.hasNext it)
              (let [[thin kept thick] (parse-window it windowsize backlog)
                    ttt (t2t/thin-2-thick thick)]
                (println ttt)
                (recur kept)))))))

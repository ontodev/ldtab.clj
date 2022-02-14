(ns ldtab.parse
  (:require [clojure.set :as s]
            [wiring.util.thickTriples :as util]
            [clojure.string :as string])
  (:import [org.apache.jena.rdf.model Model ModelFactory Resource]
           [org.apache.jena.riot.system StreamRDFBase]
           [java.io InputStream FileInputStream] 
           [org.apache.jena.graph NodeFactory Triple] 
           [org.apache.jena.riot RDFDataMgr Lang])) 

;TODO use configurable prefixes
(defn curify
  [triple]
  (let [owl (map #(string/replace % #"http://www.w3.org/2002/07/owl#" "owl:") triple) 
        rdf (map #(string/replace % #"http://www.w3.org/1999/02/22-rdf-syntax-ns#" "rdf:") owl) 
        rdfs (map #(string/replace % #"http://www.w3.org/2000/01/rdf-schema#" "rdfs:") rdf)]
    rdfs)) 

;used to make triples usable in wiring
(defn jena-triple-2-string
  [triple] 
     (let [subject (.getSubject triple)
           subject-rendering (if (.isBlank subject)
                                 (str "_:" (.getBlankNodeLabel subject))
                                 (.toString subject))
           predicate (.getPredicate triple)
           predicate-rendering (.toString predicate)
           object (.getObject triple)
           object-rendering (if (.isBlank object)
                                 (str "_:" (.getBlankNodeLabel object))
                                 (.toString object))
           triple [subject-rendering predicate-rendering object-rendering]          
           curiefied (curify triple)] 
       curiefied))

(defn get-root-subjects
  [triples]
  """Given a set of triples,
    return the set of root subjects w.r.t. blank node dependencies"""
  (let [subjects (distinct (map #(.getSubject %) triples)) 
        objects (set (filter #(.isBlank %) (map #(.getObject %) triples)))
        root (filter #(not (contains? objects %)) subjects)]
    root))

(defn get-triples
  [it n]
  """Given a stream of triples and a natural number n,
    return the next n triples from the stream."""
   (loop [x 0
          triples '()]
     (if (and (.hasNext it)
              (< x n))
       (recur (inc x) (conj triples (.next it)))
       triples))) 

(defn map-on-hash-map-vals
  [f m]
 (zipmap (keys m) (map f (vals m))))


(defn get-blanknode-dependency
  [subject subject-2-blanknode]
  """Given a subject and a (direct) map from subjects to blank node dependencies,
  return the transitive closure of blank node dependencies for the subject."""
  (let [direct (get subject-2-blanknode subject)
        ;TODO: You don't need to recompute this for all nodes - 
        ;you could work in a bottom-up fashion (however, this shouldn't speed things up too drastically)
        indirect (flatten (map #(get-blanknode-dependency % subject-2-blanknode) direct))]
    (into () (remove nil? (s/union direct indirect)))))


(defn get-blanknode-dependency-map
  [triples]
  """Given a set of triples,
    return a map from subjects to (direct) blank node dependencies."""
  (let [subject-map (group-by #(.getSubject %) triples) ;TODO this is somewhat expensive?
        subject-2-object (map-on-hash-map-vals (fn [x] (map (fn [y] (.getObject y)) x)) subject-map)
        subject-2-blanknode (map-on-hash-map-vals (fn [x] (filter (fn [y] (.isBlank y)) x)) subject-2-object)
        subjects (map #(.getSubject %) triples)
        dependency-map (map #(get-blanknode-dependency % subject-2-blanknode) subjects)]
    (zipmap subjects dependency-map)))

;check whether a subject has unresolved blank node dependencies in a set of triples
;a subject is resolved if all its blank node depenencies are resolved
;a bank node is resolved if it has an empty blank nod dependecy l ist
(defn resolved?-helper
  [subject triples dependency-map]
  (let [subject-dependency (get dependency-map subject)]
    (cond
      (and (.isBlank subject)
           (not (contains? dependency-map subject))) false
      (empty? subject-dependency) true
      :else (every? #(resolved?-helper % triples dependency-map) subject-dependency))))

(defn resolved?
  [subject triples] 
  """Given a subject and a set of triples,
    check whether this subject can be represented as a thick triple."""
  (let [dependency-map (get-blanknode-dependency-map triples)]
    ;NB: the helper function is used to avoid the recomputation of the dependency-map
    (resolved?-helper subject triples dependency-map))) 

(defn thin-triple?
  [triple]
  (let [s (.getSubject triple)
        o (.getObject triple)]
    (and (not (.isBlank s))
         (not (.isBlank o)))))

(defn occurs-in?
  [subject triples]
  """Given a subject and a set of triples,
    test whether there exists a triple in which the input subject occurs as a subject."""
  (let [subjects (set (map #(.getSubject %) triples))]
    (contains? subjects subject)))

(defn has-updates?
  [dependencies triples]
  """Given a set of blank nodes (dependencies),
    test whether they occur in the given set of triples."""
  (some #(occurs-in? % triples) dependencies))

(defn get-stanza
  [subject triples]
    (let [root (filter #(.equals (.getSubject %) subject) triples)
          objects (map #(.getObject %) root)
          dependencies (filter #(.isBlank %) objects)
          resolved (map #(get-stanza % triples) dependencies)
          res (reduce concat resolved)
          res (concat root res)]
      res))

(defn reflexive 
  [m]
  """Given a graph structure represented by a map,
    return the map so that it is reflexive.""" 
  (let [k (keys m)
        v (vals m)
        rv (map conj v k)]
    (zipmap k rv))) 

(defn fetch-new-window
  [it windowsize]
  (let [new-triples (get-triples it windowsize) ;extract windowsize many new triples
        thin-triples (filter thin-triple? new-triples)
        new-thick-triples (remove thin-triple? new-triples)]
    [new-triples thin-triples new-thick-triples])) 

(defn process-backlog
  [backlog-triples]
  (let [blank-node-dependency (get-blanknode-dependency-map backlog-triples);TODO: instead of recomputing this, it could be updated & maintained
        roots (get-root-subjects backlog-triples)
        resolved (set (filter #(resolved?-helper % backlog-triples blank-node-dependency) roots));TODO you don't have to recompute these every time
        not-resolved (set (filter #(not (resolved?-helper % backlog-triples blank-node-dependency)) roots))]
    [resolved not-resolved blank-node-dependency]))

;TODO: implement guard against ever growing window of backlog-triples
(defn parse-window
  [it windowsize backlog-triples]
  (if (empty? backlog-triples)
    (let [new-triples (get-triples it windowsize);triples in the working window 
          thin-triples (filter thin-triple? new-triples)
          thick-triples (remove thin-triple? new-triples)]
      [thin-triples thick-triples '()])

    (let [;(A) setup data structures for previous window of triples
          [resolved not-resolved backlog-blank-node-dependency] (process-backlog backlog-triples);TODO: speed this up? Is this possible? only by a factgor of 2 as far as I can see

          ;(B) fetch new window of triples
          [new-triples thin-triples new-thick-triples] (fetch-new-window it windowsize) 

          old-and-new-triples (concat backlog-triples new-triples)
          backlog-blank-node-dependency-r (reflexive backlog-blank-node-dependency)

          ;(C) check for updates of old data in new triples 

          ;TODO you may not want to collect stanzas here since they include thin triples that will need to be filtered out later
          ;(C-1) identify resolved subjects WITH NO updates (these will be returned)
          resolved-no-updates (set (filter #(not (has-updates? (get backlog-blank-node-dependency-r %) new-triples)) resolved)) 
          resolved-no-updates-triples (map #(get-stanza % backlog-triples) resolved-no-updates)

          ;(C-2) identify resolved subjects WITH updates
          ;note that their stanza needs to be collected from 'old-and-new-triples' 
          ;(these will be kept for the next call)
          resolved-with-updates (set (filter #(has-updates? (get backlog-blank-node-dependency-r %) new-triples) resolved)) 
          resolved-with-updates-triples (map #(get-stanza % old-and-new-triples) resolved-with-updates)

          ;(C-3) get updates for unresolved subjects (these will be kept for the next call)
          unresolved-subjects-triples (map #(get-stanza % old-and-new-triples) not-resolved) 

          ;TODO: thick-triples can contain duplicates - this may be avoided (should?)
          thick-triples (flatten (concat new-thick-triples resolved-with-updates-triples unresolved-subjects-triples))]

          ;TODO resolved-no-updates-triples contain thin triples - these could be filtered out
          ;(map #(filter (fn [x] (not (thin-triple? x))) %) resolved-no-updates-triples)
      [thin-triples (distinct thick-triples) resolved-no-updates-triples]))) 


(defn -main
  "Currently only used for manual testing."
  [& args] 

  ;testing
  (let [is (new FileInputStream (first args))
        it (RDFDataMgr/createIteratorTriples is Lang/RDFXML "base")
        windowsize 50]
    (loop [backlog '()]
      (when (.hasNext it) 
        (let [[thin kept thick] (parse-window it windowsize backlog)]
          (recur kept)))))

  ;takes about a second for obi
  ;(time (let [is (new FileInputStream (first args))
  ;      it (RDFDataMgr/createIteratorTriples is Lang/RDFXML "base")]
  ;  (loop [backlog '()]
  ;    (when (.hasNext it) 
  ;      (recur (.next it))))))

)




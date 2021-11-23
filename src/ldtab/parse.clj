(ns ldtab.parse
  (:require [clojure.set :as s])
  (:import [org.apache.jena.rdf.model Model ModelFactory Resource]
           [org.apache.jena.riot.system StreamRDFBase]
           [java.io InputStream FileInputStream] 
           [org.apache.jena.graph NodeFactory Triple] 
           [org.apache.jena.riot RDFDataMgr Lang])) 

(defn statement-to-string
  [statement] 
     (let [subject (.getSubject statement)
           subject-rendering (if (.isBlank (.asNode subject))
                                 (str "_:" (.getBlankNodeLabel (.asNode subject)))
                                 (.toString subject))
           predicate (.getPredicate statement)
           predicate-rendering (.toString predicate)
           object (.getObject statement)
           object-rendering (if (.isBlank (.asNode object))
                                 (str "_:" (.getBlankNodeLabel (.asNode object)))
                                 (.toString object))] 
       [subject-rendering predicate-rendering object-rendering]))

(defn get-blank-node-objects
  [statements]
  (let [objects (map #(.getObject %) statements)
        blanknodes (filter #(.isBlank (.asNode %)) objects)]
    (distinct blanknodes)) )

(defn get-stanza
  [subject model]
    (let [triples (.listStatements model subject nil nil) 
          tripleList (seq (.toList triples))
          blank-nodes (get-blank-node-objects tripleList) 
          blank-node-dependencies (map #(get-stanza % model) blank-nodes)
          res (reduce concat blank-node-dependencies)
          res (concat tripleList res)]
      res))

;from a model
(defn get-root-subjects
  [model]
  (let [subjects (set (seq (.toList (.listSubjects model))))
        objects (set (filter #(.isBlank (.asNode %)) (seq (.toList (.listObjects model)))))
        root (filter #(not (contains? objects %)) subjects)]
    root))

(defn as-thin-triples-streamed
  [input]
  (let [in (RDFDataMgr/open input)
        model (time (.read (ModelFactory/createDefaultModel) in ""))
        root-subjects (time (get-root-subjects model))
        subject-stanzas (time (map #(get-stanza % model) root-subjects))]
   subject-stanzas))

;NB: don't use this
;this implementation is just a reference for testing/validation purposes
(defn as-thin-triples
  [input] 
   (def in (RDFDataMgr/open input))

   (def model (ModelFactory/createDefaultModel)) 
   (.read model in "") 

   (def iter (.listStatements model)) 
   (def res '())
   (while (.hasNext iter)
     (let [statement (.nextStatement iter)
           subject (.getSubject statement)
           subject-rendering (if (.isBlank (.asNode subject))
                                 (str "_:" (.getBlankNodeLabel (.asNode subject)))
                                 (.toString subject))
           predicate (.getPredicate statement)
           predicate-rendering (.toString predicate)
           object (.getObject statement)
           object-rendering (if (.isBlank (.asNode object))
                                 (str "_:" (.getBlankNodeLabel (.asNode object)))
                                 (.toString object))]
       (def res (conj res [subject-rendering predicate-rendering object-rendering]))))
   res) 


;=============================================
;=============================================
;=============================================
;=============================================

;get a list of triples - resolve dependencies
;buld map (root) subject to blank node dependency

(defn get-root-subjects-from-triples
  [triples]
  (let [subjects (distinct (map #(.getSubject %) triples)) 
        objects (set (filter #(.isBlank %) (map #(.getObject %) triples)))
        root (filter #(not (contains? objects %)) subjects)]
    root))

(defn get-triples
  [it n]
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
  (let [direct (get subject-2-blanknode subject)
        indirect (flatten (map #(get-blanknode-dependency % subject-2-blanknode) direct))]
    (into () (remove nil? (s/union direct indirect)))))


;TODO:  filter out nils?
(defn get-blanknode-dependency-map
  [triples]
  (let [subject-map (group-by #(.getSubject %) triples)
        subject-2-object (map-on-hash-map-vals (fn [x] (map (fn [y] (.getObject y)) x)) subject-map)
        subject-2-blanknode (map-on-hash-map-vals (fn [x] (filter (fn [y] (.isBlank y)) x)) subject-2-object)
        subjects (map #(.getSubject %) triples)
        dependency-map (map #(get-blanknode-dependency % subject-2-blanknode) subjects)]
    (zipmap subjects dependency-map)))

;TODO: test that this works
;check whether a subject has unresolved blank node dependencies in a set of triples
;a subject is resolved if all its blank node depenencies are resolved
;a bank node is resolved if it has an empty blank nod dependecy l ist
(defn resolved?
  [subject triples] 
  (let [dependency-map (get-blanknode-dependency-map triples)
        subject-dependency (get dependency-map subject)]
    (cond
      (and (.isBlank subject)
           (not (contains? dependency-map subject))) false
      (empty? subject-dependency) true
      :else (every? #(resolved? % triples) subject-dependency))))

(defn thin-triple?
  [triple]
  (let [s (.getSubject triple)
        o (.getObject triple)]
    (and (not (.isBlank s))
         (not (.isBlank o)))))

(defn occurs-in?
  [subject triples]
  (let [subjects (set (map #(.getSubject %) triples))]
    (contains? subjects subject)))

(defn has-updates?
  [dependencies triples]
  (some #(occurs-in? % triples) dependencies))

(defn get-thick-triple
  [subject triples]
  (filter #(and (.equals (.getSubject %) subject)
                (.isBlank (.getObject %))) triples) )


(defn get-stanza-triples
  [subject triples]
    (let [root (filter #(.equals (.getSubject %) subject) triples)
          objects (map #(.getObject %) root)
          dependencies (filter #(.isBlank %) objects)
          resolved (map #(get-stanza-triples % triples) dependencies)
          res (reduce concat resolved)
          res (concat root res)]
      res))

(defn reflexive 
  [m]
  (let [k (keys m)
        v (vals m)
        rv (map conj v k)]
    (zipmap k rv))) 


;TODO refactor this
;TODO test this
(defn parse-window
  [it windowsize backlog-triples]
  (if (empty? backlog-triples)
    (let [new-triples (get-triples it windowsize);triples in the working window 
          thin-triples (filter thin-triple? new-triples)
          thick-triples (filter #(not (thin-triple? %)) new-triples)]
      [thin-triples thick-triples '()])

    (let [;(A) setup data structures for previous window of triples
          old-blank-node-dependency (get-blanknode-dependency-map backlog-triples)
          old-blank-node-dependency-r (reflexive old-blank-node-dependency) 
          old-triples-roots (get-root-subjects-from-triples backlog-triples)
          resolved-subjects-old (set (filter #(resolved? % backlog-triples) old-triples-roots))
          not-resolved-subjects-old (set (filter #(not (resolved? % backlog-triples)) old-triples-roots))
          
          ;(B) fetch new data
          new-triples (get-triples it windowsize) ;extract windowsize many new triples ; 
          thin-triples (filter thin-triple? new-triples) ;get thin-triples which are to be returned
          old-and-new-triples (concat backlog-triples new-triples)
          new-thick-triples (into () (s/difference (set new-triples) (set thin-triples))); (these will also be kept)

          ;(C) check for updates of old data in new triples 

          ;(C-1) identify resolved subjects WITH NO updates (these will be returned)
          resolved-subjects-no-updates (set (filter #(not (has-updates? (get old-blank-node-dependency-r %) new-triples)) resolved-subjects-old)) 
          resolved-subjects-no-updates-triples (map #(get-stanza-triples % backlog-triples) resolved-subjects-no-updates)

          ;(C-2) identify resolved subjects WITH updates (these will be kept for the next call)
          resolved-subjects-with-updates (set (filter #(has-updates? (get old-blank-node-dependency-r %) new-triples) resolved-subjects-old)) 
          resolved-subjects-with-updates-triples (map #(get-stanza-triples % old-and-new-triples) resolved-subjects-with-updates)

          ;(C-3) get updates for unresolved subjects (these will be kept for the next call)
          unresolved-subjects-triples (map #(get-stanza-triples % old-and-new-triples) not-resolved-subjects-old) 
          thick-triples (flatten (concat new-thick-triples resolved-subjects-with-updates-triples unresolved-subjects-triples))] 

      [thin-triples thick-triples resolved-subjects-no-updates-triples])))


;THIS DOESN'T work because triples are not in the correct order
(defn extract-thick-triple
  [it blanknodes acc]
    (if (.hasNext it)
      (let [triple (.next it)
            subject (.getSubject triple)
            object  (.getObject triple)]
        (cond 
          (and (not (.isBlank subject))
               (.isBlank object)
               (empty? blanknodes)) (extract-thick-triple it (conj blanknodes object) (conj acc triple))
          (and (.isBlank subject)
               (.isBlank object)
               (contains? blanknodes subject)) (extract-thick-triple it (conj blanknodes object) (conj acc triple))
          :else [acc triple]))))

(defn extend-stanza
  [it triple acc]
    (let [stanza-subject (.getSubject triple)]
      (if (.hasNext it)
        (let [n (.next it)]
          (if (.equals (.getSubject n) stanza-subject) 
            (extend-stanza it n (conj acc n))
            [it acc triple])))))

(defn print-process 
  [x]
  (println (count (first x)))
  (println (count (second x)))
  (println (count (nth x 2))) 
  (println "Thick kept")
  (run! println (second x))
  (println "Thick returned")
  (run! println (nth x 2))
  (println ""))

(defn -main
  "Currently only used for manual testing."
  [& args] 

  (def is (new FileInputStream (first args) )) 
  (def it (RDFDataMgr/createIteratorTriples is Lang/RDFXML "bas"))
  ;(def it (RDFDataMgr/createIteratorTriples is Lang/NT "bas")) ;use window of size 10

  (def a (parse-window it 100 '()))
  (print-process a) 
  (def aa (parse-window it 100 (second a)))
  (print-process aa) 


   ;(def stanzas (as-thin-triples-streamed (first args)))
   ;(run! println (map #(map statement-to-string %) stanzas)) 

   ;(println (count (as-thin-triples (first args))))
   ;(println (reduce + (map count (as-thin-triples-streamed (first args)))))
   ;(println (count (seq (.toList (.listStatements model)))))

   ;(println (filter #(not (= 3 (count %))) (as-thin-triples (first args)))) 

   ;(def model (ModelFactory/createDefaultModel)) 
   ;(.read model in "") 
   ;(.write model System/out) 
)




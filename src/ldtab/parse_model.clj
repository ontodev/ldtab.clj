(ns ldtab.parse-model
  (:require [clojure.set :as s])
  (:import [org.apache.jena.rdf.model ModelFactory]
           [org.apache.jena.riot RDFDataMgr]))

(defn get-blank-node-objects
  [statements]
  (let [objects (map #(.getObject %) statements)
        blanknodes (filter #(.isBlank (.asNode %)) objects)]
    (distinct blanknodes)))

(defn get-stanza
  [subject model]
    (let [triples (.listStatements model subject nil nil) 
          tripleList (seq (.toList triples))
          blank-nodes (get-blank-node-objects tripleList) 
          blank-node-dependencies (map #(get-stanza % model) blank-nodes)
          res (reduce concat blank-node-dependencies)
          res (concat tripleList res)]
      res))

(defn get-root-subjects
  [model]
  (let [subjects (set (seq (.toList (.listSubjects model))))
        objects (set (filter #(.isBlank (.asNode %)) (seq (.toList (.listObjects model)))))
        root (filter #(not (contains? objects %)) subjects)]
    root))

(defn group-thin-triple-dependencies
  [input]
  (let [in (RDFDataMgr/open input)
        model (.read (ModelFactory/createDefaultModel) in "")
        root-subjects (get-root-subjects model)
        subject-stanzas (map #(get-stanza % model) root-subjects)
        subject-stanza-triples (map #(map (fn [x] (.asTriple x)) %) subject-stanzas)]
   subject-stanza-triples))

;=======
;=======
;=======
;NB: don't use this
;this implementation is just a reference for testing/validation purposes
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

(defn as-thin-triples-string
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

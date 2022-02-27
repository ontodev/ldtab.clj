(ns ldtab.parse-model
  (:require [clojure.set :as s])
  (:import [org.apache.jena.rdf.model ModelFactory]
           [org.apache.jena.riot RDFDataMgr]))

(defn get-blank-node-objects
  [statements]
  (let [objects (map #(.getObject %) statements)
        blanknodes (filter #(.isBlank (.asNode %)) objects)]
    (distinct blanknodes)))

(defn get-blanknode-dependencies
  "Given a subject in an RDF model,
  return all statements with information about this subject.";(i.e., a stanza)
  [subject model]
    (let [triples (.listStatements model subject nil nil) 
          tripleList (seq (.toList triples))
          blank-nodes (get-blank-node-objects tripleList) 
          blank-node-dependencies (map #(get-blanknode-dependencies % model) blank-nodes)
          res (reduce concat blank-node-dependencies)
          res (concat tripleList res)]
      res))

(defn get-root-subjects
  "Given an RDF mode, return all subjects that are at the root
  of a (possibly empty) blanknode dependency chain."
  [model]
  (let [subjects (set (seq (.toList (.listSubjects model))))
        objects (seq (.toList (.listObjects model))) 
        blanknode-objects (set (filter #(.isBlank (.asNode %)) objects))
        roots (filter #(not (contains? blanknode-objects %)) subjects)]
    roots))

(defn group-thin-triple-dependencies
  "Given an RDF graph, group triples according to information about subjects."
  [input]
  (let [in (RDFDataMgr/open input)
        model (.read (ModelFactory/createDefaultModel) in "")
        root-subjects (get-root-subjects model)
        subject-with-dependencies (map #(get-blanknode-dependencies % model) root-subjects)
        dependency-triples (map #(map (fn [x] (.asTriple x)) %) subject-with-dependencies)]
   dependency-triples))

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

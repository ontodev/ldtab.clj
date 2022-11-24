(ns ldtab.rdf-model
  (:require [clojure.set :as set])
  (:import [org.apache.jena.rdf.model ModelFactory Model StmtIterator Resource Property RDFNode Statement]
           [org.apache.jena.riot RDFDataMgr]
           [org.apache.jena.atlas.web TypedInputStream]))

(defn get-blank-node-objects
  [statements]
  (let [objects (map (fn [^Statement x] (.getObject x)) statements)
        blanknodes (filter (fn [^RDFNode x] (.isBlank (.asNode x))) objects)]
    (distinct blanknodes)))

(defn get-blanknode-dependencies
  "Given a subject S in a model of an RDF graph G,
  return all triples (s,p,o) in G where
  (a) s = S, or
  (b) s is a blank node
      and there exists a path (S,p_1,o_1),...,(s_n,p_n,s),(s,p,o)" 
  [^Resource subject ^Model model]
    (let [^Property prop nil
          ^RDFNode node nil
          ^StmtIterator triples (.listStatements model subject prop node) 
          tripleList (seq (.toList triples))
          blank-nodes (get-blank-node-objects tripleList) 
          blank-node-dependencies (map #(get-blanknode-dependencies % model) blank-nodes)
          res (reduce concat blank-node-dependencies)
          res (concat tripleList res)]
      res))

(defn get-root-subjects
  "Given a model of an RDF graph, return all subjects that are 
  (a) not a blank node, or 
  (b) a blank node that does not occur as an object in any triple of the given RDF model." 
  [^Model model]
  (let [subjects (set (seq (.toList (.listSubjects model))))
        objects (seq (.toList (.listObjects model))) 
        blanknode-objects (set (filter (fn [^RDFNode x] (.isBlank (.asNode x))) objects))
        roots (filter (fn [^Resource x] (not (contains? blanknode-objects x))) subjects)]
    roots))

(defn group-blank-node-paths
  "Given an RDF graph, group triples w.r.t. bank node paths
  (a blank node path is a path (s_1,p_1,o_1),...,(s_n,p_n,o_n) where
 o_i = s_{i+1} are blank nodes for 1 <= i <= n."
  [^String input]
  (let [^TypedInputStream in (RDFDataMgr/open input)
        ^Model model (.read (ModelFactory/createDefaultModel) in "")
        root-subjects (get-root-subjects model)
        subject-with-dependencies (map #(get-blanknode-dependencies % model) root-subjects)
        dependency-triples (map #(map (fn [^Statement x] (.asTriple x)) %) subject-with-dependencies)]
   dependency-triples))

(defn group-blank-node-paths-n-triples
  "Given an RDF graph, group triples w.r.t. bank node paths
  (a blank node path is a path (s_1,p_1,o_1),...,(s_n,p_n,o_n) where
 o_i = s_{i+1} are blank nodes for 1 <= i <= n."
  [input]
  (let [in (RDFDataMgr/open input)
        model (.read (ModelFactory/createDefaultModel) in "" "NTRIPLES")
        root-subjects (get-root-subjects model)
        subject-with-dependencies (map (fn [x] (get-blanknode-dependencies x model)) root-subjects)
        dependency-triples (map #(map (fn [^Statement x] (.asTriple x)) %) subject-with-dependencies)]
   dependency-triples))

(defn group-blank-node-paths-turtle
  "Given an RDF graph, group triples w.r.t. bank node paths
  (a blank node path is a path (s_1,p_1,o_1),...,(s_n,p_n,o_n) where
 o_i = s_{i+1} are blank nodes for 1 <= i <= n."
  [input]
  (let [in (RDFDataMgr/open input)
        model (.read (ModelFactory/createDefaultModel) in "" )
        root-subjects (get-root-subjects model)
        subject-with-dependencies (map #(get-blanknode-dependencies % model) root-subjects)
        dependency-triples (map #(map (fn [^Statement x] (.asTriple x)) %) subject-with-dependencies)]
   dependency-triples))


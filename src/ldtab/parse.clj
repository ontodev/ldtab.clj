(ns ldtab.parse
  (:import [org.apache.jena.rdf.model Model ModelFactory Resource]
           [org.apache.jena.riot RDFDataMgr])) 

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

(defn get-root-subjects
  [model]
  (let [subjects (set (seq (.toList (.listSubjects model))))
        objects (set (filter #(.isBlank (.asNode %)) (seq (.toList (.listObjects model)))))
        root (filter #(not (contains? objects %)) subjects)]
    root))

(defn as-thin-triples-streamed
  [input]
  (let [in (RDFDataMgr/open input)
        model (.read (ModelFactory/createDefaultModel) in "")
        root-subjects (get-root-subjects model)
        subject-stanzas (map #(get-stanza % model) root-subjects)]
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

(defn -main
  "Currently only used for manual testing."
  [& args] 

   (def stanzas (as-thin-triples-streamed (first args)))
   ;(run! println (map #(map statement-to-string %) stanzas)) 

   ;(println (count (as-thin-triples (first args))))
   ;(println (reduce + (map count (as-thin-triples-streamed (first args)))))
   ;(println (count (seq (.toList (.listStatements model)))))

   ;(println (filter #(not (= 3 (count %))) (as-thin-triples (first args)))) 

   ;(def model (ModelFactory/createDefaultModel)) 
   ;(.read model in "") 
   ;(.write model System/out) 
)




(ns ldtab.gci-handling
  (:require [cheshire.core :as cs])
  (:gen-class))

;TODO: make predicate-map keys consistent (either use strings or keys - but not both)

(defn is-compound-class-expression
  [predicate-map]
  (and (map? predicate-map)
       (or (contains? predicate-map "owl:intersectionOf")
           (contains? predicate-map "owl:someValuesFrom")
           (contains? predicate-map "owl:allValuesFrom")
           (contains? predicate-map "owl:unionOf")
           (contains? predicate-map "owl:complementOf")
           (contains? predicate-map "owl:oneOf")
           (contains? predicate-map "owl:hasValue")
           (contains? predicate-map "owl:hasSelf")
           (contains? predicate-map "owl:minCardinality")
           (contains? predicate-map "owl:minQualifiedCardinality")
           (contains? predicate-map "owl:maxCardinality")
           (contains? predicate-map "owl:maxQualifiedCardinality")
           (contains? predicate-map "owl:cardinality"))))

(defn encode-raw-gci-without-annotation
  [raw-triple]
  raw-triple)

;TODO: owl:equivalentClass
(defn is-raw-gci-without-annotation
  [raw-triple]
  (let [object (:object raw-triple)]
      (and (map? object)
           (contains? object "rdfs:subClassOf")
           (is-compound-class-expression object)))) 

(defn encode-raw-gci-without-annotation
  [raw-triple];predicate map or triple?
  (println "encode")
  (let [object (:object raw-triple)
        subclass (dissoc object "rdfs:subClassOf")
        superclass (first (get object "rdfs:subClassOf"));TODO handle longer lists
        ;superclass (first (:object example)) 
        datatype (:datatype superclass)]
    {:subject subclass
     :predicate "rdfs:subClassOf"
     :object (:object superclass)
     :datatype datatype}))



(defn encode-annotated-gci-raw
  [raw-triple]
  ;check that both target and source are maps and property is "subclassOf / equivalent" 
  )

(defn encode-raw-gci-map
  [raw-triple]
  (if (is-raw-gci-without-annotation raw-triple)
    (encode-raw-gci-without-annotation raw-triple)
    raw-triple))

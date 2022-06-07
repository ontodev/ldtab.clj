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

;TODO: owl:equivalentClass
(defn is-raw-gci-without-annotation
  [raw-triple]
  (let [object (:object raw-triple)]
      (and (map? object)
           (contains? object "rdfs:subClassOf")
           (is-compound-class-expression object)))) 

(defn is-raw-gci-with-annotation
  [raw-triple]
  (let [object (:object raw-triple)]
    (and (map? object)
         (contains? object "owl:annotatedSource")
         (contains? object "owl:annotatedTarget")
         (contains? object "owl:annotatedProperty")
         (= (:object (first (get object "owl:annotatedProperty"))) "rdfs:subClassOf")
         (is-compound-class-expression (:object (first (get object "owl:annotatedSource")))))))

(defn encode-raw-gci-without-annotation
  [raw-triple];predicate map or triple?
  (let [object (:object raw-triple)
        subclass (dissoc object "rdfs:subClassOf")
        superclass (first (get object "rdfs:subClassOf"));TODO handle longer lists
        datatype (:datatype superclass)
        superclass (:object superclass)]
    {:subject subclass
     :predicate "rdfs:subClassOf"
     :object superclass
     :datatype datatype}))

(defn encode-raw-gci-with-annotation
  [raw-triple]
  (let [object (:object raw-triple)
        source (first (get object "owl:annotatedSource")) 
        subclass (dissoc (:object source) "rdfs:subClassOf")
        superclass (first (get object "owl:annotatedTarget")) ;done 
        datatype (:datatype superclass)
        superclass (:object superclass)
        annotation (dissoc object "owl:annotatedSource")
        annotation (dissoc annotation "owl:annotatedProperty")
        annotation (dissoc annotation "owl:annotatedTarget") ]
    {:subject subclass
     :predicate "rdfs:subClassOf"
     :object superclass
     :datatype datatype
     :annotation annotation}))



(defn encode-raw-gci-map
  [raw-triple]
  (cond (is-raw-gci-without-annotation raw-triple)
        (encode-raw-gci-without-annotation raw-triple)
        (is-raw-gci-with-annotation raw-triple)
        (encode-raw-gci-with-annotation raw-triple)
        :else raw-triple))

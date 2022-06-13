(ns ldtab.gci-handling
  (:require [cheshire.core :as cs] 
            [ldtab.annotation-handling :as ann])
  (:gen-class))

;TODO: make predicate-map keys consistent (either use strings or keys - but not both)

(defn encode-annotation-type
  [m v]
  (loop [ks (keys m)
         res m] 
    (if (empty? ks)
      res
      (recur (rest ks) 
             (update res (first ks) 
          #(vec (map (fn [x] (assoc x :meta v)) %)))))))

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

(defn is-raw-gci-without-annotation
  [raw-triple property]
  (let [object (:object raw-triple)]
      (and (map? object)
           (contains? object property)
           (is-compound-class-expression object)))) 

(defn is-raw-gci-with-annotation
  [raw-triple property]
  (let [object (:object raw-triple)]
    (and (map? object)
         (contains? object "owl:annotatedSource")
         (contains? object "owl:annotatedTarget")
         (contains? object "owl:annotatedProperty")
         (= (:object (first (get object "owl:annotatedProperty"))) property)
         (is-compound-class-expression (:object (first (get object "owl:annotatedSource")))))))

(defn encode-raw-gci-without-annotation
  [raw-triple property];predicate map or triple?
  (let [object (:object raw-triple)
        subclass (dissoc object property)
        superclass (first (get object property));TODO handle longer lists
        datatype (:datatype superclass)
        superclass (:object superclass)]
    {:subject subclass
     :predicate property
     :object superclass
     :datatype datatype}))

(defn encode-raw-gci-with-annotation
  [raw-triple property]
  (let [object (:object raw-triple)
        source (first (get object "owl:annotatedSource")) 
        subclass (dissoc (:object source) property)
        superclass (first (get object "owl:annotatedTarget")) ;done 
        datatype (:datatype superclass)
        superclass (:object superclass)
        annotation (dissoc object "owl:annotatedSource")
        annotation (dissoc annotation "owl:annotatedProperty")
        annotation (dissoc annotation "owl:annotatedTarget")
        rdf-type (:object (first (get annotation "rdf:type")))
        annotation (dissoc annotation "rdf:type")
        annotation (encode-annotation-type annotation rdf-type)
        ;TODO handle recursive annotations
        ;ann (ann/encode-raw-annotation-map-base object {})
        ]
    {:subject subclass
     :predicate property
     :object superclass
     :datatype datatype
     :annotation annotation}))

(defn encode-raw-gci-map
  [raw-triple]
  (cond (is-raw-gci-without-annotation raw-triple "rdfs:subClassOf")
        (encode-raw-gci-without-annotation raw-triple "rdfs:subClassOf")
        (is-raw-gci-without-annotation raw-triple "owl:equivalentClass")
        (encode-raw-gci-without-annotation raw-triple "owl:equivalentClass")
        (is-raw-gci-with-annotation raw-triple "rdfs:subClassOf")
        (encode-raw-gci-with-annotation raw-triple "rdfs:subClassOf") 
        (is-raw-gci-with-annotation raw-triple "owl:equivalentClass")
        (encode-raw-gci-with-annotation raw-triple "owl:equivalentClass")
        :else raw-triple))

(ns ldtab.annotation-handling
  (:require [cheshire.core :as cs])
  (:gen-class))

;TODO CURIEs
(defn is-annotation-map?
  [predicate-map]
  (and
    (contains? predicate-map "owl:annotatedSource")
    (contains? predicate-map "owl:annotatedProperty")
    (contains? predicate-map "owl:annotatedTarget"))) 

;NB this is something specific to wiring
(defn is-annotation-triple?
  [thick-triple]
  (let [predicate (:predicate thick-triple)]
    (= predicate "owl:Axiom")))

(defn is-owl-property?
  [property]
  (cond
    (= property "owl:annotatedSource") true
    (= property "owl:annotatedProperty") true
    (= property "owl:annotatedTarget") true
    (= property "rdf:type") true
    (= property "annotation") true ;NB this is something specific to thick triples
    :else false))

(defn update-annotation-map
  [annotation-map previous-annotation]
  (let [annotated-property (get previous-annotation "owl:annotatedProperty")
        annotated-object (get previous-annotation "owl:annotatedTarget")]
    (update annotation-map annotated-property #(vec (map 
                                                          (fn [x] 
                                                            (if (= (:object x) annotated-object)
                                                              (assoc x :annotation (:annotation previous-annotation))
                                                              x)) 
                                                          %))))) 

(defn encode-raw-annotation-map-base
  [predicate-map previous-annotation]
  (let [subject (:object (first (get predicate-map "owl:annotatedSource")))
        predicate (:object (first (get predicate-map "owl:annotatedProperty")))
        object (:object (first (get predicate-map "owl:annotatedTarget"))) 
        datatype (:datatype (first (get predicate-map "owl:annotatedTarget")))

        annotation-properties (remove is-owl-property? (keys predicate-map))
        annotation-objects (map #(get predicate-map %) annotation-properties) 
        annotation-objects (map #(map (fn [x] (assoc x :meta "owl:Annotation")) %) annotation-objects) 
        annotation-map (zipmap annotation-properties annotation-objects)] 

    (if (not-empty previous-annotation)
      {:object object,
       :predicate predicate,
       :subject subject,
       :datatype datatype,
       :annotation (update-annotation-map annotation-map previous-annotation)} 
      {:object object,
       :predicate predicate,
       :subject subject,
       :datatype datatype,
       :annotation annotation-map}))) 

(declare encode-raw-annotation-map)

(defn encode-raw-annotation-map-recursion
  [predicate-map previous-annotation]
  (let [subject (:object (first (get predicate-map "owl:annotatedSource")))
        predicate (:object (first (get predicate-map "owl:annotatedProperty")))
        object (:object (first (get predicate-map "owl:annotatedTarget")))

        annotation-properties (remove is-owl-property? (keys predicate-map))
        annotation-objects (map #(get predicate-map %) annotation-properties) 
        annotation-objects (map #(map (fn [x] (assoc x :meta "owl:Annotation")) %) annotation-objects) 
        annotation-map (zipmap annotation-properties annotation-objects)
        updated-annotation (update-annotation-map annotation-map previous-annotation)] 

    (if (empty? previous-annotation)
      (encode-raw-annotation-map subject {:annotation annotation-map
                                          "owl:annotatedProperty" predicate
                                          "owl:annotatedTarget" object}) 
      (encode-raw-annotation-map subject
                                 {:annotation updated-annotation
                                  "owl:annotatedProperty" predicate
                                  "owl:annotatedTarget" object})
      )))

(defn encode-raw-reification-map-base
  [predicate-map previous-annotation]
  (let [subject (:object (first (get predicate-map "rdf:subject")))
        predicate (:object (first (get predicate-map "rdf:predicate")))
        object (:object (first (get predicate-map "rdf:object"))) 

        annotation-properties (remove is-owl-property? (keys predicate-map))
        annotation-objects (map #(get predicate-map %) annotation-properties) 
        annotation-objects (map #(map (fn [x] (assoc x :meta "rdf:Reification")) %) annotation-objects) 
        annotation-map (zipmap annotation-properties annotation-objects)] 

    (if (not-empty previous-annotation)
      {:object object,
       :predicate predicate,
       :subject subject,
       :annotation (update-annotation-map annotation-map previous-annotation)} 
      {:object object,
       :predicate predicate,
       :subject subject,
       :annotation annotation-map})))

(defn encode-raw-reification-map-recursion
  [predicate-map previous-annotation]
  (let [subject (:object (first (get predicate-map "rdf:subject")))
        predicate (:object (first (get predicate-map "rdf:predicate")))
        object (:object (first (get predicate-map "rdf:object")))

        annotation-properties (remove is-owl-property? (keys predicate-map))
        annotation-objects (map #(get predicate-map %) annotation-properties) 
        annotation-objects (map #(map (fn [x] (assoc x :meta "rdf:Reification")) %) annotation-objects) 
        annotation-map (zipmap annotation-properties annotation-objects)
        updated-annotation (update-annotation-map annotation-map previous-annotation)] 

    (if (empty? previous-annotation)
      (encode-raw-annotation-map subject {:annotation annotation-map
                                          "rdf:predicate" predicate
                                          "rdf:object" object}) 
      (encode-raw-annotation-map subject
                                 {:annotation updated-annotation
                                  "rdf:predicate" predicate
                                  "rdf:object" object})
      )))

(defn encode-raw-annotation-map
  ([predicate-map]
   """Given a raw predicate map,
     test whether it encodes an OWL annotation or RDF reification.
     If so, transform it into a thick triple.

     Example:

     The raw thick triple of an OWL annotation

     {:subject wiring:blanknode:G__1130,
      :predicate owl:Axiom,
      :object {obo:IAO_0010000 [{:object obo:050-003}],
               owl:annotatedTarget [{:object \"literal\"}],
               owl:annotatedProperty [{:object obo:IAO_0000602}],
               owl:annotatedSource [{:object obo:BFO_0000020}],
               rdf:type [{:object owl:Axiom}]}}

      is transformed into the thick triple

      {subject obo:BFO_0000020,
       predicate obo:IAO_0000602,
       object \"literal\",
       annotation {obo:IAO_0010000 [{object obo:050-003}]}} 
     """
   (encode-raw-annotation-map predicate-map {}))
  ([predicate-map previous-annotation]
   """Given a predicate map, recursively translate raw thick triple annotations
     as described above."""
  (let [owl-annototation (:object (first (get predicate-map "owl:annotatedSource")))
        rdf-reification (:object (first (get predicate-map "rdf:subject")))]
    (cond owl-annototation (if (map? owl-annototation) 
                             (encode-raw-annotation-map-recursion predicate-map previous-annotation)
                             (encode-raw-annotation-map-base predicate-map previous-annotation))
          rdf-reification (if (map? rdf-reification)
                            (encode-raw-reification-map-recursion predicate-map previous-annotation)
                            (encode-raw-reification-map-base predicate-map previous-annotation))
                            )))) 

(defn get-annotated-triple
  [annotation]
  (let [predicate-map (:object annotation)
        subject (:object (first (get predicate-map "owl:annotatedSource")))
        predicate (:object (first (get predicate-map "owl:annotatedProperty")))
        object (:object (first (get predicate-map "owl:annotatedTarget")))]
    {:object object, :predicate predicate, :subject subject})) 

(defn get-annotations-for-assertions
  [thick-triples]
  """Given a set of thick triples,
    return anntations for asserted statements.""" 
  (let [triple-set (set thick-triples)
        annotations (filter is-annotation-triple? thick-triples) 
        annotated-triples-contained (filter #(contains? triple-set (get-annotated-triple %)) annotations)]
    annotated-triples-contained))


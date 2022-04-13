(ns ldtab.thick2thin
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [ldtab.annotation-handling :as ann]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]
            [cheshire.core :as cs])
  (:import [org.apache.jena.graph NodeFactory Triple]
           [org.apache.jena.rdf.model Model ModelFactory Resource]
           [org.apache.jena.riot RDFDataMgr RDFFormat Lang]
           [org.apache.jena.riot.system StreamRDFWriter StreamRDFOps]
           [org.apache.jena.graph NodeFactory])
  (:gen-class))

(defn curry-predicate-map
  [predicateMap]
  (fn [x] (-> predicateMap
              (get x)
              first
              :object)))

(declare translate)
(declare translate-property)

(defn curie-2-uri
  [curie prefix-2-base]
  (let [split (str/split curie #":")
        prefix (first split)
        local (second split)
        base (get prefix-2-base prefix)]
    (if base
      (str base local)
      (if (and (str/starts-with? curie "<")
               (str/ends-with? curie ">"))
        (subs curie 1 (- (count curie) 1));remove enclosing < >
        curie))))
      ;curie)))

(defn translate-some
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        filler (translate (get-object :owl:someValuesFrom) prefix-2-base model)

        owl-on-property (curie-2-uri "owl:onProperty" prefix-2-base)
        owl-some-values (curie-2-uri "owl:someValuesFrom" prefix-2-base)
        owl-restriction (curie-2-uri "owl:Restriction" prefix-2-base) 
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        on-property (.createProperty model owl-on-property)
        some-values (.createProperty model owl-some-values) 
        rdf-type (.createProperty model rdf-type)

        restriction (.createResource model owl-restriction)

        ;bnode (NodeFactory/createBlankNode)]
        bnode (.createResource model)]

    (.add model bnode some-values filler)
    (.add model bnode on-property property)
    (.add model bnode rdf-type restriction)

    bnode))

(defn translate-all
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        filler (translate (get-object :owl:allValuesFrom) prefix-2-base model)

        owl-on-property (curie-2-uri "owl:onProperty" prefix-2-base)
        owl-all-values (curie-2-uri "owl:allValuesFrom" prefix-2-base)
        owl-restriction (curie-2-uri "owl:Restriction" prefix-2-base) 
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        on-property (.createProperty model owl-on-property)
        some-values (.createProperty model owl-all-values) 
        rdf-type (.createProperty model rdf-type)

        restriction (.createResource model owl-restriction) 
        bnode (.createResource model)]

    (.add model bnode some-values filler)
    (.add model bnode on-property property)
    (.add model bnode rdf-type restriction)

    bnode))

(defn translate-min-cardinality 
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        cardinality (get-object :owl:minCardinality)
        
        owl-on-property (curie-2-uri "owl:onProperty" prefix-2-base)
        owl-min-cardinality (curie-2-uri "owl:minCardinality" prefix-2-base)
        owl-restriction (curie-2-uri "owl:Restriction" prefix-2-base) 
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        on-property (.createProperty model owl-on-property)
        min-cardinality (.createProperty model owl-min-cardinality)
        rdf-type (.createProperty model rdf-type)
        cardinality (.createTypedLiteral model cardinality (curie-2-uri "xsd:nonNegativeInteger" prefix-2-base)) 
        
        restriction (.createResource model owl-restriction)

        ;bnode (NodeFactory/createBlankNode)]
        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode min-cardinality cardinality)
    (.add model bnode rdf-type restriction)

    bnode)) 

(defn translate-min-qualified-cardinality 
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        filler (translate (get-object :owl:onClass) prefix-2-base model)
        cardinality (get-object :owl:minQualifiedCardinality)
        
        owl-on-property (curie-2-uri "owl:onProperty" prefix-2-base)
        owl-on-class (curie-2-uri "owl:onClass" prefix-2-base)
        owl-min-cardinality (curie-2-uri "owl:minQualifiedCardinality" prefix-2-base)
        owl-restriction (curie-2-uri "owl:Restriction" prefix-2-base) 
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        on-property (.createProperty model owl-on-property)
        on-class (.createProperty model owl-on-class)
        min-cardinality (.createProperty model owl-min-cardinality)
        rdf-type (.createProperty model rdf-type)
        cardinality (.createTypedLiteral model cardinality (curie-2-uri "xsd:nonNegativeInteger" prefix-2-base)) 
        
        restriction (.createResource model owl-restriction)

        ;bnode (NodeFactory/createBlankNode)]
        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode min-cardinality cardinality)
    (.add model bnode rdf-type restriction)
    (.add model bnode on-class filler)

    bnode)) 

(defn translate-max-cardinality 
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        cardinality (get-object :owl:maxCardinality)
        
        owl-on-property (curie-2-uri "owl:onProperty" prefix-2-base)
        owl-max-cardinality (curie-2-uri "owl:maxCardinality" prefix-2-base)
        owl-restriction (curie-2-uri "owl:Restriction" prefix-2-base) 
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        on-property (.createProperty model owl-on-property)
        max-cardinality (.createProperty model owl-max-cardinality)
        rdf-type (.createProperty model rdf-type)
        cardinality (.createTypedLiteral model cardinality (curie-2-uri "xsd:nonNegativeInteger" prefix-2-base)) 
        
        restriction (.createResource model owl-restriction)

        ;bnode (NodeFactory/createBlankNode)]
        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode max-cardinality cardinality)
    (.add model bnode rdf-type restriction)

    bnode)) 

(defn translate-max-qualified-cardinality 
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        filler (translate (get-object :owl:onClass) prefix-2-base model)
        cardinality (get-object :owl:maxQualifiedCardinality)
        
        owl-on-property (curie-2-uri "owl:onProperty" prefix-2-base)
        owl-on-class (curie-2-uri "owl:onClass" prefix-2-base)
        owl-max-cardinality (curie-2-uri "owl:maxQualifiedCardinality" prefix-2-base)
        owl-restriction (curie-2-uri "owl:Restriction" prefix-2-base) 
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        on-property (.createProperty model owl-on-property)
        on-class (.createProperty model owl-on-class)
        max-cardinality (.createProperty model owl-max-cardinality)
        rdf-type (.createProperty model rdf-type)
        cardinality (.createTypedLiteral model cardinality (curie-2-uri "xsd:nonNegativeInteger" prefix-2-base)) 
        
        restriction (.createResource model owl-restriction)

        ;bnode (NodeFactory/createBlankNode)]
        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode max-cardinality cardinality)
    (.add model bnode rdf-type restriction)
    (.add model bnode on-class filler)

    bnode)) 

(defn translate-exact-cardinality 
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        cardinality (get-object :owl:cardinality)
        
        owl-on-property (curie-2-uri "owl:onProperty" prefix-2-base)
        owl-cardinality (curie-2-uri "owl:cardinality" prefix-2-base)
        owl-restriction (curie-2-uri "owl:Restriction" prefix-2-base) 
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        on-property (.createProperty model owl-on-property)
        exact-cardinality (.createProperty model owl-cardinality)
        rdf-type (.createProperty model rdf-type)
        cardinality (.createTypedLiteral model cardinality (curie-2-uri "xsd:nonNegativeInteger" prefix-2-base)) 
        
        restriction (.createResource model owl-restriction)

        ;bnode (NodeFactory/createBlankNode)]
        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode exact-cardinality cardinality)
    (.add model bnode rdf-type restriction)

    bnode)) 

(defn translate-exact-qualified-cardinality 
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        filler (translate (get-object :owl:onClass) prefix-2-base model)
        cardinality (get-object :owl:qualifiedCardinality)
        
        owl-on-property (curie-2-uri "owl:onProperty" prefix-2-base)
        owl-on-class (curie-2-uri "owl:onClass" prefix-2-base)
        owl-exact-cardinality (curie-2-uri "owl:qualifiedCardinality" prefix-2-base)
        owl-restriction (curie-2-uri "owl:Restriction" prefix-2-base) 
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        on-property (.createProperty model owl-on-property)
        on-class (.createProperty model owl-on-class)
        exact-cardinality (.createProperty model owl-exact-cardinality)
        rdf-type (.createProperty model rdf-type)
        cardinality (.createTypedLiteral model cardinality (curie-2-uri "xsd:nonNegativeInteger" prefix-2-base)) 
        
        restriction (.createResource model owl-restriction)

        ;bnode (NodeFactory/createBlankNode)]
        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode exact-cardinality cardinality)
    (.add model bnode rdf-type restriction)
    (.add model bnode on-class filler)

    bnode)) 


(defn translate-has-self 
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        
        owl-on-property (curie-2-uri "owl:onProperty" prefix-2-base)
        owl-has-self (curie-2-uri "owl:hasSelf" prefix-2-base)
        owl-restriction (curie-2-uri "owl:Restriction" prefix-2-base) 
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        on-property (.createProperty model owl-on-property)
        has-self (.createProperty model owl-has-self)
        rdf-type (.createProperty model rdf-type)
        self (.createTypedLiteral model "true" (curie-2-uri "xsd:boolean" prefix-2-base)) 
        
        restriction (.createResource model owl-restriction)

        ;bnode (NodeFactory/createBlankNode)]
        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode has-self self)
    (.add model bnode rdf-type restriction)

    bnode)) 

(defn translate-has-value 
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        individual (translate (get-object :owl:hasValue) prefix-2-base model)
        
        owl-on-property (curie-2-uri "owl:onProperty" prefix-2-base)
        owl-has-value (curie-2-uri "owl:hasValue" prefix-2-base)
        owl-restriction (curie-2-uri "owl:Restriction" prefix-2-base) 
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        on-property (.createProperty model owl-on-property)
        has-value (.createProperty model owl-has-value)
        rdf-type (.createProperty model rdf-type)
        
        restriction (.createResource model owl-restriction)

        ;bnode (NodeFactory/createBlankNode)]
        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode has-value individual);TODO test
    (.add model bnode rdf-type restriction)

    bnode)) 



(defn translate-restriction
  [object-map prefix-2-base model]
  (cond
    (contains? object-map :owl:someValuesFrom) (translate-some object-map prefix-2-base model)
    (contains? object-map :owl:allValuesFrom) (translate-all object-map prefix-2-base model)
    (contains? object-map :owl:minCardinality) (translate-min-cardinality object-map prefix-2-base model)
    (and (contains? object-map :owl:minQualifiedCardinality)
         (contains? object-map :owl:onClass))
    (translate-min-qualified-cardinality object-map prefix-2-base model) 
    (contains? object-map :owl:maxCardinality) (translate-max-cardinality object-map prefix-2-base model)
    (and (contains? object-map :owl:maxQualifiedCardinality) 
         (contains? object-map :owl:onClass)) 
    (translate-max-qualified-cardinality object-map prefix-2-base model)
    (contains? object-map :owl:cardinality) (translate-exact-cardinality object-map prefix-2-base model)
    (and (contains? object-map :owl:qualifiedCardinality) 
         (contains? object-map :owl:onClass)) 
    (translate-exact-qualified-cardinality object-map prefix-2-base model)
    (contains? object-map :owl:hasSelf) (translate-has-self object-map prefix-2-base model)
    (contains? object-map :owl:hasValue) (translate-has-value object-map prefix-2-base model)
    ;TODO onDataRange (qualifiedCardinliaties)

    ))

(defn translate-list
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        first-argument (translate (get-object :rdf:first) prefix-2-base model) 
        rest-argument (translate (get-object :rdf:rest) prefix-2-base model)
        
        rdf-first (curie-2-uri "rdf:first" prefix-2-base)
        rdf-rest (curie-2-uri "rdf:rest" prefix-2-base)

        rdf-first (.createProperty model rdf-first)
        rdf-rest (.createProperty model rdf-rest)

        bnode (.createResource model)]

    (.add model bnode rdf-first first-argument)
    (.add model bnode rdf-rest rest-argument)

    bnode))

(defn translate-intersection
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:intersectionOf) prefix-2-base model) 

        owl-class (curie-2-uri "owl:Class" prefix-2-base)
        owl-intersection (curie-2-uri "owl:intersectionOf" prefix-2-base)
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        owl-intersection (.createProperty model owl-intersection)
        owl-class (.createResource model owl-class)
        rdf-type (.createProperty model rdf-type)

        bnode (.createResource model)]

    (.add model bnode owl-intersection arguments)
    (.add model bnode rdf-type owl-class)

    bnode))

(defn translate-union
  [object-map prefix-2-base model] 
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:unionOf) prefix-2-base model) 

        owl-class (curie-2-uri "owl:Class" prefix-2-base)
        owl-union (curie-2-uri "owl:unionOf" prefix-2-base)
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        owl-union (.createProperty model owl-union)
        owl-class (.createResource model owl-class)
        rdf-type (.createProperty model rdf-type)

        bnode (.createResource model)]

    (.add model bnode owl-union arguments)
    (.add model bnode rdf-type owl-class)

    bnode)) 

(defn translate-class-one-of
  [object-map prefix-2-base model] 
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:oneOf) prefix-2-base model) 

        owl-class (curie-2-uri "owl:Class" prefix-2-base)
        owl-one-of (curie-2-uri "owl:oneOf" prefix-2-base)
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        owl-one-of (.createProperty model owl-one-of)
        owl-class (.createResource model owl-class)
        rdf-type (.createProperty model rdf-type)

        bnode (.createResource model)]

    (.add model bnode owl-one-of arguments)
    (.add model bnode rdf-type owl-class)

    bnode)) 

(defn translate-class-complement
  [object-map prefix-2-base model] 
  (let [get-object (curry-predicate-map object-map)
        argument (translate (get-object :owl:complementOf) prefix-2-base model) 

        owl-class (curie-2-uri "owl:Class" prefix-2-base)
        owl-complement-of (curie-2-uri "owl:complementOf" prefix-2-base)
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        owl-complement-of (.createProperty model owl-complement-of)
        owl-class (.createResource model owl-class)
        rdf-type (.createProperty model rdf-type)

        bnode (.createResource model)]

    (.add model bnode owl-complement-of argument)
    (.add model bnode rdf-type owl-class)

    bnode)) 

(defn translate-all-disjoint-classes
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:members) prefix-2-base model)

        owl-members (curie-2-uri "owl:members" prefix-2-base)
        owl-disjoint-classes (curie-2-uri "owl:AllDisjointClasses" prefix-2-base)
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        members (.createProperty model owl-members)
        disjoint-classes (.createResource model owl-disjoint-classes)
        rdf-type (.createProperty model rdf-type)
        bnode (.createResource model)]

    (.add model bnode members arguments)
    (.add model bnode rdf-type disjoint-classes)
    bnode))

(defn translate-all-different
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        ;arguments (translate (get-object :owl:members) prefix-2-base model)
        ;TODO check distinctMembers vs members
        arguments (translate (get-object :owl:distinctMembers) prefix-2-base model)

        owl-members (curie-2-uri "owl:members" prefix-2-base)
        owl-disjoint-classes (curie-2-uri "owl:AllDifferent" prefix-2-base)
        rdf-type (curie-2-uri "rdf:type" prefix-2-base)

        members (.createProperty model owl-members)
        disjoint-classes (.createResource model owl-disjoint-classes)
        rdf-type (.createProperty model rdf-type)
        bnode (.createResource model)]

    (.add model bnode members arguments)
    (.add model bnode rdf-type disjoint-classes)
    bnode))

(defn translate-inverse-of
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        argument (translate (get-object :owl:inverseOf) prefix-2-base model)

        owl-inverse-of (curie-2-uri "owl:inverseOf" prefix-2-base)
        inverse-of (.createProperty model owl-inverse-of)

        bnode (.createResource model)]
    (.add model bnode inverse-of argument)
    bnode)) 


(defn translate-class
  [object-map prefix-2-base model]
  (cond
    (contains? object-map :owl:intersectionOf) (translate-intersection object-map prefix-2-base model)
    (contains? object-map :owl:unionOf) (translate-union object-map prefix-2-base model)
    (contains? object-map :owl:oneOf) (translate-class-one-of object-map prefix-2-base model)
    (contains? object-map :owl:complementOf) (translate-class-complement object-map prefix-2-base model)))

(defn translate-datatype
  [object-map prefix-2-base model]
  "") 


(defn translate-typed-map
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        t (get-object :rdf:type)]
    (case t
      "owl:Restriction" (translate-restriction object-map prefix-2-base model)
      "owl:Class" (translate-class object-map prefix-2-base model)
      "rdfs:Datatype" (translate-datatype object-map prefix-2-base model)
      "owl:AllDisjointClasses" (translate-all-disjoint-classes object-map prefix-2-base model)
      "owl:AllDifferent" (translate-all-different object-map prefix-2-base model)
      )))

(defn translate-untyped-map
  [object-map prefix-2-base model]
  (cond
    (contains? object-map :rdf:first) (translate-list object-map prefix-2-base model)
    (contains? object-map :owl:intersectionOf) (translate-intersection object-map prefix-2-base model)
    (contains? object-map :owl:inverseOf) (translate-inverse-of object-map prefix-2-base model)
    :else (println "Untyped ERROR")))

(defn translate-object-map
  [object-map prefix-2-base model]
  (if (contains? object-map :rdf:type)
    (translate-typed-map object-map prefix-2-base model)
    (translate-untyped-map object-map prefix-2-base model))) 

(defn translate
  [object prefix-2-base model] 
  (if (map? object)
    (translate-object-map object prefix-2-base model)
    (.createResource model (curie-2-uri object prefix-2-base))))

(defn translate-property
  [object prefix-2-base model] 
    (.createProperty model (curie-2-uri object prefix-2-base))) 

(defn set-prefix-map
  [model prefixes]
  (doseq [row prefixes]
    (.setNsPrefix model (:prefix row) (:base row)))
  model) 

(defn get-prefix-map
  [prefixes]
  (loop [ps prefixes
         prefix-2-base {}]
    (if (empty? ps) 
      prefix-2-base
      (recur (rest ps)
             (assoc prefix-2-base
                    (:prefix (first ps))
                    (:base (first ps))))))) 

(defn translate-literal
  [literal datatype-language-tag prefix-2-base model]
  (if (str/starts-with? datatype-language-tag "@" )
    (.createLiteral model literal (subs datatype-language-tag 1))
    (.createTypedLiteral model literal (curie-2-uri datatype-language-tag prefix-2-base))))

(defn translate-subject
  [entity prefix-2-base model]
  (let [success (try 
                  (cs/parse-string entity true)
                  (catch Exception e nil))]
    (if success
      (translate success prefix-2-base model)
      (translate entity prefix-2-base model))))

(defn translate-object 
  [entity datatype prefix-2-base model]
  (cond
    (= datatype "_JSON") (translate (cs/parse-string entity true) prefix-2-base model)
    (= datatype "_IRI") (.createResource model (curie-2-uri entity prefix-2-base))
    :else (translate-literal entity datatype prefix-2-base model)))


(defn add-annotation
  [bnode subject predicate object prefix-2-base model]
  (let [rdf-type (curie-2-uri "rdf:type" prefix-2-base)
        owl-annotation (curie-2-uri "owl:Annotation" prefix-2-base)
        owl-annotated-source (curie-2-uri "owl:annotatedSource" prefix-2-base)
        owl-annotated-property (curie-2-uri "owl:annotatedProperty" prefix-2-base)
        owl-annotated-target (curie-2-uri "owl:annotatedTarget" prefix-2-base)

        owl-annotation (.createResource model owl-annotation)
        owl-annotated-source (.createProperty model owl-annotated-source)
        owl-annotated-property (.createProperty model owl-annotated-property)
        owl-annotated-target (.createProperty model owl-annotated-target)
        rdf-type (.createProperty model rdf-type)]
      (.add model bnode rdf-type owl-annotation)
      (.add model bnode owl-annotated-source subject)
      (.add model bnode owl-annotated-property (.asResource predicate)) ;need a resource here 
      (.add model bnode owl-annotated-target object)))


(defn add-reification
  [bnode subject predicate object prefix-2-base model]
  (let [rdf-type (curie-2-uri "rdf:type" prefix-2-base)
        rdf-statement (curie-2-uri "rdf:Statement" prefix-2-base)
        rdf-subject (curie-2-uri "rdf:subject" prefix-2-base)
        rdf-predicate (curie-2-uri "rdf:predicate" prefix-2-base)
        rdf-object (curie-2-uri "rdf:object" prefix-2-base)

        rdf-statement (.createResource model rdf-statement)
        rdf-subject (.createProperty model rdf-subject)
        rdf-predicate (.createProperty model rdf-predicate)
        rdf-object (.createProperty model rdf-object)
        rdf-type (.createProperty model rdf-type)]
      (.add model bnode rdf-type rdf-statement)
      (.add model bnode rdf-subject subject)
      (.add model bnode rdf-predicate (.asResource predicate)) ;need a resource here 
      (.add model bnode rdf-object object)))

(defn translate-annotation
  [annotation subject predicate object prefix-2-base model]
  (when annotation
    (let [predicate-map (cs/parse-string annotation true) 
          bnode (.createResource model)
          ks (keys predicate-map)
          ;a predicate map always encodes the 'same' annotation 
          example-key (first ks)
          meta-key (:meta (first (get predicate-map example-key)))] 

      (doseq [k ks] 
        (doseq [x (get predicate-map k)]
          (.add model
                bnode
                (.createProperty model (curie-2-uri (name k) prefix-2-base))
                (translate-object (:object x) (:datatype x) prefix-2-base model))))
      (cond
        (= meta-key "owl:Annotation")
        (add-annotation bnode subject predicate object prefix-2-base model)
        (= meta-key "rdf:Reification")
        (add-reification bnode subject predicate object prefix-2-base model)))))


(defn thick-2-rdf-model
  [thick-triple prefixes]
  ;(println thick-triple)
  (let [;{:keys [assertion retraction graph s p o datatype annotation]} thick-triple 
        model (set-prefix-map (ModelFactory/createDefaultModel) prefixes)
        prefix-2-base (get-prefix-map prefixes)

        subject (translate-subject (:subject thick-triple) prefix-2-base model)
        predicate (.createProperty model (curie-2-uri (:predicate thick-triple) prefix-2-base))
        object (translate-object (:object thick-triple) (:datatype thick-triple) prefix-2-base model)
        p (:predicate thick-triple)
        a (:annotation thick-triple)
        datatype (:datatype thick-triple)]
    (translate-annotation a subject predicate object prefix-2-base model)

    (cond
      (= p "<unknown>") (println (str "ERROR Unknown Predicate: " thick-triple));TODO : handle wiring specific things
      (= p "owl:AllDisjointClasses") model;do nothing 
      (= p "owl:AllDifferent") model;do nothing
      (= datatype "_JSON") (.add model subject predicate object)
      (= datatype "_IRI") (.add model subject predicate object)
      :else (.add model subject predicate object))
    model))

(defn stanza-2-rdf-model
  [thick-triples prefixes]
  (let [models (map #(thick-2-rdf-model % prefixes) thick-triples) 
        model (reduce #(.add %1 %2) models)]
    model))


(defn load-db
  [path]
  {:classname "org.sqlite.JDBC"
  :subprotocol "sqlite"
  :subname path})

;TODO Jena only supports full URI's?
(defn -main
  "Currently only used for manual testing."
  [& args]
  (let [db (load-db (first args))
        prefix (jdbc/query db [(str "SELECT * FROM prefix")]) 
        ;data (jdbc/query db [(str "SELECT * FROM statement LIMIT 25")])
        data (jdbc/query db [(str "SELECT * FROM statement")])
        ;data (jdbc/query db [(str "SELECT * FROM statement WHERE subject='obo:OBI_0302905'")])
        ;data (jdbc/query db [(str "SELECT * FROM statement WHERE subject='obo:OBI_0000797'")])
 
        model (stanza-2-rdf-model data prefix)
        ;data2 (jdbc/query db [(str "SELECT * FROM statement WHERE subject='obo:OBI_0002946'")])
        ;data2 (jdbc/query db [(str "SELECT * FROM statement WHERE subject='obo:IAO_0000032'")])
        ;model2 (stanza-2-rdf-model data2 prefix)
        
        out (io/output-stream "ddd")
        graph1 (.getGraph model)
        ;graph2 (.getGraph model2)
        stream (StreamRDFWriter/getWriterStream out RDFFormat/TURTLE_BLOCKS)
        prefixes (.lock model)

        
        ]
    (.start stream)
    (StreamRDFOps/sendPrefixesToStream prefixes stream)
    (StreamRDFOps/sendTriplesToStream graph1 stream)
    ;(StreamRDFOps/sendTriplesToStream graph2 stream)
    (.finish stream)


    ;(StreamRDFWriter/write out graph1 RDFFormat/TURTLE_BLOCKS)
    ;(StreamRDFWriter/write out graph2 RDFFormat/TURTLE_BLOCKS)

    ;(.write model System/out "TTL")
    ;(.write model2 System/out "TTL")
    ;(RDFDataMgr/write out model (Lang/TTL))
    ;(RDFDataMgr/write out model2 (Lang/TTL))
    ;(RDFDataMgr/write out model (RDFFormat/TURTLE_BLOCKS))
    ;(RDFDataMgr/write out model2 (RDFFormat/TURTLE_BLOCKS))
    ))
      ;(doseq [row data]
      ;  (println row)
      ;  (.write (thick-2-rdf-model row prefix) System/out))))
          ;(println row)))))

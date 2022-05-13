(ns ldtab.thick2thin
  (:require [clojure.string :as str]
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

(defn create-jena-property
  [property model prefix-2-base]
  (let [uri (curie-2-uri property prefix-2-base)]
    (.createProperty model uri)))

(defn create-jena-resource
  [resource model prefix-2-base]
  (let [uri (curie-2-uri resource prefix-2-base)]
    (.createResource model uri))) 

(defn create-jena-typed-literal
  [literal datatype model prefix-2-base]
   (let [uri (curie-2-uri datatype prefix-2-base)]
     (.createTypedLiteral model literal uri)))

(defn translate-some
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        filler (translate (get-object :owl:someValuesFrom) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        on-property (create-property "owl:onProperty") 
        some-values (create-property "owl:someValuesFrom")
        rdf-type (create-property "rdf:type") 
        restriction (create-resource "owl:Restriction")

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

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        on-property (create-property "owl:onProperty") 
        all-values (create-property "owl:allValuesFrom")
        rdf-type (create-property "rdf:type") 
        restriction (create-resource "owl:Restriction") 

        bnode (.createResource model)]

    (.add model bnode all-values filler)
    (.add model bnode on-property property)
    (.add model bnode rdf-type restriction)

    bnode))

(defn translate-min-cardinality 
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        cardinality (get-object :owl:minCardinality)

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        on-property (create-property "owl:onProperty") 
        min-cardinality (create-property "owl:minCardinality")
        rdf-type (create-property "rdf:type") 
        restriction (create-resource "owl:Restriction") 
        cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger")
        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode min-cardinality cardinality)
    (.add model bnode rdf-type restriction)

    bnode)) 

;HERE

(defn translate-min-qualified-cardinality 
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        property (translate (get-object :owl:onProperty) prefix-2-base model)
        filler (translate (get-object :owl:onClass) prefix-2-base model)
        cardinality (get-object :owl:minQualifiedCardinality)

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base)) 

        on-property (create-property "owl:onProperty") 
        on-class (create-property "owl:onClass")
        min-cardinality (create-property "owl:minQualifiedCardinality")
        rdf-type (create-property "rdf:type") 
        restriction (create-resource "owl:Restriction") 
        cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger") 

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

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        on-property (create-property "owl:onProperty") 
        max-cardinality (create-property "owl:maxCardinality")
        rdf-type (create-property "rdf:type") 
        restriction (create-resource "owl:Restriction") 
        cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger")

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

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base)) 

        on-property (create-property "owl:onProperty") 
        on-class (create-property "owl:onClass")
        max-cardinality (create-property "owl:maxQualifiedCardinality")
        rdf-type (create-property "rdf:type") 
        restriction (create-resource "owl:Restriction") 
        cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger") 

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

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base)) 

        on-property (create-property "owl:onProperty") 
        exact-cardinality (create-property "owl:cardinality")
        rdf-type (create-property "rdf:type") 
        restriction (create-resource "owl:Restriction") 
        cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger") 

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

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        on-property (create-property "owl:onProperty")
        on-class (create-property "owl:onClass")
        exact-cardinality (create-property "owl:qualifiedCardinality")
        rdf-type (create-property "rdf:type")
        restriction (create-resource "owl:Restriction")
        cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger")

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

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        on-property (create-property "owl:onProperty") 
        has-self (create-property "owl:hasSelf")
        rdf-type (create-property "rdf:type") 
        self (create-typed-literal "true" "xsd:boolean") 
        restriction (create-resource "owl:Restriction") 

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

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        on-property (create-property "owl:onProperty") 
        has-value (create-property "owl:hasValue")
        rdf-type (create-property "rdf:type") 
        restriction (create-resource "owl:Restriction")

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

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 

        rdf-first (create-property "rdf:first")
        rdf-rest (create-property "rdf:rest")

        bnode (.createResource model)]

    (.add model bnode rdf-first first-argument)
    (.add model bnode rdf-rest rest-argument)

    bnode))

(defn translate-class-intersection
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:intersectionOf) prefix-2-base model) 

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        owl-intersection (create-property "owl:intersectionOf")
        owl-class (create-resource "owl:Class")
        rdf-type (create-property "rdf:type") 

        bnode (.createResource model)]

    (.add model bnode owl-intersection arguments)
    (.add model bnode rdf-type owl-class)

    bnode))

(defn translate-class-union
  [object-map prefix-2-base model] 
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:unionOf) prefix-2-base model) 

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        owl-union (create-property "owl:unionOf")
        owl-class (create-resource "owl:Class")
        rdf-type (create-property "rdf:type") 

        bnode (.createResource model)]

    (.add model bnode owl-union arguments)
    (.add model bnode rdf-type owl-class)

    bnode)) 

(defn translate-class-one-of
  [object-map prefix-2-base model] 
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:oneOf) prefix-2-base model) 

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        owl-one-of (create-property "owl:oneOf")
        owl-class (create-resource "owl:Class")
        rdf-type (create-property "rdf:type") 

        bnode (.createResource model)]

    (.add model bnode owl-one-of arguments)
    (.add model bnode rdf-type owl-class)

    bnode)) 

(defn translate-class-complement
  [object-map prefix-2-base model] 
  (let [get-object (curry-predicate-map object-map)
        argument (translate (get-object :owl:complementOf) prefix-2-base model) 

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        owl-complement-of (create-property "owl:complementOf")
        owl-class (create-resource "owl:Class")
        rdf-type (create-property "rdf:type") 

        bnode (.createResource model)]

    (.add model bnode owl-complement-of argument)
    (.add model bnode rdf-type owl-class)

    bnode)) 

(defn translate-all-disjoint-classes
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:members) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        members (create-property "owl:members")
        disjoint-classes (create-resource "owl:AllDisjointClasses")
        rdf-type (create-property "rdf:type") 

        bnode (.createResource model)]

    (.add model bnode members arguments)
    (.add model bnode rdf-type disjoint-classes)
    bnode))

(defn translate-all-different
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        ;TODO check distinctMembers vs members
        ;arguments (translate (get-object :owl:members) prefix-2-base model)
        arguments (translate (get-object :owl:distinctMembers) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        members (create-property "owl:members")
        different-individuals (create-resource "owl:AllDifferent")
        rdf-type (create-property "rdf:type") 

        bnode (.createResource model)]

    (.add model bnode members arguments)
    (.add model bnode rdf-type different-individuals)
    bnode))

(defn translate-inverse-of
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        argument (translate (get-object :owl:inverseOf) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        inverse-of (create-property "owl:inverseOf")

        bnode (.createResource model)]
    (.add model bnode inverse-of argument)
    bnode)) 


(defn translate-class
  [object-map prefix-2-base model]
  (cond
    (contains? object-map :owl:intersectionOf) (translate-class-intersection object-map prefix-2-base model)
    (contains? object-map :owl:unionOf) (translate-class-union object-map prefix-2-base model)
    (contains? object-map :owl:oneOf) (translate-class-one-of object-map prefix-2-base model)
    (contains? object-map :owl:complementOf) (translate-class-complement object-map prefix-2-base model)))

(defn translate-datatype-intersection
  [object-map prefix-2-base model]
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:intersectionOf) prefix-2-base model) 

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        owl-intersection (create-property "owl:intersectionOf")
        rdfs-datatype (create-resource "rdfs:Datatype")
        rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-intersection arguments)
    (.add model bnode rdf-type rdfs-datatype)

    bnode))

(defn translate-datatype-union
  [object-map prefix-2-base model] 
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:unionOf) prefix-2-base model) 

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base)) 

        owl-union (create-property "owl:unionOf")
        rdfs-datatype (create-resource "rdfs:Datatype")
        rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-union arguments)
    (.add model bnode rdf-type rdfs-datatype)

    bnode)) 

(defn translate-datatype-one-of
  [object-map prefix-2-base model] 
  (let [get-object (curry-predicate-map object-map)
        arguments (translate (get-object :owl:oneOf) prefix-2-base model) 

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        owl-one-of (create-property "owl:oneOf")
        rdfs-datatype (create-resource "rdfs:Datatype")
        rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-one-of arguments)
    (.add model bnode rdf-type rdfs-datatype)

    bnode)) 

(defn translate-datatype-complement
  [object-map prefix-2-base model] 
  (let [get-object (curry-predicate-map object-map)
        argument (translate (get-object :owl:complementOf) prefix-2-base model) 

        create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        owl-complement-of (create-property "owl:datatypeComplementOf")
        rdfs-datatype (create-resource "rdfs:Datatype")
        rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-complement-of argument)
    (.add model bnode rdf-type rdfs-datatype)

    bnode)) 

(defn translate-datatype
  [object-map prefix-2-base model]
  (cond 
    (contains? object-map :owl:intersectionOf) (translate-datatype-intersection object-map prefix-2-base model)
    (contains? object-map :owl:unionOf) (translate-datatype-union object-map prefix-2-base model) 
    (contains? object-map :owl:oneOf) (translate-datatype-one-of object-map prefix-2-base model)
    (contains? object-map :owl:datatypeComplementOf) (translate-datatype-complement object-map prefix-2-base model))) 

(declare translate-object)
;TODO: test this
(defn translate-rdf
  [object-map prefix-2-base model]
  (let [bnode (.createResource model)]
    (doseq [k (keys object-map)] 
      (doseq [x (get object-map k)]
        (.add model
              bnode
              (.createProperty model (curie-2-uri (name k) prefix-2-base))
              (translate-object (:object x) (:datatype x) prefix-2-base model)))
      bnode)))

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
      :else (translate-rdf object-map prefix-2-base model))))

(defn translate-untyped-map
  [object-map prefix-2-base model]
  (cond
    (contains? object-map :rdf:first) (translate-list object-map prefix-2-base model)
    ;(contains? object-map :owl:intersectionOf) (translate-intersection object-map prefix-2-base model)
    (contains? object-map :owl:inverseOf) (translate-inverse-of object-map prefix-2-base model)
    :else (translate-rdf object-map prefix-2-base model)))

;TODO: separate OWL and RDF translation here?
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
  (let [create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base)) 

        ;owl-annotation (create-resource "owl:Annotation")
        owl-annotation (create-resource "owl:Axiom")
        owl-annotated-source (create-property "owl:annotatedSource")
        owl-annotated-property (create-property "owl:annotatedProperty")
        owl-annotated-target (create-property "owl:annotatedTarget")
        rdf-type (create-property "rdf:type")]

      (.add model bnode rdf-type owl-annotation)
      (.add model bnode owl-annotated-source subject)
      (.add model bnode owl-annotated-property (.asResource predicate)) ;need a resource here 
      (.add model bnode owl-annotated-target object)))


(defn add-reification
  [bnode subject predicate object prefix-2-base model]
  (let [create-property (fn [x] (create-jena-property x model prefix-2-base)) 
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        
        rdf-statement (create-resource "rdf:Statement")
        rdf-subject (create-property "rdf:subject")
        rdf-predicate (create-property "rdf:predicate")
        rdf-object (create-property "rdf:object")
        rdf-type (create-property "rdf:type")]

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
                (.createProperty model (curie-2-uri (subs (str k) 1) prefix-2-base))
                (translate-object (:object x) (:datatype x) prefix-2-base model))))
      (cond
        (= meta-key "owl:Annotation")
        (add-annotation bnode subject predicate object prefix-2-base model)
        (= meta-key "rdf:Reification")
        (add-reification bnode subject predicate object prefix-2-base model)))))


;TODO:
;owl:NegativePropertyAssertion
;owl:hasKey
;owl:AllDisjointProperties
;owl:propertyChainAxiom ?

(defn thick-2-rdf-model
  [thick-triple prefixes]
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
      (= p "<unknown>") model;do nothing (blank node statements have already been added with 'translate-object')
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

(defn stanza-2-rdf-model-stream
  [thick-triples prefixes output]
  (let [out-stream (io/output-stream output)
        example-model (thick-2-rdf-model (first thick-triples) prefixes)
        prefix-map (.lock example-model)
        writer-stream (StreamRDFWriter/getWriterStream out-stream RDFFormat/TURTLE_BLOCKS)]
    (.start writer-stream)
    (StreamRDFOps/sendPrefixesToStream prefix-map writer-stream)
    (doseq [triple thick-triples]
      (StreamRDFOps/sendTriplesToStream (.getGraph (thick-2-rdf-model triple prefixes)) writer-stream))
    (.finish writer-stream))) 

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
        data (jdbc/query db [(str "SELECT * FROM statement")])]
    (stanza-2-rdf-model-stream data prefix "test-output")))

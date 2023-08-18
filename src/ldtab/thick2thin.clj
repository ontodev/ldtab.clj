(ns ldtab.thick2thin
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]
            [cheshire.core :as cs])
  (:import [org.apache.jena.graph NodeFactory Triple]
           [org.apache.jena.rdf.model Literal Model ModelFactory Property RDFNode Resource]
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

(defn curie-2-uri ^String
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

(defn create-jena-property ^RDFNode
  [property ^Model model prefix-2-base]
  (let [uri (curie-2-uri property prefix-2-base)]
    (.createProperty model uri)))

(defn create-jena-resource ^RDFNode
  [resource ^Model model prefix-2-base]
  (let [uri (curie-2-uri resource prefix-2-base)]
    (.createResource model uri)))

(defn create-jena-typed-literal ^RDFNode
  [literal datatype ^Model model prefix-2-base]
   (let [uri (curie-2-uri datatype prefix-2-base)]
     (.createTypedLiteral model literal uri)))

(defn translate-some ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode property (translate (get-object :owl:onProperty) prefix-2-base model)
        ^RDFNode filler (translate (get-object :owl:someValuesFrom) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property on-property (create-property "owl:onProperty")
        ^Property some-values (create-property "owl:someValuesFrom")
        ^Property rdf-type (create-property "rdf:type")
        ^RDFNode restriction (create-resource "owl:Restriction")

        bnode (.createResource model)]

    (.add model bnode some-values filler)
    (.add model bnode on-property property)
    (.add model bnode rdf-type restriction)

    bnode))

(defn translate-all ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode property (translate (get-object :owl:onProperty) prefix-2-base model)
        ^RDFNode filler (translate (get-object :owl:allValuesFrom) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property on-property (create-property "owl:onProperty")
        ^Property all-values (create-property "owl:allValuesFrom")
        ^Property rdf-type (create-property "rdf:type")
        ^RDFNode restriction (create-resource "owl:Restriction")

        bnode (.createResource model)]

    (.add model bnode all-values filler)
    (.add model bnode on-property property)
    (.add model bnode rdf-type restriction)

    bnode))

(defn translate-min-cardinality ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode property (translate (get-object :owl:onProperty) prefix-2-base model)
        cardinality (get-object :owl:minCardinality)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        ^Property on-property (create-property "owl:onProperty")
        ^Property min-cardinality (create-property "owl:minCardinality")
        ^Property rdf-type (create-property "rdf:type")
        ^RDFNode restriction (create-resource "owl:Restriction")
        ^RDFNode cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger")
        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode min-cardinality cardinality)
    (.add model bnode rdf-type restriction)

    bnode))

;HERE

(defn translate-min-qualified-cardinality ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode property (translate (get-object :owl:onProperty) prefix-2-base model)
        ^RDFNode filler (translate (get-object :owl:onClass) prefix-2-base model)
        cardinality (get-object :owl:minQualifiedCardinality)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        ^Property on-property (create-property "owl:onProperty")
        ^Property on-class (create-property "owl:onClass")
        ^Property min-cardinality (create-property "owl:minQualifiedCardinality")
        ^Property rdf-type (create-property "rdf:type")
        ^RDFNode restriction (create-resource "owl:Restriction")
        ^RDFNode cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger")

        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode min-cardinality cardinality)
    (.add model bnode rdf-type restriction)
    (.add model bnode on-class filler)

    bnode))

(defn translate-max-cardinality ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode property (translate (get-object :owl:onProperty) prefix-2-base model)
        cardinality (get-object :owl:maxCardinality)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        ^Property on-property (create-property "owl:onProperty")
        ^Property max-cardinality (create-property "owl:maxCardinality")
        ^Property rdf-type (create-property "rdf:type")
        ^RDFNode restriction (create-resource "owl:Restriction")
        ^RDFNode cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger")

        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode max-cardinality cardinality)
    (.add model bnode rdf-type restriction)

    bnode))

(defn translate-max-qualified-cardinality ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode property (translate (get-object :owl:onProperty) prefix-2-base model)
        ^RDFNode filler (translate (get-object :owl:onClass) prefix-2-base model)
        cardinality (get-object :owl:maxQualifiedCardinality)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        ^Property on-property (create-property "owl:onProperty")
        ^Property on-class (create-property "owl:onClass")
        ^Property max-cardinality (create-property "owl:maxQualifiedCardinality")
        ^Property rdf-type (create-property "rdf:type")
        ^RDFNode restriction (create-resource "owl:Restriction")
        ^RDFNode cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger")

        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode max-cardinality cardinality)
    (.add model bnode rdf-type restriction)
    (.add model bnode on-class filler)

    bnode))

(defn translate-exact-cardinality ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode property (translate (get-object :owl:onProperty) prefix-2-base model)
        cardinality (get-object :owl:cardinality)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        ^Property on-property (create-property "owl:onProperty")
        ^Property exact-cardinality (create-property "owl:cardinality")
        ^Property rdf-type (create-property "rdf:type")
        ^RDFNode restriction (create-resource "owl:Restriction")
        ^RDFNode cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger")

        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode exact-cardinality cardinality)
    (.add model bnode rdf-type restriction)

    bnode))

(defn translate-exact-qualified-cardinality ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode property (translate (get-object :owl:onProperty) prefix-2-base model)
        ^RDFNode filler (translate (get-object :owl:onClass) prefix-2-base model)
        cardinality (get-object :owl:qualifiedCardinality)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        ^Property on-property (create-property "owl:onProperty")
        ^Property on-class (create-property "owl:onClass")
        ^Property exact-cardinality (create-property "owl:qualifiedCardinality")
        ^Property rdf-type (create-property "rdf:type")
        ^RDFNode restriction (create-resource "owl:Restriction")
        ^RDFNode cardinality (create-typed-literal cardinality "xsd:nonNegativeInteger")

        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode exact-cardinality cardinality)
    (.add model bnode rdf-type restriction)
    (.add model bnode on-class filler)

    bnode))


(defn translate-has-self ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode property (translate (get-object :owl:onProperty) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))
        create-typed-literal (fn [x y] (create-jena-typed-literal x y model prefix-2-base))

        ^Property on-property (create-property "owl:onProperty")
        ^Property has-self (create-property "owl:hasSelf")
        ^Property rdf-type (create-property "rdf:type")
        ^RDFNode self (create-typed-literal "true" "xsd:boolean")
        ^RDFNode restriction (create-resource "owl:Restriction")

        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode has-self self)
    (.add model bnode rdf-type restriction)

    bnode))

(defn translate-has-value ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode property (translate (get-object :owl:onProperty) prefix-2-base model)
        ^RDFNode individual (translate (get-object :owl:hasValue) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property on-property (create-property "owl:onProperty")
        ^Property has-value (create-property "owl:hasValue")
        ^Property rdf-type (create-property "rdf:type")
        ^RDFNode restriction (create-resource "owl:Restriction")

        bnode (.createResource model)]

    (.add model bnode on-property property)
    (.add model bnode has-value individual);TODO test
    (.add model bnode rdf-type restriction)

    bnode))

(defn translate-restriction ^Resource
  [object-map prefix-2-base ^Model model]
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

(defn translate-list ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode first-argument (translate (get-object :rdf:first) prefix-2-base model)
        ^RDFNode rest-argument (translate (get-object :rdf:rest) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))

        ^Property rdf-first (create-property "rdf:first")
        ^Property rdf-rest (create-property "rdf:rest")

        bnode (.createResource model)]

    (.add model bnode rdf-first first-argument)
    (.add model bnode rdf-rest rest-argument)

    bnode))

(defn translate-class-intersection ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode arguments (translate (get-object :owl:intersectionOf) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property owl-intersection (create-property "owl:intersectionOf")
        ^RDFNode owl-class (create-resource "owl:Class")
        ^Property rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-intersection arguments)
    (.add model bnode rdf-type owl-class)

    bnode))

(defn translate-class-union ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode arguments (translate (get-object :owl:unionOf) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property owl-union (create-property "owl:unionOf")
        ^RDFNode owl-class (create-resource "owl:Class")
        ^Property rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-union arguments)
    (.add model bnode rdf-type owl-class)

    bnode))

(defn translate-class-one-of ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode arguments (translate (get-object :owl:oneOf) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property owl-one-of (create-property "owl:oneOf")
        ^RDFNode owl-class (create-resource "owl:Class")
        ^Property rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-one-of arguments)
    (.add model bnode rdf-type owl-class)

    bnode))

(defn translate-class-complement ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode argument (translate (get-object :owl:complementOf) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property owl-complement-of (create-property "owl:complementOf")
        ^RDFNode owl-class (create-resource "owl:Class")
        ^Property rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-complement-of argument)
    (.add model bnode rdf-type owl-class)

    bnode))

(defn translate-all-disjoint-classes ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode arguments (translate (get-object :owl:members) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property members (create-property "owl:members")
        ^RDFNode disjoint-classes (create-resource "owl:AllDisjointClasses")
        ^Property rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode members arguments)
    (.add model bnode rdf-type disjoint-classes)
    bnode))

(defn translate-all-different ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ;TODO check distinctMembers vs members
        ;arguments (translate (get-object :owl:members) prefix-2-base model)
        ^RDFNode arguments (translate (get-object :owl:distinctMembers) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property members (create-property "owl:members")
        ^RDFNode different-individuals (create-resource "owl:AllDifferent")
        ^Property rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode members arguments)
    (.add model bnode rdf-type different-individuals)
    bnode))

(defn translate-inverse-of ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode argument (translate (get-object :owl:inverseOf) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        ^Property inverse-of (create-property "owl:inverseOf")

        bnode (.createResource model)]
    (.add model bnode inverse-of argument)
    bnode))


(defn translate-class ^Resource
  [object-map prefix-2-base ^Model model]
  (cond
    (contains? object-map :owl:intersectionOf) (translate-class-intersection object-map prefix-2-base model)
    (contains? object-map :owl:unionOf) (translate-class-union object-map prefix-2-base model)
    (contains? object-map :owl:oneOf) (translate-class-one-of object-map prefix-2-base model)
    (contains? object-map :owl:complementOf) (translate-class-complement object-map prefix-2-base model)))

(defn translate-datatype-intersection ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode arguments (translate (get-object :owl:intersectionOf) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property owl-intersection (create-property "owl:intersectionOf")
        ^RDFNode rdfs-datatype (create-resource "rdfs:Datatype")
        ^Property rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-intersection arguments)
    (.add model bnode rdf-type rdfs-datatype)

    bnode))

(defn translate-datatype-union ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode arguments (translate (get-object :owl:unionOf) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property owl-union (create-property "owl:unionOf")
        ^RDFNode rdfs-datatype (create-resource "rdfs:Datatype")
        ^Property rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-union arguments)
    (.add model bnode rdf-type rdfs-datatype)

    bnode))

(defn translate-datatype-one-of ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode arguments (translate (get-object :owl:oneOf) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property owl-one-of (create-property "owl:oneOf")
        ^RDFNode rdfs-datatype (create-resource "rdfs:Datatype")
        ^Property rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-one-of arguments)
    (.add model bnode rdf-type rdfs-datatype)

    bnode))

(defn translate-datatype-complement ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        ^RDFNode argument (translate (get-object :owl:complementOf) prefix-2-base model)

        create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^Property owl-complement-of (create-property "owl:datatypeComplementOf")
        ^RDFNode rdfs-datatype (create-resource "rdfs:Datatype")
        ^Property rdf-type (create-property "rdf:type")

        bnode (.createResource model)]

    (.add model bnode owl-complement-of argument)
    (.add model bnode rdf-type rdfs-datatype)

    bnode))

(defn translate-datatype ^Resource
  [object-map prefix-2-base ^Model model]
  (cond
    (contains? object-map :owl:intersectionOf) (translate-datatype-intersection object-map prefix-2-base model)
    (contains? object-map :owl:unionOf) (translate-datatype-union object-map prefix-2-base model)
    (contains? object-map :owl:oneOf) (translate-datatype-one-of object-map prefix-2-base model)
    (contains? object-map :owl:datatypeComplementOf) (translate-datatype-complement object-map prefix-2-base model)))

(declare ^Resource translate-object)
;TODO: test this
(defn translate-rdf ^Resource
  [object-map prefix-2-base ^Model model]
  (let [bnode (.createResource model)]
    (doseq [k (keys object-map)]
      (doseq [x (get object-map k)]
        (.add model
              bnode
              (.createProperty model (curie-2-uri (name k) prefix-2-base))
              (translate-object (:object x) (:datatype x) prefix-2-base model)))
      bnode)))

(defn translate-typed-map ^Resource
  [object-map prefix-2-base ^Model model]
  (let [get-object (curry-predicate-map object-map)
        t (get-object :rdf:type)]
    (case t
      "owl:Restriction" (translate-restriction object-map prefix-2-base model)
      "owl:Class" (translate-class object-map prefix-2-base model)
      "rdfs:Datatype" (translate-datatype object-map prefix-2-base model)
      "owl:AllDisjointClasses" (translate-all-disjoint-classes object-map prefix-2-base model)
      "owl:AllDifferent" (translate-all-different object-map prefix-2-base model)
      :else (translate-rdf object-map prefix-2-base model))))

(defn translate-untyped-map ^Resource
  [object-map prefix-2-base ^Model model]
  (cond
    (contains? object-map :rdf:first) (translate-list object-map prefix-2-base model)
    ;(contains? object-map :owl:intersectionOf) (translate-intersection object-map prefix-2-base model)
    (contains? object-map :owl:inverseOf) (translate-inverse-of object-map prefix-2-base model)
    :else (translate-rdf object-map prefix-2-base model)))

;TODO: separate OWL and RDF translation here?
(defn translate-object-map ^Resource
  [object-map prefix-2-base ^Model model]
  (if (contains? object-map :rdf:type)
    (translate-typed-map object-map prefix-2-base model)
    (translate-untyped-map object-map prefix-2-base model)))

(defn translate ^Resource
  [object prefix-2-base ^Model model]
  (if (map? object)
    (translate-object-map object prefix-2-base model)
    (.createResource model (curie-2-uri object prefix-2-base))))

(defn translate-property ^Property
  [object prefix-2-base ^Model model]
    (.createProperty model (curie-2-uri object prefix-2-base)))

(defn set-prefix-map ^Model
  [^Model model prefixes]
  (doseq [row prefixes]
    (.setNsPrefix model ^String (:prefix row) ^String (:base row)))
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

(defn translate-literal ^Literal
  [literal datatype-language-tag prefix-2-base ^Model model]
  (if (str/starts-with? datatype-language-tag "@" )
    (.createLiteral model literal (subs datatype-language-tag 1))
    (.createTypedLiteral model literal (curie-2-uri datatype-language-tag prefix-2-base))))

(defn translate-subject ^Resource
  [entity prefix-2-base ^Model model]
  (let [success (try
                  (cs/parse-string entity true)
                  (catch Exception e nil))]
    (if success
      (translate success prefix-2-base model)
      (translate entity prefix-2-base model))))

(defn translate-object ^Resource
  [entity datatype prefix-2-base ^Model model]
  (cond
    (= datatype "_JSON") (translate (cs/parse-string entity true) prefix-2-base model)
    (= datatype "_IRI") (.createResource model (curie-2-uri entity prefix-2-base))
    :else (translate-literal entity datatype prefix-2-base model)))


(defn add-annotation ^Model
  [^Resource bnode ^Resource subject ^Property predicate object prefix-2-base ^Model model]
  (let [create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ;owl-annotation (create-resource "owl:Annotation")
        ^RDFNode owl-annotation (create-resource "owl:Axiom")
        ^Property owl-annotated-source (create-property "owl:annotatedSource")
        ^Property owl-annotated-property (create-property "owl:annotatedProperty")
        ^Property owl-annotated-target (create-property "owl:annotatedTarget")
        ^Property rdf-type (create-property "rdf:type")]

      (.add model bnode rdf-type owl-annotation)
      (.add model bnode owl-annotated-source subject)
      (.add model bnode owl-annotated-property (.asResource predicate)) ;need a resource here
      (.add model bnode owl-annotated-target object)))


(defn add-reification ^Model
  [^Resource bnode ^RDFNode subject ^Property predicate object prefix-2-base ^Model model]
  (let [create-property (fn [x] (create-jena-property x model prefix-2-base))
        create-resource (fn [x] (create-jena-resource x model prefix-2-base))

        ^RDFNode rdf-statement (create-resource "rdf:Statement")
        ^Property rdf-subject (create-property "rdf:subject")
        ^Property rdf-predicate (create-property "rdf:predicate")
        ^Property rdf-object (create-property "rdf:object")
        ^Property rdf-type (create-property "rdf:type")]

      (.add model bnode rdf-type rdf-statement)
      (.add model bnode rdf-subject subject)
      (.add model bnode rdf-predicate (.asResource predicate)) ;need a resource here
      (.add model bnode rdf-object object)))

(defn translate-annotation ^Model
  [annotation subject predicate object prefix-2-base ^Model model]
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

(defn thick-2-rdf-model ^Model
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

(defn stanza-2-rdf-model ^Model
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

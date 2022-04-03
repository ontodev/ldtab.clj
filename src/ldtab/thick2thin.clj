(ns ldtab.thick2thin
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [ldtab.annotation-handling :as ann]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :as cs])
  (:import [org.apache.jena.graph NodeFactory Triple]
           [org.apache.jena.rdf.model Model ModelFactory Resource]
           [org.apache.jena.graph NodeFactory])
  (:gen-class))

;TODO: given a thick triple -> return the corresponding Jena model 

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
      curie)))

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
        on-property (get-object :owl:onProperty)
        all-values (get-object :owl:allValuesFrom)]
  ""))

(defn translate-restriction
  [object-map prefix-2-base model]
  (cond
    (contains? object-map :owl:someValuesFrom) (translate-some object-map prefix-2-base model)
    (contains? object-map :owl:allValuesFrom) (translate-all object-map prefix-2-base model)
    ;min
    ;minQuali
    ;max
    ;maxQuali
    ;exact
    ;exactQuali
    ))

(defn translate-class
  [object-map prefix-2-base model]
  "")

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
      "rdfs:Datatype" (translate-datatype object-map prefix-2-base model))))

(defn translate-untyped-map
  [object-map prefix-2-base model]
  "")

(defn translate-object-map
  [object-map prefix-2-base model]
  ;TODO this needs to check :datatype 
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

;TODO datatype URIs
(defn translate-literal
  [literal datatype-language-tag model]
  (if (str/starts-with? datatype-language-tag "@" )
    (.createLiteral model literal (subs datatype-language-tag 1))
    (.createTypedLiteral model literal datatype-language-tag)));TODO use full URIs here?

(defn thick-2-rdf-model
  [thick-triple prefixes]
  (let [;{:keys [assertion retraction graph s p o datatype annotation]} thick-triple 
        s (:subject thick-triple)
        p (:predicate thick-triple)
        o (:object thick-triple)
        datatype (:datatype thick-triple)
        model (set-prefix-map (ModelFactory/createDefaultModel) prefixes)
        prefix-2-base (get-prefix-map prefixes)
        subject (translate s prefix-2-base model)
        predicate (.createProperty model (curie-2-uri p prefix-2-base))]
    (if (or (= datatype "_IRI")
            (= datatype "_JSON"))
      (.add model subject predicate (translate (cs/parse-string o true) prefix-2-base model))
      (.add model subject predicate (translate-literal o datatype model)))
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
        data (jdbc/query db [(str "SELECT * FROM statement LIMIT 25")])]
      (doseq [row data]
        (when (= (:datatype row) "_JSON")
        (.write (thick-2-rdf-model row prefix) System/out)))))
          ;(println row)))))
        ;(.write (thick-2-rdf-model row prefix) System/out))))
        ;(println row))))

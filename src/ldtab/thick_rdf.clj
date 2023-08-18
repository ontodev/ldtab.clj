(ns ldtab.thick-rdf
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]
            [cheshire.core :as cs])
  (:import [org.apache.jena.graph NodeFactory Triple]
           [org.apache.jena.rdf.model Literal Model ModelFactory Property Resource]
           [org.apache.jena.riot RDFDataMgr RDFFormat Lang]
           [org.apache.jena.riot.system StreamRDFWriter StreamRDFOps]
           [org.apache.jena.graph NodeFactory])
  (:gen-class))

(declare ^Resource translate-predicate-map)

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

(defn translate-iri ^Resource
  [iri prefix-2-base ^Model model]
  (let [uri (curie-2-uri iri prefix-2-base)]
    (.createResource model uri)))

(defn translate-property ^Property
  [property prefix-2-base ^Model model]
  (let [uri (curie-2-uri property prefix-2-base)]
    (.createProperty model uri)))

(defn translate-literal ^Literal
  [^String literal ^String datatype-language-tag prefix-2-base ^Model model]
  (if (str/starts-with? datatype-language-tag "@")
    (.createLiteral model literal (subs datatype-language-tag 1))
    (.createTypedLiteral model literal (curie-2-uri datatype-language-tag prefix-2-base))))

(defn translate-typed-literal ^Literal
  [literal datatype prefix-2-base ^Model model]
  (let [uri (curie-2-uri datatype prefix-2-base)]
    (.createTypedLiteral model literal uri)))

(defn translate-json ^Resource
  [json prefix-2-base ^Model model]
  (let [bnode (.createResource model)]
    (doseq [k (keys json)]
      (doseq [x (get json k)]
        (.add model
              bnode
              (.createProperty model (curie-2-uri k prefix-2-base))
              (translate-predicate-map x prefix-2-base model))))
    bnode))

(defn translate-predicate-map ^Resource
  [predicateMap prefix-2-base ^Model model]
  (case (get predicateMap "datatype")
    "_IRI" (translate-iri (get predicateMap "object") prefix-2-base model)
    "_JSON" (translate-json (get predicateMap "object") prefix-2-base model)
    (translate-literal (get predicateMap "object") (get predicateMap "datatype") prefix-2-base model)))

(defn translate-annotation ^Resource
  [^Resource subject predicate ^Resource object annotation prefix-2-base ^Model model]
  (let [bnode (.createResource model)
        example-key (first (keys annotation))
        rdf-type (get (first (get annotation example-key)) "meta")]
    ;add annotation type 
    (.add model
          bnode
          (.createProperty model (curie-2-uri "rdf:type" prefix-2-base))
          (.createResource model (curie-2-uri rdf-type prefix-2-base)))

    ;add subject 
    (.add model
          bnode
          (.createProperty model (curie-2-uri "owl:annotatedSource" prefix-2-base))
          subject)

    ;add property 
    (.add model
          bnode
          (.createProperty model (curie-2-uri "owl:annotatedProperty" prefix-2-base))
          predicate)

    ;add object 
    (.add model
          bnode
          (.createProperty model (curie-2-uri "owl:annotatedTarget" prefix-2-base))
          object)

    ;add annotation properties
    (doseq [k (keys (dissoc annotation "meta"))] ;TODO dissoc "meta" shouldn't be necessary
      (doseq [x (get annotation k)]
        (.add model
              bnode
              (.createProperty model (curie-2-uri k prefix-2-base))
              (if (contains? x "annotation")
                (translate-annotation bnode (curie-2-uri k prefix-2-base) (translate-predicate-map x prefix-2-base model) (get x "annotation") prefix-2-base model)
                (translate-predicate-map (dissoc x "meta") prefix-2-base model)))))
    bnode))

(defn parse-json
  [json]
  (let [success (try
                  (cs/parse-string json)
                  (catch Exception e nil))
        success (map? success)]
    (if success
      (cs/parse-string json)
      json)))

(defn is-wiring-blanknode
  [input]
  (and (string? input)
       (str/starts-with? input "<wiring:blanknode")))

(defn thick-2-rdf-model ^Model
  [thick-triple prefixes]
  (let [;{:keys [assertion retraction graph s p o datatype annotation]} thick-triple 
        model (set-prefix-map (ModelFactory/createDefaultModel) prefixes)
        prefix-2-base (get-prefix-map prefixes)
        tt {"object" (parse-json (:object thick-triple))
            "datatype" (:datatype thick-triple)}
        ;subject (translate-iri (:subject thick-triple) prefix-2-base model) 
        ;provisional handling of GCIs (with JSON objects in the position of subject column)
        subject-json (parse-json (:subject thick-triple))
        subject (if (string? subject-json)
                  (translate-iri subject-json prefix-2-base model)
                  (translate-json subject-json prefix-2-base model))
        predicate (translate-property (:predicate thick-triple) prefix-2-base model)
        object (translate-predicate-map tt prefix-2-base model)
        annotation (parse-json (:annotation thick-triple))]
    (when annotation
      (translate-annotation subject predicate object annotation prefix-2-base model))
    (if (is-wiring-blanknode subject-json)
      model ;remove generated wiring:blank nodes
      (.add model subject predicate object))))

(defn triples-2-rdf-model-stream
  [thick-triples prefixes output]
  (let [out-stream (io/output-stream output)
        model (set-prefix-map (ModelFactory/createDefaultModel) prefixes)
        prefix-map (.lock model)
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

(defn -main
  "Currently only used for manual testing."
  [& args]
  (let [db (load-db (first args))
        prefix (jdbc/query db [(str "SELECT * FROM prefix")])
        data (jdbc/query db [(str "SELECT * FROM statement")])]
    (triples-2-rdf-model-stream data prefix "test-output")))

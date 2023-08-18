(ns ldtab.export
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]
            [ldtab.thick-rdf :as thick-2-rdf]
            [clojure.string :as str])
  (:import [java.io FileInputStream]
           [org.apache.jena.rdf.model Model ModelFactory Resource]
           [org.apache.jena.riot.system StreamRDFWriter StreamRDFOps]
           [org.apache.jena.riot RDFDataMgr RDFFormat Lang]
           [org.apache.jena.riot RDFDataMgr Lang]
           [org.apache.jena.shared PrefixMapping])
  (:gen-class))

;TODO: export to a file or STDOUT
;FORMATS: Turtle, RDFXML, HTML+RDFa, TSV table?

(defn encode-new-line
  [x]
  (str/replace x #"\r\n|\n|\r" "\\n"))

(defn triple-2-tsv
  "Given a ThickTriple
   return a string of the triple's values separated by tabs."
  [triple]
  (let [vs (vals triple)
        ;vs (map #(str/escape (str %) char-escape-string) vs) 
        vs (map #(str/escape (str %) {\newline "\\n"}) vs)
        tsv (str/join "\t" vs)]
    tsv))

(defn get-tsv-header
  "Given a list of maps,
   return a string of the map's keys separated by tabs."
  [data]
  (let [triple (first data)
        ks (keys triple)
        columns (map name ks)
        header (str/join "\t" columns)]
    header))

(defn export-tsv
  "Given a path to an SQLite database containing ThickTriples,
   write ThickTriples in a TSV file to output."
  ([dp-path output]
   (export-tsv dp-path "statement" output))
  ([db-connection table output]
   (let [data (jdbc/query db-connection [(str "SELECT * FROM " table)])
         output-path (io/as-relative-path output)]
     (with-open [w (io/writer output-path :append true)]
       (.write w (str (get-tsv-header data) "\n"))
       (doseq [row data]
         (.write w (str (triple-2-tsv row) "\n")))))))

(defn export-turtle
  ([db-connection output]
   (export-turtle db-connection "statement" output))
  ([db-connection table output]
   (let [data (jdbc/query db-connection [(str "SELECT * FROM " table)])
         prefix (jdbc/query db-connection [(str "SELECT * FROM prefix")])
         output-path (io/as-relative-path output)]
     (thick-2-rdf/triples-2-rdf-model-stream data prefix output-path))))

(defn set-prefix-map ^Model
  [^Model model prefixes]
  (doseq [row prefixes]
    (.setNsPrefix model ^String (:prefix row) ^String (:base row)))
  model)

(defn export-turtle-stream
  ([db-connection output]
   (export-turtle-stream db-connection "statement" output))
  ([db-connection table output]
   (let [data (jdbc/reducible-query db-connection [(str "SELECT * FROM " table)] {:raw? true})
         prefixes (jdbc/query db-connection [(str "SELECT * FROM prefix")])
         output-path (io/as-relative-path output)
         out-stream (io/output-stream output-path)
         model (set-prefix-map (ModelFactory/createDefaultModel) prefixes)
         prefix-map (.lock model)
         writer-stream (StreamRDFWriter/getWriterStream out-stream RDFFormat/TURTLE_BLOCKS)]
     (.start writer-stream)
     (StreamRDFOps/sendPrefixesToStream prefix-map writer-stream)

     (reduce (fn [prev {:keys [assertion
                               retraction
                               graph
                               subject
                               predicate
                               object
                               datatype
                               annotation]}]
               (StreamRDFOps/sendTriplesToStream (.getGraph (thick-2-rdf/thick-2-rdf-model {:assertion assertion
                                                                                            :retraction retraction
                                                                                            :graph graph
                                                                                            :subject subject
                                                                                            :predicate predicate
                                                                                            :object object
                                                                                            :datatype datatype
                                                                                            :annotation annotation} prefixes)) writer-stream)) "" data)

     (.finish writer-stream))))

(defn export
  [db-connection table file-format output]
  (let [db {:connection-uri db-connection}]
    (cond
      (= file-format "tsv")
      (export-tsv db table output)
      (= file-format "ttl")
      (export-turtle db table output))))

(defn export-stream
  [db-connection table file-format output]
  (let [db {:connection-uri db-connection}]
    (cond
      (= file-format "tsv")
      (throw (Exception. "Streaming otpion for exporting to TSV currently not supported"))
      (= file-format "ttl")
      (export-turtle-stream db table output))))

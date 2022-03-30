(ns ldtab.export
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io FileInputStream] 
           [org.apache.jena.riot RDFDataMgr Lang])
  (:gen-class)) 

;TODO: export to a file or STDOUT
;FORMATS: Turtle, RDFXML, HTML+RDFa, TSV table?

(defn load-db
  [path]
  {:classname "org.sqlite.JDBC"
  :subprotocol "sqlite"
  :subname path})

(defn triple-2-tsv
  [triple]
   (str/join "\t" (vals triple)))

(defn get-tsv-header
  [data]
  (let [triple (first data)
        ks (keys triple)
        columns (map name ks)
        header (str/join "\t" columns)]
    header))

(defn export-tsv
  ([dp-path output]
   (export-tsv dp-path "statement" output))
  ([db-path table output] 
  (let [db (load-db db-path)
        data (jdbc/query db [(str "SELECT * FROM " table)])
        output-path (io/as-relative-path output)] 
    (with-open [w (io/writer output-path :append true)] 
      (.write w (str (get-tsv-header data) "\n"))
      (doseq [row data]
        (.write w (str (triple-2-tsv row) "\n")))))))
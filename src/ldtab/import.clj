(ns ldtab.import
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [ldtab.parse :as parse]
            [clojure.set :as s]
            [wiring.thin2thick.core :as thin2thick])
  (:import [java.io InputStream FileInputStream] 
           [org.apache.jena.riot RDFDataMgr Lang])
  (:gen-class)) 

(defn load-db
  [path]
  {:classname "org.sqlite.JDBC"
  :subprotocol "sqlite"
  :subname path})

(defn insert-triple
  [db transaction graph subject predicate object datatype annotation] 
  (jdbc/insert! db :statement {:assertion transaction
                               :retraction 0
                               :graph graph
                               :subject subject
                               :predicate predicate 
                               :object object
                               :datatype "no datatype"
                               :annotation "no anntation"}))


(defn insert-thin-triple
  [db transaction graph thin-triple annotation] ;NB: this is given in Jena
  (let [subject  (.getSubject thin-triple)
        predicate (.getPredicate thin-triple)
        object (.getObject thin-triple)]
    (if (.isLiteral object)
      (let [literal (.getLiteral object)
            lexicalForm (.getLexicalForm literal)
            languageTag (.language literal)
            datatype (.getLiteralDatatypeURI object)]
        (if (not (= languageTag ""));NB: Jena introduces data types for everything 
          (str db " " transaction " " graph " " subject " " predicate " "  lexicalForm " " languageTag " " annotation)
          (str db " " transaction " " graph " " subject " " predicate " "  lexicalForm " " datatype " " annotation)))
      (str subject " " predicate " " object))))
;TODO ;(insert-triple db transaction graph subject predicate lexicalForm (str languageTag datatype) annotation)


(defn annotations-for-stated-triples
  [annotations thin-triples thick-triples]
  (filter #(or
             (contains? thin-triples (dissoc % "annotation"))
             (contains? thick-triples %))
          annotations))

(defn annotations-for-non-stated-triples
  [annotations thin-triples thick-triples]
  (filter #(not (or
                  (contains? thin-triples (dissoc % "annotation"))
                  (contains? thick-triples %)))
          annotations))

(defn import-rdf
  [rdf-path db-path]
  (let [db (load-db db-path)
        is (new FileInputStream rdf-path)
        it (RDFDataMgr/createIteratorTriples is Lang/RDFXML "base")
        windowsize 50]
    ;TODO refactor this into parsing.clj? or translation.clj since this translates RDF into thick-triples
    (loop [backlog '()
           thin-backlog [0 0 0]
           thick-backlog [0 0 0]
           unstated-annotation-backlog (hash-set)]
      (when (.hasNext it) 
        (let [[thin kept thick] (parse/parse-window it windowsize backlog)

            thin-rdf (map parse/jena-triple-2-string thin)
            thick-rdf (map #(map parse/jena-triple-2-string %) thick)

            thin-json (thin2thick/thin-2-thick thin-rdf)
            thick-json (map #(first (thin2thick/thin-2-thick %)) thick-rdf)

            annotation-json (filter #(contains? % "annotation") thick-json) 
            thin-backlog-flat (set (flatten thin-backlog))
            thick-backlog-flat (set (flatten thick-backlog))

            stated-annotation-json (annotations-for-stated-triples annotation-json
                                                                   thin-backlog-flat 
                                                                   thick-backlog-flat)
            unstated-annotation-json (annotations-for-non-stated-triples annotation-json
                                                                         thin-backlog-flat
                                                                         thick-backlog-flat)


            ;check for previously 'unstated' anotations whether they are in the current backlog windows
            stated-annotation-backlog (annotations-for-stated-triples unstated-annotation-backlog
                                                                      thin-backlog-flat
                                                                      thick-backlog-flat)

            ;keep 'unstated' annotations in case their statements appear later in the streaming
            updated-unstated-annotation-backlog (s/union unstated-annotation-backlog
                                                         (set unstated-annotation-json))
            ;remove found annotations
            updated-unstated-annotation-backlog (s/difference updated-unstated-annotation-backlog
                                                              (set stated-annotation-backlog))]

          (run! println unstated-annotation-backlog)
          (recur kept
                 [thin-json (first thin-backlog) (second thin-backlog)]
                 [thick-json (first thick-backlog) (second thick-backlog)]
                 updated-unstated-annotation-backlog))))))



(defn -main
  "Currently only used for manual testing."
  [& args] 

  (import-rdf (first args) (second args)) 
  
  ) 

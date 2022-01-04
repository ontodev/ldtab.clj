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
                               :retraction 0 ;hard-coded value
                               :graph graph
                               :subject subject
                               :predicate predicate 
                               :object object
                               :datatype datatype
                               :annotation annotation})) 
(defn prepere-insert
  [transaction graph subject predicate object datatype annotation] 
  {:assertion transaction
   :retraction 0 ;hard-coded value
   :graph graph
   :subject subject
   :predicate predicate 
   :object object
   :datatype datatype
   :annotation annotation})

(defn multi-insert
  [json-triples db transaction graph]
  (jdbc/insert-multi! db :statement (map #(prepere-insert transaction
                                                          graph
                                                          (get % "subject")
                                                          (get % "predicate")
                                                          (get % "object")
                                                          (get % "datatype")
                                                          (get % "annotation")) json-triples)))

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

(defn remove-from-thin-backlog 
  [backlog annotations]
  (let [anno (map #(dissoc % "annotation") annotations)
        anno-set (set anno)]
    (into [] (map #(remove (fn [x] (contains? anno-set x)) %) backlog))))

(defn remove-from-thick-backlog 
  [backlog annotations]
  (let [anno-set (set annotations)]
    (into [] (map #(remove (fn [x] (contains? anno-set x)) %) backlog))))

(defn import-rdf
  [rdf-path db-path graph]
  (let [db (load-db db-path)
        is (new FileInputStream rdf-path)
        it (RDFDataMgr/createIteratorTriples is Lang/RDFXML "base")
        windowsize 50]
    ;TODO refactor this into parsing.clj? or translation.clj since this translates RDF into thick-triples
    (loop [backlog '()
           thin-backlog [nil nil nil] 
           thick-backlog [nil nil nil]
           unstated-annotation-backlog (hash-set)
           transaction 1]
      (when (.hasNext it) 
        (let [[thin kept thick] (parse/parse-window it windowsize backlog)

            thin-rdf-string (map parse/jena-triple-2-string thin)
            thick-rdf-string (map #(map parse/jena-triple-2-string %) thick)

            thin-json (thin2thick/thin-2-thick thin-rdf-string)
            thick-json (map #(first (thin2thick/thin-2-thick %)) thick-rdf-string)

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
                                                              (set stated-annotation-backlog))

            annotations (concat annotation-json stated-annotation-backlog)
            [thin1 thin2 thin3] (remove-from-thin-backlog thin-backlog annotations)
            [thick1 thick2 thick3] (remove-from-thin-backlog thick-backlog annotations)]

          ;insert into database
          ;TODO update transaction 
          (when thin3
            (multi-insert thin3 db transaction graph))
          (when thick3
            (multi-insert thick3 db transaction graph))
          (when annotations
            (multi-insert annotations db transaction graph))

          (recur kept
                 [thin-json thin1 thin2] 
                 [thick-json thick1 thick2]
                 updated-unstated-annotation-backlog
                 transaction))))))



(defn -main
  "Currently only used for manual testing."
  [& args] 

  ;(let [rdf-path (first args)
  ;      is (new FileInputStream rdf-path)
  ;      it (RDFDataMgr/createIteratorTriples is Lang/RDFXML "base")
  ;      windowsize 50]

  ;  (loop [backlog '()] 
  ;    (when (.hasNext it) 
  ;      (let [[thin kept thick] (parse/parse-window it windowsize backlog) 
  ;             thick-rdf-string (map #(map parse/jena-triple-2-string %) thick)
  ;             thick-json (map #(first (thin2thick/thin-2-thick %)) thick-rdf-string)]
  ;        (run! println thick-json)
  ;        ;(run! println (thin2thick/thin-2-thick (map parse/jena-triple-2-string thin)))
  ;        ;(run! println (thin2thick/thin-2-thick (map #(map parse/jena-triple-2-string %) thick)))
  ;        ;(thin2thick/thin-2-thick (map parse/jena-triple-2-string thin))
  ;        (recur kept)))))
          

  (time (import-rdf (first args) (second args) "graph"))
  
  ) 

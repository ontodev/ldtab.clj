(ns ldtab.import
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [ldtab.parse :as parse];TODO remove this dependency
            [ldtab.parse-model :as parseModel]
            [ldtab.parse-alternative :as parseAlternative] 
            [clojure.set :as s]
            [cheshire.core :as cs]
            [ldtab.thin2thick :as thin2thick])
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
   :subject (cs/generate-string subject);TODO use cheshire here to encode things as JSON strings
   :predicate (cs/generate-string predicate)
   :object (cs/generate-string object)
   :datatype (cs/generate-string datatype)
   :annotation (cs/generate-string annotation)})

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
  """Given a backlog map m, and a list of updated subjects,
    update m's :updated keys for all (dependent) subjects"""
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

(defn insert-tail
  [db transaction graph backlog thin-backlog thick-backlog unstated-annotation-backlog]
  (let [[thin1 thin2 thin3] thin-backlog
        [thick1 thick2 thick3] thick-backlog
        backlog-triples (flatten (map #(:triples %)  (vals backlog))) 
        backlog-json (thin2thick/thin-2-thick backlog-triples)] 
  (do
    (when backlog-json 
      (multi-insert backlog-json db transaction graph))
    (when thin1
      (multi-insert thin1 db transaction graph))
    (when thick1
      (multi-insert thick1 db transaction graph))
    (when thin2
      (multi-insert thin2 db transaction graph))
    (when thick2
      (multi-insert thick2 db transaction graph))
    (when thin3
      (multi-insert thin3 db transaction graph))
    (when thick3
      (multi-insert thick3 db transaction graph))
    (when unstated-annotation-backlog
      (multi-insert unstated-annotation-backlog db transaction graph)))))

(defn import-rdf-streamed
  [db-path rdf-path graph]
  (let [db (load-db db-path)
        is (new FileInputStream rdf-path)
        it (RDFDataMgr/createIteratorTriples is Lang/RDFXML "base")
        windowsize 60000] ;Note: this window size is pretty large
    ;TODO refactor this
    ;into parsing.clj? or annotation-handling.clj?
    (loop [backlog {}
           thin-backlog [nil nil nil] 
           thick-backlog [nil nil nil]
           unstated-annotation-backlog (hash-set)
           transaction 1]
      (if (not (.hasNext it))
        (insert-tail db transaction graph backlog thin-backlog thick-backlog unstated-annotation-backlog)
        (let [[thin kept thick] (parseAlternative/parse-window it windowsize backlog)

            thin-json (thin2thick/thin-2-thick thin)
            thick-json (thin2thick/thin-2-thick thick)


            ;TODO refactor: the follownig is just annotatiion handling
            ;TODO set operations are slow
            annotation-json (filter #(contains? % "annotation") thick-json)
            thin-backlog-flat (set (flatten thin-backlog))
            thick-backlog-flat (set (flatten thick-backlog))

            ;takes annotations (as JSON) as well as thin and thick triples (as JSON)
            ;and checks whether an annotation 'talks' about a thin/thick triple
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
          ;TODO: this is slow (takes 11 sec for obi)
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

;TODO: we still cannot represent annotations/reifications that are not also stated
(defn import-rdf-model
  [db-path rdf-path graph]
  (let [db (load-db db-path)
        thin-triples (parseModel/as-thin-triples-streamed rdf-path)
        thick-triples (map thin2thick/thin-2-thick thin-triples)
        thick-triples (apply concat thick-triples)
        annotation-triples (filter #(contains? % "annotation") thick-triples)
        superfuous (set (map #(dissoc % "annotation") annotation-triples))
        thick-triples (remove superfuous thick-triples)] 
    (multi-insert thick-triples db 1 graph))) 

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
          

  ;(time (import-rdf-streamed (first args) (second args) "graph"))
   (import-rdf-model (first args) (second args) "graph")
  
  ) 

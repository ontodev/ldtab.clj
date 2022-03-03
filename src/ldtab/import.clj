(ns ldtab.import
  (:require [clojure.java.jdbc :as jdbc]
            [ldtab.rdf-model :as rdf-model]
            [ldtab.parsing :as parsing] 
            [clojure.set :as set]
            [cheshire.core :as cs]
            [ldtab.thin2thick :as thin2thick])
  (:import [java.io FileInputStream] 
           [org.apache.jena.riot RDFDataMgr Lang])
  (:gen-class)) 

(defn load-db
  [path]
  {:classname "org.sqlite.JDBC"
  :subprotocol "sqlite"
  :subname path})

(defn encode-json
  "Given information for a row in the statment table,
  encode thick-triple information as JSON strings."
  [transaction graph subject predicate object datatype annotation] 
  {:assertion transaction
   :retraction 0 ;hard-coded value: data is being inserted
   :graph graph
   ;encode data as JSON strings
   :subject (if (string? subject) subject (cs/generate-string subject))
   :predicate predicate
   :object (if (string? object) object (cs/generate-string object))
   :datatype datatype
   :annotation (when annotation (cs/generate-string annotation))}) 

(defn insert-triples
  "Inserts a list of thick triples into a database."
  [json-triples db transaction graph]
  (jdbc/insert-multi! db :statement (map #(encode-json transaction
                                                       graph
                                                       (get % "subject")
                                                       (get % "predicate")
                                                       (get % "object")
                                                       (get % "datatype")
                                                       (get % "annotation")) json-triples)))

(defn annotations-for-stated-triples
  "Given lists of thick triples for
    (1) annotations
    (2) thin-triples
    (3) thick triples
  return annotations in (1) for tripes in (2) or (3)" 
  [annotations thin-triples thick-triples]
  (filter #(or
             (contains? thin-triples (dissoc % "annotation"))
             (contains? thick-triples %))
          annotations))

(defn annotations-for-non-stated-triples
  "Given lists of thick triples for
    (1) annotations
    (2) thin-triples
    (3) thick triples
  return annotations in (1) for tripes not in (2) or (3)" 
  [annotations thin-triples thick-triples]
  (filter #(not (or
                  (contains? thin-triples (dissoc % "annotation"))
                  (contains? thick-triples %)))
          annotations)) 

(defn remove-from-thin-backlog 
  [backlog annotations]
  (let [anno (map #(dissoc % "annotation") annotations)
        anno-set (set anno)]
    (vec (map #(remove (fn [x] (contains? anno-set x)) %) backlog))))

;TODO test this
(defn remove-from-thick-backlog 
  [backlog annotations]
  (let [anno-set (set annotations)] ;why don't you have to remove the annotation here?
    (vec (map #(remove (fn [x] (contains? anno-set x)) %) backlog))))

(defn insert-tail
  "Insert thin and thick triples currently in the backlog windows."
  [db transaction graph backlog thin-backlog thick-backlog unstated-annotation-backlog]
  (let [[thin1 thin2 thin3] thin-backlog
        [thick1 thick2 thick3] thick-backlog
        backlog-triples (flatten (map :triples (vals backlog))) 
        backlog-json (thin2thick/thin-2-thick backlog-triples)] 
    (when backlog-json (insert-triples backlog-json db transaction graph))
    (when thin1 (insert-triples thin1 db transaction graph))
    (when thick1 (insert-triples thick1 db transaction graph))
    (when thin2 (insert-triples thin2 db transaction graph))
    (when thick2 (insert-triples thick2 db transaction graph))
    (when thin3 (insert-triples thin3 db transaction graph))
    (when thick3 (insert-triples thick3 db transaction graph))
    (when unstated-annotation-backlog
      (insert-triples unstated-annotation-backlog db transaction graph))))

(defn associate-annotations-with-statements
  [thin thick thin-backlog thick-backlog unstated-annotation-backlog]
  (let [thin-json (thin2thick/thin-2-thick thin)
        thick-json (thin2thick/thin-2-thick thick) 

        ;TODO set operations are slow
        annotation-json (filter #(contains? % "annotation") thick-json)
        thin-backlog-flat (set (flatten thin-backlog))
        thick-backlog-flat (set (flatten thick-backlog))

        unstated-annotation-json (annotations-for-non-stated-triples annotation-json
                                                                     thin-backlog-flat
                                                                     thick-backlog-flat)


        ;check for previously 'unstated' anotations whether they are in the current backlog windows
        stated-annotation-backlog (annotations-for-stated-triples unstated-annotation-backlog
                                                                  thin-backlog-flat
                                                                  thick-backlog-flat)

        ;keep 'unstated' annotations in case their statements appear later in the streaming
        updated-unstated-annotation-backlog (set/union unstated-annotation-backlog
                                                       (set unstated-annotation-json))
        ;remove found annotations
        updated-unstated-annotation-backlog (set/difference updated-unstated-annotation-backlog
                                                            (set stated-annotation-backlog))

        annotations (concat annotation-json stated-annotation-backlog)
        [thin1 thin2 thin3] (remove-from-thin-backlog thin-backlog annotations)
        [thick1 thick2 thick3] (remove-from-thick-backlog thick-backlog annotations)] 
    {:thin thin3
     :thick thick3
     :annotations annotations
     :thin-backlog [thin-json thin1 thin2]
     :thick-backlog [thick-json thick1 thick2]
     :unstated-annotation-backlog updated-unstated-annotation-backlog}))


(defn import-rdf-stream
  [db-path rdf-path graph]
  (let [db (load-db db-path)
        is (new FileInputStream rdf-path)
        it (RDFDataMgr/createIteratorTriples is Lang/RDFXML "base")
        windowsize 60000] ;Note: this window size is pretty large
    (loop [backlog {}
           thin-backlog [nil nil nil] 
           thick-backlog [nil nil nil]
           unstated-annotation-backlog (hash-set)
           transaction 1]
      (if-not (.hasNext it)
        (insert-tail db transaction graph backlog thin-backlog thick-backlog unstated-annotation-backlog)
        (let [[thin kept thick] (parsing/parse-window it windowsize backlog)
              annotation-handling (associate-annotations-with-statements thin
                                                                         thick
                                                                         thin-backlog
                                                                         thick-backlog
                                                                         unstated-annotation-backlog)
              thin (:thin annotation-handling)
              thick (:thick annotation-handling)
              annotations (:annotations annotation-handling)] 

          ;insert into database 
          (when thin (insert-triples thin db transaction graph))
          (when thick (insert-triples thick db transaction graph))
          (when annotations (insert-triples annotations db transaction graph))

          (recur kept 
                 (:thin-backlog annotation-handling)
                 (:thick-backlog annotation-handling)
                 (:unstated-annotation-backlog annotation-handling)
                 transaction)))))) 

(defn load-prefixes
  [db]
  (jdbc/query db ["SELECT * FROM prefix"]))

;TODO: we still cannot represent annotations/reifications that are not also stated
(defn import-rdf-model
  [db-path rdf-path graph]
  (let [db (load-db db-path)
        thin-triples (rdf-model/group-thin-triple-dependencies rdf-path)
        iri2prefix (load-prefixes db)
        thick-triples (map #(thin2thick/thin-2-thick % iri2prefix) thin-triples)
        thick-triples (apply concat thick-triples)
        annotation-triples (filter #(contains? % "annotation") thick-triples)
        superfluous (set (map #(dissoc % "annotation") annotation-triples))
        thick-triples (remove superfluous thick-triples)]
    (insert-triples thick-triples db 1 graph))) 


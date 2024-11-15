(ns ldtab.import
  (:require [clojure.java.jdbc :as jdbc]
            [ldtab.rdf-model :as rdf-model]
            [ldtab.parsing :as parsing]
            [clojure.string :as str]
            [clojure.set :as set]
            [cheshire.core :as cs]
            [ldtab.thin2thick :as thin2thick])
  (:import [java.io FileInputStream]
           [java.util Iterator]
           [org.apache.jena.riot RDFDataMgr Lang]
           [org.apache.jena.graph Triple])
  (:gen-class))

(defn load-prefixes
  [db]
  (jdbc/query db ["SELECT * FROM prefix"]))

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
  [json-triples db table transaction graph]
  (jdbc/insert-multi! db (keyword table) (map #(encode-json transaction
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
  [db table transaction graph backlog thin-backlog thick-backlog unstated-annotation-backlog iri2prefix]
  (let [[thin1 thin2 thin3] thin-backlog
        [thick1 thick2 thick3] thick-backlog
        backlog-triples (flatten (map :triples (vals backlog)))
        ;NB: 'existential blank nodes', i.e., blank nodes appearing as objects
        ;that cannot be replaced by a nested JSON structure are not associated with any triples.
        ;So, backlog-triples will contain nil values for these blank nodes.
        ;These nil values need to be removed before translating RDF triples to LDTab ThickTriples
        backlog-triples (remove nil? backlog-triples) 
        backlog-json (thin2thick/thin-2-thick backlog-triples iri2prefix)]

    (when backlog-json (insert-triples backlog-json db table transaction graph))
    (when thin1 (insert-triples thin1 db table transaction graph))
    (when thick1 (insert-triples thick1 db table transaction graph))
    (when thin2 (insert-triples thin2 db table transaction graph))
    (when thick2 (insert-triples thick2 db table transaction graph))
    (when thin3 (insert-triples thin3 db table transaction graph))
    (when thick3 (insert-triples thick3 db table transaction graph))
    (when unstated-annotation-backlog
      (insert-triples unstated-annotation-backlog db table transaction graph))))

(defn associate-annotations-with-statements
  [thin thick thin-backlog thick-backlog unstated-annotation-backlog iri2prefix]
  (let [thin-json (thin2thick/thin-2-thick thin iri2prefix)
        thick-json (thin2thick/thin-2-thick thick iri2prefix)

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
  ([^String db-path ^String rdf-path ^String graph]
   (import-rdf-stream db-path "statement" rdf-path graph))
  ([^String db-connection ^String table ^String rdf-path ^String graph]
   (let [db {:connection-uri db-connection}
         iri2prefix (load-prefixes db)
         ^FileInputStream is (new FileInputStream rdf-path)
         ^String extension (last (str/split rdf-path #"\."))
         ^Iterator it (cond ;guess file format
                        (= extension "ttl") (RDFDataMgr/createIteratorTriples is Lang/TTL "base")
                        (= extension "nt") (RDFDataMgr/createIteratorTriples is Lang/NT "base")
                        :else ;use RDFXML by default
                        (RDFDataMgr/createIteratorTriples is Lang/RDFXML "base")) 
         windowsize 500]
     (loop [backlog {}
            thin-backlog [nil nil nil]
            thick-backlog [nil nil nil]
            unstated-annotation-backlog (hash-set)
            transaction 1]

       (if-not (.hasNext it)
         (insert-tail db table transaction graph backlog thin-backlog thick-backlog unstated-annotation-backlog iri2prefix)
         (let [[thin kept thick] (parsing/parse-window it windowsize backlog)
               annotation-handling (associate-annotations-with-statements thin
                                                                          thick
                                                                          thin-backlog
                                                                          thick-backlog
                                                                          unstated-annotation-backlog
                                                                          iri2prefix)
               thin-annotated (:thin annotation-handling)
               thick-annotated (:thick annotation-handling)
               annotations (:annotations annotation-handling)]

          ;insert into database 
           (when thin-annotated (insert-triples thin-annotated db table transaction graph))
           (when thick-annotated (insert-triples thick-annotated db table transaction graph))
           (when annotations (insert-triples annotations db table transaction graph))

           (recur kept
                  (:thin-backlog annotation-handling)
                  (:thick-backlog annotation-handling)
                  (:unstated-annotation-backlog annotation-handling)
                  transaction)))))))


;TODO: we still cannot represent annotations/reifications that are not also stated


(defn import-rdf-model
  ([database-connection ^String rdf-path ^String graph]
   (import-rdf-model database-connection "statement" rdf-path graph))
  ([database-connection ^String table ^String rdf-path ^String graph]
   (let [db {:connection-uri database-connection}
         thin-triples (rdf-model/group-blank-node-paths rdf-path)
         iri2prefix (load-prefixes db)
         thick-triples (map (fn [^Triple x] (thin2thick/thin-2-thick x iri2prefix)) thin-triples)
         thick-triples (apply concat thick-triples)
         annotation-triples (filter #(contains? % "annotation") thick-triples)
         superfluous (set (map #(dissoc % "annotation") annotation-triples))
         thick-triples (remove superfluous thick-triples)]
     (insert-triples thick-triples db table 1 graph))))

(defn -main
  "Currently only used for manual testing."
  [& args]
  (let [db {:connection-uri (first args)}
        thin-triples (rdf-model/group-blank-node-paths (second args))
        iri2prefix (load-prefixes db)
        thick-triples (map #(thin2thick/thin-2-thick % iri2prefix) thin-triples)]
    (doseq [t thick-triples]
      (println t))))

  ;(time (import-rdf-stream (first args) (second args) "graph")))

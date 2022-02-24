(ns ldtab.init
  (:require [clojure.java.jdbc :as jdbc]))

(defn create-database
  "Initializes a SQLite database by creating three tables:
  1. ldtab: [key, value] pairs for tracking metadata
  2. prefix: [prefix, base] for tracking prefixes
  3. statement: [assertion, retraction, graph, subject, predicate, object, datatype, annotation] for statements (thick triples)" 
  [dbname] 
  (def db-spec
    {:dbtype "sqlite"
     :dbname (str dbname) 
     :user "myaccount"
     :password "secret"})

  (def metadata-table-ddl
  (jdbc/create-table-ddl :ldtab
                         [[:key "TEXT" "PRIMARY KEY"]
                          [:value "TEXT"]]))

  (def prefix-table-ddl
  (jdbc/create-table-ddl :prefix
                         [[:prefix "TEXT" "PRIMARY KEY"]
                          [:base "TEXT" "NOT NULL"]]))

  (def statement-table-ddl
  (jdbc/create-table-ddl :statement
                         [[:assertion :int "NOT NULL"] 
                          [:retraction :int "NOT NULL DEFAULT 0"]
                          [:graph "TEXT" "NOT NULL"]
                          [:subject "TEXT" "NOT NULL"]
                          [:predicate "TEXT" "NOT NULL"]
                          [:object "TEXT" "NOT NULL"]
                          [:datatype "TEXT" "NOT NULL"]
                          [:annotation "TEXT"]])) 

  ;add tables to database
  (jdbc/db-do-commands db-spec metadata-table-ddl)
  (jdbc/db-do-commands db-spec prefix-table-ddl)
  (jdbc/db-do-commands db-spec statement-table-ddl)

  (jdbc/insert! db-spec :ldtab {:key "ldtab version" :value "0.0.1"})
  (jdbc/insert! db-spec :ldtab {:key "schema version" :value "0"})) 

(defn -main
  "Currently only used for manual testing."
  [& args]

  ;expect database name as first argument
  (create-database (first args)) 
  ;(println (jdbc/query   db-spec ["SELECT * FROM ldtab"])) 
  )


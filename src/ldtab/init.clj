(ns ldtab.init
  (:require [clojure.java.jdbc :as jdbc]))

(defn get-ldtab-statement-schema
  "Returns the schema of the ldtab database."
  []
  [[:assertion :int "NOT NULL DEFAULT 1"]
   [:retraction :int "NOT NULL DEFAULT 0"]
   [:graph "TEXT" "NOT NULL DEFAULT graph"]
   [:subject "TEXT" "NOT NULL"]
   [:predicate "TEXT" "NOT NULL"]
   [:object "TEXT" "NOT NULL"]
   [:datatype "TEXT" "NOT NULL DEFAULT '_IRI'"]
   [:annotation "TEXT DEFAULT NULL"]])

(defn setup
  [db-spec table]
  (let [metadata-table-ddl (jdbc/create-table-ddl :ldtab
                                                  [[:key "TEXT" "PRIMARY KEY"]
                                                   [:value "TEXT"]])

        prefix-table-ddl (jdbc/create-table-ddl :prefix
                                                [[:prefix "TEXT" "PRIMARY KEY"]
                                                 [:base "TEXT" "NOT NULL"]])

        statement-table-ddl (jdbc/create-table-ddl (keyword table)
                                                   (get-ldtab-statement-schema))]

    ;add tables to database
    (jdbc/db-do-commands db-spec metadata-table-ddl)
    (jdbc/db-do-commands db-spec prefix-table-ddl)
    (jdbc/db-do-commands db-spec statement-table-ddl)

    (jdbc/insert! db-spec :ldtab {:key "ldtab version" :value "0.0.1"})
    (jdbc/insert! db-spec :ldtab {:key "schema version" :value "0"})))

;example: {:connection-uri "jdbc:postgresql://127.0.0.1:5432/kdb?user=knocean&password=knocean"} 
(defn initialise-database
  [connection table]
  (let [db-spec {:connection-uri connection}]
    (setup db-spec table)))

(defn create-sql-database
  "Creates an SQLite file with three tables:
  1. 'ldtab' with columns: [key, value],
  2. 'prefix' with columns: [prefix, base],
  3. 'statement' with columns: [assertion, retraction, graph, subject, predicate, object, datatype, annotation]."
  [dbname table]
  (let [db-spec {:dbtype "sqlite"
                 :dbname (str dbname)
                 :user "myaccount"
                 :password "secret"}]
    (setup db-spec table)))

(defn add-table
  [dbname table]
  (let [db-spec {:dbtype "sqlite"
                 :dbname (str dbname)
                 :user "myaccount"
                 :password "secret"}]
    (jdbc/db-do-commands db-spec (jdbc/create-table-ddl (keyword table)
                                                        (get-ldtab-statement-schema)))))

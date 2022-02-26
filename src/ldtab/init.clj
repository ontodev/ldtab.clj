(ns ldtab.init
  (:require [clojure.java.jdbc :as jdbc]))

(defn create-database
  "Creates an SQLite file with three tables:
  1. 'ldtab' with columns: [key, value],
  2. 'prefix' with columns: [prefix, base],
  3. 'statement' with columns: [assertion, retraction, graph, subject, predicate, object, datatype, annotation]." 
  [dbname] 
  (let [db-spec {:dbtype "sqlite"
                 :dbname (str dbname) 
                 :user "myaccount"
                 :password "secret"}

        metadata-table-ddl (jdbc/create-table-ddl :ldtab
                                                  [[:key "TEXT" "PRIMARY KEY"]
                                                   [:value "TEXT"]])

        prefix-table-ddl (jdbc/create-table-ddl :prefix
                                                [[:prefix "TEXT" "PRIMARY KEY"]
                                                 [:base "TEXT" "NOT NULL"]])

        statement-table-ddl (jdbc/create-table-ddl :statement
                                                   [[:assertion :int "NOT NULL"] 
                                                    [:retraction :int "NOT NULL DEFAULT 0"]
                                                    [:graph "TEXT" "NOT NULL"]
                                                    [:subject "TEXT" "NOT NULL"]
                                                    [:predicate "TEXT" "NOT NULL"]
                                                    [:object "TEXT" "NOT NULL"]
                                                    [:datatype "TEXT" "NOT NULL"]
                                                    [:annotation "TEXT"]])]

    ;add tables to database
    (jdbc/db-do-commands db-spec metadata-table-ddl)
    (jdbc/db-do-commands db-spec prefix-table-ddl)
    (jdbc/db-do-commands db-spec statement-table-ddl)

    (jdbc/insert! db-spec :ldtab {:key "ldtab version" :value "0.0.1"})
    (jdbc/insert! db-spec :ldtab {:key "schema version" :value "0"})))

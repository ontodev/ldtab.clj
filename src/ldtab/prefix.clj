(ns ldtab.prefix
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc])
  (:gen-class)) 

(defn parse-tsv
  [input]
  (with-open [reader (io/reader input)]
  (doall
    (csv/read-csv reader :separator \tab)))) 

(defn load-db
  [path]
  {:classname "org.sqlite.JDBC"
  :subprotocol "sqlite"
  :subname path})

(defn insert-prefix
  [db prefix base]
  (jdbc/insert! db :prefix {:prefix prefix :base base}))

(defn query-prefixes
  [db-path]
  (let [db (load-db db-path)]
    (jdbc/query db ["SELECT * FROM prefix"])))

(defn get-prefixes-as-string
  "Query for prefixes in an SQLite database and return them as a string."
  [db-path]
  (let [prefixes (query-prefixes db-path)
        prefix-strings (map #(str (:prefix %) "," (:base %) "\n") prefixes)
        prefix-string (str/join prefix-strings)] 
    (str "Prefixes in " db-path ":\n\n" prefix-string))) 

(defn insert-prefixes
  "Add prefixes from a TSV file to an SQLite database."
 [db-path prefixes-path]
 (let [db (load-db db-path)
       prefixes (rest (parse-tsv prefixes-path))];rest: drop header "prefix base"
   (doseq [p prefixes] 
     (insert-prefix db (first p) (second p)))))

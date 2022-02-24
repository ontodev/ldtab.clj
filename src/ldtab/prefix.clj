(ns ldtab.prefix
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
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

(defn insert-prefixes
 [db-path prefixes-path]
 (let [db (load-db db-path)
       ;rest: parse prefixes and drop first row "prefix base"
       prefixes (rest (parse-tsv prefixes-path))]
   (doseq [p prefixes] 
     (insert-prefix db (first p) (second p)))))

;manual testing
(defn -main [& args]
  (def db-spec (load-db (second args)))
  (println (jdbc/query db-spec ["SELECT * FROM prefix"]))
  (insert-prefixes (first args) (second args))
  (println (jdbc/query db-spec ["SELECT * FROM prefix"])))

(ns ldtab.cli
  (:require [clojure.tools.cli :refer [parse-opts]]
            [ldtab.prefix :as prefix] 
            )
  (:gen-class))

(def cli-options
  [["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :update-fn inc]
   ["-h" "--help"]])

;TODO: implement options for subcommands
(def init-options
  [["-h" "--help init"]])

(def prefix-options
  [["-h" "--help prefix"]])

(def import-options
  [["-h" "--help import"]])

(def export-options
  [["-h" "--help export"]])

(defn parse-subcommand
  [command]
  (let [subcommand (first command)]
    ;TODO: Do we need nested subcommands? 
    (cond
      (= subcommand "init") (parse-opts command init-options :in-order true)
      (= subcommand "prefix") (parse-opts command prefix-options :in-order true)
      (= subcommand "import") (parse-opts command import-options :in-order true)
      (= subcommand "export") (parse-opts command export-options :in-order true)
      :else "print help")));TODO: define help message

(defn execute-prefix-command 
  [prefixes-path db-path]
  (prefix/insert-prefixes prefixes-path db-path))

(defn execute-subcommand
  [parse-map]
  (let [command (:arguments parse-map)
        subcommand (first command)]
    (cond 
      (= subcommand "init") (println "Work in progress")
      (= subcommand "prefix") (execute-prefix-command (nth command 1) (nth command 2))
      (= subcommand "import") (println "Work in progress")
      (= subcommand "export") (println "Work in progress")
      :else "Incorrect command")));TODO: define help message


(defn -main [& args]
  (println (parse-subcommand (:arguments (parse-opts args cli-options :in-order true))))
  (execute-subcommand (parse-subcommand (:arguments (parse-opts args cli-options :in-order true)))))


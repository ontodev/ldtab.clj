(ns ldtab.cli
  (:require [clojure.tools.cli :refer [parse-opts]])
  (:gen-class))

(def cli-options
  [["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :update-fn inc]
   ["-h" "--help"]])

;TODO: implement options for subcommands
(def init-options
  [["-h" "--help"]])

(def prefix-options
  [["-h" "--help"]])

(def import-options
  [["-h" "--help"]])

(def export-options
  [["-h" "--help"]])

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


(defn -main [& args]
  (println (parse-subcommand (:arguments (parse-opts args cli-options :in-order true)))))

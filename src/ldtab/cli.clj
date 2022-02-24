(ns ldtab.cli
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [ldtab.init :as init-db])
  (:gen-class))

(def cli-options
  [["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :update-fn inc]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["LDTab is a tool for working with RDF datasets and OWL using SQL databases.
         The immediate use case for LDTab is an ontology term browser
         with support for history and multiple named graphs.
         The current version is focused on embedded database use case, building on SQLite. 
        This is my program. There are many like it, but this one is mine."
        ""
        "Usage: ldtab [options] action"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "  init     Create a new LDTab database"
        "  prefix   Define prefixes to shorten IRIs to CURIEs."
        "  import   Import an RDFXML file into the databse."
        ""
        "Please refer to the manual page for more information."]
       (string/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with an error message, and optional ok status), or a map
  indicating the action the program should take and the options provided."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options :in-order true)]
    (cond
      (:help options) 
      {:exit-message (usage summary) :ok? true}
      errors ; errors => exit with description of errors
      {:exit-message (error-msg errors)}

      ;TODO: refactor "init" validation into its own function
      (and (= "init" (first arguments));check arity
           (not (= 2 (count arguments))))
      {:exit-message "Invalid input: init requires a single argument."} 

      (and (= "init" (first arguments));
           (= 2 (count arguments))
           (.exists (io/as-file (second arguments))));check whether file exists
      {:exit-message (str "Invalid input: File " (second arguments) " already exists.")} 

      (and (= "init" (first arguments))
           (= 2 (count arguments))
           (not (.exists (io/as-file (second arguments)))))
      {:action arguments :options options}

      (and (= "prefix" (first arguments))
           (= 3 (count arguments)))
      {:action  arguments :options options}

      (and (= "import" (first arguments))
           (= 3 (count arguments)))
      {:action  arguments :options options}

      :else ; failed custom validation => exit with usage summary
      {:exit-message (usage summary)})))

(defn exit [status msg]
  (println msg)
  (System/exit status))


;TODO: implement options for subcommands
(def init-options
  [["-h" "--help"]
   ["-i" "--info"]
   ["-o" "--output"]])

(def prefix-options
  [["-h" "--help"]])

(def import-options
  [["-h" "--help"]])

(def export-options
  [["-h" "--help"]])

;TODO handle options for subcommand
(defn init
  [command]
  (let [db (second (:arguments (parse-opts command init-options :in-order true)))]
   (init-db/create-database db)))

(defn parse-subcommand
  [command]
  (let [subcommand (first command)]
    (cond
      (= subcommand "init") (init command)
      (= subcommand "prefix") (parse-opts command prefix-options :in-order true)
      (= subcommand "import") (parse-opts command import-options :in-order true)
      (= subcommand "export") (parse-opts command export-options :in-order true)
      :else "Unknown subcommand")));this should not occur


(defn -main [& args]
  (let [{:keys [action options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (parse-subcommand action))))
      ;(case action
      ;  "start"  (server/start! options)
      ;  "stop"   (server/stop! options)
      ;  "status" (server/status! options)))))


  ;(println (parse-opts args cli-options :in-order true)))
  ;(println (parse-subcommand (:arguments (parse-opts args cli-options :in-order true)))))

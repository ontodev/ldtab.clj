(ns ldtab.cli
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [ldtab.init :as init-db]
            [ldtab.prefix :as prefix]
            [ldtab.import :as import-db])
  (:gen-class))

(def cli-options
  [["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :update-fn inc]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["LDTab is a tool for working with RDF datasets and OWL using SQL databases."
        "The immediate use case for LDTab is an ontology term browser"
        "with support for history and multiple named graphs."
        "The current version is focused on embedded database use case, building on SQLite."
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

(defn validate-init
  [arguments]
  (cond 
    (not (= 2 (count arguments)))
    {:exit-message "Invalid input: init requires a single argument."} 

    (.exists (io/as-file (second arguments)))
    {:exit-message (str "Invalid input: File " (second arguments) " already exists.")} 

    :else
    {:action arguments}))

(defn validate-prefix
  [arguments]
  (cond
    (not (= 3 (count arguments)))
    {:exit-message "Invalid input: prefix requires two arguments."} 

    (not (.exists (io/as-file (second arguments))))
    {:exit-message "Invalid input: database (first argument) does not exist."} 

    (not (.exists (io/as-file (nth arguments 2))))
    {:exit-message "Invalid input: prefix table (second argument) does not exist."} 

    :else 
    {:action arguments}))

(defn validate-import 
  [arguments]
  (cond
    (not (= 3 (count arguments)))
    {:exit-message "Invalid input: import requires two arguments."} 

    (not (.exists (io/as-file (second arguments))))
    {:exit-message "Invalid input: database (first argument) does not exist."} 

    (not (.exists (io/as-file (nth arguments 2))))
    {:exit-message "Invalid input: ontology (second argument) does not exist."} 

    :else
    {:action arguments}))

(defn validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with an error message, and optional ok status), or a map
  indicating the action the program should take and the options provided."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options :in-order true)]
    (cond
      (:help options) 
      {:exit-message (usage summary) :ok? true}

      errors 
      {:exit-message (error-msg errors)}

      (= "init" (first arguments))
      (assoc (validate-init arguments) :options options)

      (= "prefix" (first arguments))
      (assoc (validate-prefix arguments) :options options)

      (= "import" (first arguments))
      (assoc (validate-import arguments) :options options)

      ;TODO implement support for export

      :else 
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
(defn ldtab-init
  [command]
  (let [db (second (:arguments (parse-opts command init-options :in-order true)))]
   (init-db/create-database db)))

;TODO handle options for subcommend
(defn ldtab-import
  [command]
  (let [arguments (:arguments (parse-opts command import-options :in-order true))
        db (second arguments)
        ontology (nth arguments 2)]
    (import-db/import-rdf db ontology "graph")));TODO how do we handle the graph input?

;TODO handle options for subcommand
;TODO validate tsv file
(defn ldtab-prefix
  [command]
  (let [arguments (:arguments (parse-opts command import-options :in-order true))
        db (second arguments)
        tsv (nth arguments 2)]
    (prefix/insert-prefixes db tsv))) 

(defn parse-subcommand
  [command]
  (let [subcommand (first command)]
    (cond
      (= subcommand "init") (ldtab-init command)
      (= subcommand "prefix") (ldtab-prefix command)
      (= subcommand "import") (ldtab-import command)
      (= subcommand "export") (parse-opts command export-options :in-order true)
      :else "Unknown subcommand")));this should not occur 

(defn -main [& args]
  (let [{:keys [action options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (parse-subcommand action))))

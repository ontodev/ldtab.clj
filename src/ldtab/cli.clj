(ns ldtab.cli
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [ldtab.init :as init-db]
            [ldtab.prefix :as prefix]
            [ldtab.import :as import-db]
            [ldtab.export :as export-db])
  (:gen-class))

;TODO what kind of options should LDTab provide?
(def cli-options
  [["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :update-fn inc]
   ["-h" "--help"]])

;TODO: implement options for subcommands
;TODO write custom help messages for subcommands

(def init-options
  [["-h" "--help"] 
   ["-c" "--connection" "Database connection uri"]])

(def prefix-options
  [["-h" "--help"]
   ["-l" "--list" "List prefixes"]
   ["-c" "--connection" "Database connection uri"]])

(def import-options
  [["-h" "--help"]
   ["-t" "--table TABLE" "Table"
    :parse-fn #(identity %)]
   ["-c" "--connection" "Database connection uri"]
   ["-s" "--streaming"]])

(def export-options
  [["-h" "--help"]
   ["-t" "--table TABLE" "Table"
    :parse-fn #(identity %)] 
   ["-f" "--format FORMAT" "Output format"
    :parse-fn #(identity %)]
   ["-c" "--connection" "Database connection uri"]
   ["-s" "--streaming"]])

(defn get-file-extension
  [path]
  (last (str/split path #"\.")))

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
        "  import   Import an RDFXML file into the database."
        "  export   Export an LDTab database to TTL or TSV."
        ""
        "Please refer to the manual page for more information."]
       (str/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn validate-init
  "Validate command line arguments for the `init` subcommand."
  [command]
  (let [{:keys [options arguments errors summary]} (parse-opts command init-options)]
  (cond 
    (:help options) 
    {:exit-message (usage summary) :ok? true}

    errors 
    {:exit-message (error-msg errors)}

    (not= 2 (count arguments))
    {:exit-message "Invalid input: init requires a single argument."} 

    :else
    {:action command})))

(defn validate-prefix
  "Validate command line arguments for the `prefix` subcommand."
  [command]
  (let [{:keys [options arguments errors summary]} (parse-opts command prefix-options)]
    (cond
      (:help options) 
      {:exit-message (usage summary) :ok? true}

      (and (:list options) 
           (not= 2 (count arguments)))
      {:exit-message "Invalid input: prefix --list requires a single argument"}

      (and (:list options) 
           (= 2 (count arguments)))
      {:exit-message (prefix/get-prefixes-as-string (second arguments)) :ok? true}

      errors 
      {:exit-message (error-msg errors)}

      (not= 3 (count arguments))
      {:exit-message "Invalid input: prefix requires two arguments."} 

      (not (.exists (io/as-file (nth arguments 2))))
      {:exit-message "Invalid input: prefix table (second argument) does not exist."} 

      :else 
      {:action command})))

(defn validate-import 
  "Validate command line arguments for the `import` subcommand."
  [command]
  (let [{:keys [options arguments errors summary]} (parse-opts command import-options)]
  (cond
    (:help options) 
    {:exit-message (usage summary) :ok? true}

    errors 
    {:exit-message (error-msg errors)}

    (not= 3 (count arguments))
    {:exit-message "Invalid input: import requires two arguments."} 

    (not (.exists (io/as-file (nth arguments 2))))
    {:exit-message "Invalid input: ontology (second argument) does not exist."} 

    :else
    {:action command})))

(defn validate-export 
  "Validate command line arguments for the `export` subcommand."
  [command]
  (let [{:keys [options arguments errors summary]} (parse-opts command export-options)]
  (cond
    (:help options) 
    {:exit-message (usage summary) :ok? true}

    errors 
    {:exit-message (error-msg errors)}

    (not= 3 (count arguments))
    {:exit-message "Invalid input: export requires two arguments."} 

    (.exists (io/as-file (nth arguments 2)))
    {:exit-message "Invalid input: output file (second argument) already exists."} 

    (not (contains? #{"ttl" "tsv"} (get-file-extension (nth arguments 2))))
    {:exit-message (str "Invalid output format: " (get-file-extension (nth arguments 2)))} 

    :else
    {:action command})))

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

      (= "export" (first arguments))
      (assoc (validate-export arguments) :options options)

      :else 
      {:exit-message (usage summary)})))

(defn exit [status msg]
  (println msg)
  (System/exit status)) 


;TODO handle options for subcommand
(defn ldtab-init
  [command]
  (let [{:keys [options arguments errors summary]} (parse-opts command import-options) 
        db (second arguments)
        database-system (:connection options)] 
        (if database-system
          (init-db/initialise-database db);expects a connnection-uri
          (init-db/create-sql-database db))));expects the name for the database 

;TODO handle options for subcommend
;TODO add options to use 'streaming' or 'non-streaming' version
(defn ldtab-import
  [command]
  (let [{:keys [options arguments errors summary]} (parse-opts command import-options)
        db (second arguments)
        ontology (nth arguments 2)
        streaming (:streaming options)
        table (:table options)
        database-system (:connection options)

        ;set defaults
        table (if table table "statement")
        database-system (if database-system database-system "sqlite")]

    (if streaming
      (if table
        (import-db/import-rdf-stream db table ontology "graph")
        (import-db/import-rdf-stream db ontology "graph"))
      (if table
        (import-db/import-rdf-model db table ontology "graph")
        (import-db/import-rdf-model db ontology "graph")))));TODO how do we handle the graph input?  

(defn ldtab-export
  [command]
  (let [{:keys [options arguments errors summary]} (parse-opts command export-options)
        db (second arguments) 
        output (nth arguments 2)
        streaming (:streaming options)
        table (:table options)
        extension (get-file-extension output)];TODO: add options for output format
    (cond
      ;TODO guess output format based on file extension
      (and table (= extension "tsv"))
      (export-db/export-tsv db table output)

      (and table (= extension "ttl"))
      (if streaming
        (export-db/export-turtle-stream db table output)
        (export-db/export-turtle db table output))
      
      (= extension "tsv")
      (export-db/export-tsv db output)

      (= extension "ttl")
      (if streaming
        (export-db/export-turtle-stream db output)
        (export-db/export-turtle db output))

      (not (nil? table))
      (export-db/export-tsv db table output)

      :else (export-db/export-tsv db output))))

;TODO handle options for subcommand
;TODO validate tsv file
;TODO issue #3 says to print prefixes if second argument is missing
(defn ldtab-prefix
  [command]
  (let [arguments (:arguments (parse-opts command import-options))
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
      (= subcommand "export") (ldtab-export command)
      :else "Unknown subcommand")))

(defn -main [& args]
  (let [{:keys [action options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (parse-subcommand action))))

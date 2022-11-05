(ns ldtab.diff 
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io])
  (:import [java.time.format DateTimeFormatter]
           [java.time LocalDateTime]))

(defn get-lines
  [fname]
  (with-open [r (io/reader fname)]
    ;rest drops header line
    (doall (rest (line-seq r)))))

;iterate over lines of both files
;if both lines are equal => advance both iterators
;if not equal -> if 1 < 2 => deleted, advance 1
;             -> if 1 > 2 => added, advance 2 
(defn get-diff
  [f1 f2]
  (let [s1 (sort (distinct f1))
        s2 (sort (distinct f2)) ] ;NB: duplicates in the input is not good
    (loop [old_version s1
           new_version s2
           added '()
          deleted '()] 
      (let [old_element (first old_version)
            new_element (first new_version)]
        (cond
          (empty? old_version)
          [(concat added new_version) deleted]
          (empty? new_version)
          [added (concat deleted old_version)]
          :else
          (if (= old_element new_element)
            (recur (rest old_version)
                   (rest new_version)
                   added
                   deleted)
            (if (< (compare old_element new_element) 0)
              (recur (rest old_version)
                     new_version
                     added
                     (conj deleted old_element))
              (recur old_version
                     (rest new_version)
                     (conj added new_element)
                     deleted))))))))

(defn write-list [lines output-path]
  (with-open [wtr (clojure.java.io/writer output-path)]
    (.write wtr (str/join "\t" ["assertion"
                                "retraction"
                                "graph"
                                "subject"
                                "predicate"
                                "object"
                                "datatype"
                                "annotation\n"]))
    (doseq [line lines] (.write wtr (str line "\n")))))

(defn set-transaction-id
  [record id]
  (let [split (str/split record #"\t")
        id-set (assoc split 0 id)
        join (str/join "\t" id-set)]
    join))

(defn set-retraction
  [record value]
  (let [split (str/split record #"\t")
        retraction-set (assoc split 1 value)
        join (str/join "\t" retraction-set)]
    join))

(defn get-time
  []
  (let [date (LocalDateTime/now)
        formatter (DateTimeFormatter/ofPattern "yyyyMMddHHmmss")
        form (.format date formatter)]
    form))

(defn diff-to-file
  [f1 f2 output]
  (let [transaction (get-time) 
        diff (get-diff f1 f2)
        added (first diff)
        deleted (second diff)

        added (map #(set-transaction-id % transaction) added)
        deleted (map #(set-transaction-id % transaction) deleted)
        deleted (map #(set-retraction % "1") deleted)
        diff-tsv (concat added deleted)] 
    (write-list diff-tsv output)))

(defn triple-2-tsv
  "Given a ThickTriple
   return a string of the triple's values separated by tabs."
  [triple]
  (let [vs (vals triple)
        vs (map #(str/escape (str %) {\newline "\\n"}) vs) 
        tsv (str/join "\t" vs)]
    tsv)) 

(defn get-tsv
  [db-connection]
  (let [data (jdbc/query db-connection [(str "SELECT * FROM statement")])
        tsv (map triple-2-tsv data)]
    tsv))

(defn max-strings
  [a b]
  (if (< (compare (Integer/parseInt a) (Integer/parseInt b)) 0)
    a
    b)) 

(defn get-max-transaction-tsv
  [tsv]
  (let [transactions (map #(first (str/split % #"\t")) tsv)
        max-transaction (reduce max-strings transactions)]
    (println max-transaction)))

(defn get-max-transaction-db
  [db-connection]
  (let [query (jdbc/query db-connection [(str "SELECT max(assertion) FROM statement")])
        vs (first (vals (first query)))]
    vs))


(defn update-state
  [state triple]
  (let [t (assoc triple :retraction "0")
        t (assoc t :assertion "1")
        t (triple-2-tsv t)]
  (if (= (:retraction triple) 1)
    (disj state t)
    (conj state t))))

(defn build-helper
  [db-connection id state]
  (let [triples (jdbc/query db-connection [(str "SELECT * FROM statement WHERE assertion='" id "'")])]
    (loop [ts triples
           res state] 
      (if (empty? ts)
        res
        (recur (rest ts)
               (update-state res (first ts))))))) 


;NB: this returns a set of tsv triples
(defn build-to-transaction
  [db-connection transaction];or DB?
  (let [transaction_ids (jdbc/query db-connection [(str "SELECT DISTINCT assertion FROM statement")])
        transaction_ids_i (map :assertion transaction_ids)
        transaction_ids (sort transaction_ids_i)
        transaction_ids (filter #(<= (compare % transaction) 0) transaction_ids)] 
    (loop  [ids transaction_ids
            res #{}]
      (if (empty? ids)
        res
        (recur (rest ids) 
               (build-helper db-connection 
                             (first ids)
                             res)))))) 

(defn diff-to-db
  [db-path tsv-path] 
  (let [db-connection (str "jdbc:sqlite:"
                          (System/getProperty "user.dir")
                          "/" db-path)
        ;db (get-tsv {:connection-uri db-connection})
        max-transaction (get-max-transaction-db db-connection)
        recent (into () (build-to-transaction db-connection max-transaction))
        file (get-lines tsv-path) 
        diff (get-diff recent file)]
    diff))

(defn to-row
  [tsv]
  (let [split (str/split tsv #"\t")
        split (conj split "") ;avoid out-of bounds exception in case of no annotation
        row {:assertion (first split)
             :retraction (second split)
             :graph (nth split 2)
             :subject (nth split 3)
             :predicate (nth split 4)
             :object (nth split 5)
             :datatype (nth split 6)
             :annotation (nth split 7)}]
    row))

(defn insert-tsv
  [db-path tsv]
  (let [db-connection (str "jdbc:sqlite:"
                          (System/getProperty "user.dir")
                          "/" db-path)
        db-spec {:connection-uri db-connection}
        split (str/split tsv #"\t")
        split (conj split "") ;avoid out-of bounds exception in case of no annotation
        row {:assertion (first split)
             :retraction (second split)
             :graph (nth split 2)
             :subject (nth split 3)
             :predicate (nth split 4)
             :object (nth split 5)
             :datatype (nth split 6)
             :annotation (nth split 7)}]

    (jdbc/insert! db-spec :statement row)))

(defn update-db
  [db-path tsv-path]
  (let [db-connection (str "jdbc:sqlite:"
                          (System/getProperty "user.dir")
                          "/" db-path)
        db-spec {:connection-uri db-connection} 
        diff (diff-to-db db-path tsv-path)
        transaction_id (get-time)
        additions (map #(set-transaction-id % transaction_id) (first diff))
        deletions (map #(set-transaction-id % transaction_id) (second diff))
        deletions (map #(set-retraction % 1) deletions)]

    (jdbc/insert-multi! db-spec :statement (map to-row additions))
    (jdbc/insert-multi! db-spec :statement (map to-row deletions))))

;diff two tsv files -> added + delted
;given database + new tsv -> add diff to database
(defn -main
  "Currently only used for manual testing."
  [& args] 
    (update-db (first args) (second args)))

(ns ldtab.rdf-list-handling
  (:require [cheshire.core :as cs])
  (:gen-class))

(declare encode-rdf-list)

(defn collect-list-elements [json acc]
  (let [element (first (get json "rdf:first"))
        remainder (:object (first (get json "rdf:rest")))]
    (if (= remainder "rdf:nil")
      (conj acc element)
      (recur remainder (conj acc element)))))

(defn is-top-level-rdf-list? [json]
  (if (and (map? json)
           (contains? json :subject)
           (contains? json :predicate)
           (contains? json :object))
    (let [object (:object json)
          datatype (:datatype json)]
      (and (map? object)
           (contains? object "rdf:first")
           (contains? object "rdf:rest")
           (= datatype "_JSONMAP")))
    false))

(defn is-rdf-list? [json]
  (if (map? json)
    (let [object (:object json)
          datatype (:datatype json)]
      (and (map? object)
           (contains? object "rdf:first")
           (contains? object "rdf:rest")
           (= datatype "_JSONMAP")))
    false))

;json is required to be an rdf list
(defn encode-rdf-list-object [json]
  (let [elements (collect-list-elements json [])
        encoded-elements (map #(encode-rdf-list %) elements)]
    {:object (into [] encoded-elements)
     :datatype "_JSONLIST"}))

(defn map-on-hash-map-vals
  "Given a hashmap m and a function f, 
  apply f to all values of m.
  Example:
  Given m = {:a 1, :b 2}  and f = (fn [x] (inc x)),
  then (map-on-hash-map-vals f m) = {:a 2, :b 3}"
  [f m]
  (zipmap (keys m) (map f (vals m))))

(defn encode-top-level-rdf-list [json]
  (let [object (:object json)
        list-object (:object (encode-rdf-list-object object))]
    (assoc json :object list-object :datatype "_JSONLIST")))

(defn encode-rdf-list
  "Given a JSON value, return a lexicographically ordered representation."
  [m]
  (cond
    (is-top-level-rdf-list? m) (encode-top-level-rdf-list m)
    (is-rdf-list? m) (encode-rdf-list-object (:object m))
    (map? m) (map-on-hash-map-vals encode-rdf-list m)
    (coll? m) (into [] (map encode-rdf-list m))
    :else m))

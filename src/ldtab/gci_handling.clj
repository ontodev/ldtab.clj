(ns ldtab.gci-handling
  (:require [cheshire.core :as cs])
  (:gen-class))

;TODO: make predicate-map keys consistent (either use strings or keys - but not both)
(defn encode-raw-gci-map
  [predicate-map]
  (let [superclass (first (get predicate-map "rdfs:subClassOf"))
        subclass (dissoc predicate-map "rdfs:subClassOf")
        datatype (:datatype superclass)
        superclass (:object superclass)]
    {:subject subclass
     :predicate "rdfs:subClassOf"
     :object superclass
     :datatype datatype}))



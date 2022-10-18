(ns ldtab.thin2thick-test
  (:require [clojure.test :refer :all] 
            [cheshire.core :as cs]
            [ldtab.thin2thick :as t2t ])) 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;Check that thick-triples can be compared as strings;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest key-order-is-different-after-parsing
  (let [s1 "{\"owl:onProperty\":[{\"datatype\":\"_IRI\",\"object\":\"obo:RO_0000085\"}],
             \"owl:someValuesFrom\":[{\"datatype\":\"_IRI\",\"object\":\"obo:OBI_0001043\"}],
             \"rdf:type\":[{\"datatype\":\"_IRI\",\"object\":\"owl:Restriction\"}]}"
        s2 "{\"owl:someValuesFrom\":[{\"datatype\":\"_IRI\",\"object\":\"obo:OBI_0001043\"}],
             \"owl:onProperty\":[{\"datatype\":\"_IRI\",\"object\":\"obo:RO_0000085\"}],
             \"rdf:type\":[{\"datatype\":\"_IRI\",\"object\":\"owl:Restriction\"}]}"

        json1 (cs/parse-string s1 true)
        json2 (cs/parse-string s2 true) 

        ss1 (str json1)
        ss2 (str json2)]
    (is (not= ss1 ss2))))

(deftest key-order-is-the-same-after-sorting
  (let [s1 "{\"owl:onProperty\":[{\"datatype\":\"_IRI\",\"object\":\"obo:RO_0000085\"}],
             \"owl:someValuesFrom\":[{\"datatype\":\"_IRI\",\"object\":\"obo:OBI_0001043\"}],
             \"rdf:type\":[{\"datatype\":\"_IRI\",\"object\":\"owl:Restriction\"}]}"
        s2 "{\"owl:someValuesFrom\":[{\"datatype\":\"_IRI\",\"object\":\"obo:OBI_0001043\"}],
             \"owl:onProperty\":[{\"datatype\":\"_IRI\",\"object\":\"obo:RO_0000085\"}],
             \"rdf:type\":[{\"datatype\":\"_IRI\",\"object\":\"owl:Restriction\"}]}"
        json1 (cs/parse-string s1 true)
        json2 (cs/parse-string s2 true)

        sort1 (t2t/sort-json json1)
        sort2 (t2t/sort-json json2)

        ss1 (str sort1)
        ss2 (str sort2)]
    (is ss1 ss2)))


(deftest array-order-is-different-after-parsing
  (let [s1 "{\"obo:IAO_0010000\":[{\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-001\"},
                    {\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-002\"},
                    {\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-003\"}]}"
        s2 "{\"obo:IAO_0010000\":[{\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-003\"},
                    {\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-001\"},
                    {\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-002\"}]}"

        json1 (cs/parse-string s1 true)
        json2 (cs/parse-string s2 true)] 
    (is (not= json1 json2))))

(deftest array-order-is-the-same-after-sorting
  (let [s1 "{\"obo:IAO_0010000\":[{\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-001\"},
                    {\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-002\"},
                    {\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-003\"}]}"
        s2 "{\"obo:IAO_0010000\":[{\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-003\"},
                    {\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-001\"},
                    {\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-002\"}]}"

        json1 (cs/parse-string s1 true)
        json2 (cs/parse-string s2 true)

        sort1 (t2t/sort-json json1)
        sort2 (t2t/sort-json json2)
        
        ss1 (str sort1)
        ss2 (str sort2)] 
    (is ss1 ss2)))

(deftest array-order-and-key-order-is-the-same-after-sorting
  (let [s1 "{\"owl:onProperty\":[{\"datatype\":\"_IRI\",\"object\":\"obo:RO_0000085\"}],
             \"owl:someValuesFrom\":[{\"datatype\":\"_IRI\",\"object\":\"obo:OBI_0001043\"}],
             \"rdf:type\":[{\"datatype\":\"_IRI\",\"object\":\"owl:Restriction\"}],
             \"obo:IAO_0010000\":[{\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-001\"},
                                 {\"datatype\":\"_IRI\",
                                  \"object\":\"obo:bfo/axiom/033-002\",
                                  \"meta\":\"owl:Axiom\"},
                                 {\"datatype\":\"_IRI\",
                                  \"meta\":\"owl:Axiom\",
                                  \"object\":\"obo:bfo/axiom/033-003\"}]}"

        s2 "{\"owl:someValuesFrom\":[{\"datatype\":\"_IRI\",\"object\":\"obo:OBI_0001043\"}],
             \"owl:onProperty\":[{\"datatype\":\"_IRI\",\"object\":\"obo:RO_0000085\"}],
             \"rdf:type\":[{\"datatype\":\"_IRI\",\"object\":\"owl:Restriction\"}], 
             \"obo:IAO_0010000\":[{\"datatype\":\"_IRI\",\"meta\":\"owl:Axiom\",\"object\":\"obo:bfo/axiom/033-003\"},
                                  {\"datatype\":\"_IRI\",
                                   \"meta\":\"owl:Axiom\",
                                   \"object\":\"obo:bfo/axiom/033-001\"},
                                  {\"meta\":\"owl:Axiom\",
                                   \"datatype\":\"_IRI\",
                                   \"object\":\"obo:bfo/axiom/033-002\"}]}"

        json1 (cs/parse-string s1 true)
        json2 (cs/parse-string s2 true)

        sort1 (t2t/sort-json json1)
        sort2 (t2t/sort-json json2)
        
        ss1 (str sort1)
        ss2 (str sort2)] 
    (is ss1 ss2)))

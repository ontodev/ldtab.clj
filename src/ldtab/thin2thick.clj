(ns ldtab.thin2thick
  (:require [clojure.repl :as repl]
            [clojure.java.io :as io]
            [clojure.set :as s]
            [clojure.string :as string]
            [ldtab.parse :as parse]
            [ldtab.annotation-handling :as ann]
            [cheshire.core :as cs])
  (:import [org.apache.jena.graph NodeFactory Triple] 
           [java.io InputStream FileInputStream]
           [org.apache.jena.riot RDFDataMgr Lang])
  (:gen-class))

(declare node-2-thick-map)

;TODO: add support for user input prefixes
(defn curify
  [s]
  (let [owl (string/replace s #"http://www.w3.org/2002/07/owl#" "owl:") 
        rdf (string/replace owl #"http://www.w3.org/1999/02/22-rdf-syntax-ns#" "rdf:") 
        rdfs (string/replace rdf #"http://www.w3.org/2000/01/rdf-schema#" "rdfs:")]
    rdfs)) 

(defn map-on-hash-map-vals
  "Given a hashmap m and a function f, 
  apply f to all values of m.
  Example:
  Given m = {:a 1, :b 2}  and f = (fn [x] (inc x)),
  then (map-on-hash-map-vals f m) = {:a 2, :b 3}"
  [f m]
  (zipmap (keys m) (map f (vals m)))) 

(defn map-on-hash-map-keys
  [f m]
  (zipmap (map f (keys m)) (vals m)))


(defn is-rdf-type?
  [string]
  (or (= string "rdf:type")
      (= string "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")
      (= string "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")))

(defn get-type
  [triples]
  (let [typing-triples (filter #(is-rdf-type? (.getURI (.getPredicate %))) triples)
        number-of-types (count typing-triples)]
    (cond
      (= number-of-types 0) (NodeFactory/createURI "unknown")
      (= number-of-types 1) (.getObject (first typing-triples))
      :else (NodeFactory/createURI "ambiguous"))))


(defn encode-blank-nodes
  [triples]
  """Given a set of triples,
    identify root blank nodes and add triples of the form

    [wiring:blanknode:id type _:blankNode]

    where 'wiring:blanknode:id' is a newly generated subject,
    type is the rdf:type of the identified root _:blankNode,
    and _:blankNode is the root node. 

    For example given the triples:

       [_:B, obo:IAO_0010000, obo:050-003]
       [_:B, owl:annotatedTarget, 'target']
       [_:B, owl:annotatedProperty, obo:IAO_0000602]
       [_:B, owl:annotatedSource, obo:BFO_0000020]
       [_:B, rdf:type, owl:Axiom]

    the following triple would be added:

       [wiring:blanknode:1, rdf:type, _:B] 

    Explanation:
    We collapse blank nodes into JSON maps.
    However, for root blank nodes, this yields a JSON map that is not part of a triple.
    So, we artificially create these triples by introducing 'dummy blank nodes' (that are not treated as blank nodes by the implementation).
    """
  (let [subject-to-triples (group-by #(.getSubject %) triples)
        subjects (set (map #(.getSubject %) triples))
        objects (set (map #(.getObject %) triples))
        root (s/difference subjects objects)
        blank-roots (filter #(.isBlank %) root)

        additions (map #(new Triple (NodeFactory/createURI (str "wiring:blanknode:" (gensym))) 
                                    ;(NodeFactory/createURI "wiring:blanknode") 
                                    (get-type (get subject-to-triples %)) 
                                    %) blank-roots)] 

    (concat triples additions)))

(defn get-datatype 
  [node]
  (cond 
    (.isBlank node) "_json"
    (.isURI node) "_IRI"
    ;NB: Jena can't identify plain literals
    (.isLiteral node) (str (.getLiteralDatatypeURI node)
                           (.getLiteralLanguage node))
    :else "ERROR"))

;TODO: stringify objects  (+ extract literal values from literals)
(defn encode-object
  "Given a triple t = [s p o] and a map from subject nodes to its triples,
  returns predicate map for the o" 
  [triple subject-2-thin-triples]
  (hash-map :object (node-2-thick-map (.getObject triple) subject-2-thin-triples),
            :datatype (get-datatype (.getObject triple))))

(defn encode-node
  "Given a Jena Node, return String for 
  1. URIs (:TODO use CURIES?) 
  2. Literal Value for Literals"
  [node]
  (cond
    (.isURI node) (curify (.getURI node)) ;TODO
    ;NB: Jena can't identify plain literals
    (.isLiteral node) (.getLiteralValue node)
    :else "ERROR"))


(defn node-2-thick-map
  "Given a node and a map from subject nodes to its triples,
    returns a predicate map if its a blank node
    and itself otherwise" 
  [node subject-2-thin-triples]
  ;(println node)
  (if (.isBlank node)
    (let [triples (get subject-2-thin-triples node)
          predicates (group-by #(.getPredicate %) triples)
          predicates (map-on-hash-map-keys encode-node predicates)] 
      (map-on-hash-map-vals ;encode objects recursively
        #(into [] (map (fn [x] (encode-object x subject-2-thin-triples)) %))
        predicates)) 
    (encode-node node)))

(defn root-triples
  "Given a set of thin triples,
    return all triples that are at the root of a blank node dependency chain, i.e.,
    a triple s.t. its subject is not a blank node that
    occurs as an object in another triple."
  [triples]
  (let [subjects (set (map #(.getSubject %) triples))
        objects (map #(.getObject %) triples)
        object-blanknode (set (filter #(.isBlank %) objects))
        root (s/difference subjects object-blanknode)
        root-triples (filter #(contains? root (.getSubject %)) triples)]
    root-triples))

;NB: sorting transfoms keywords to strings 
(defn sort-json
  "Given a JSON value, return a lexicographically ordered representation."
  [m]
    (cond
      (map? m) (into (sorted-map) (map-on-hash-map-vals sort-json m)) ;sort by key
      (coll? m) (into [] (map cs/parse-string ;sort by string comparison
                              (sort (map #(cs/generate-string (sort-json %))
                                         m))))
      :else m))

(defn map-subject-2-thin-triples
  "Given a set of thin triples,
    return a map from subjects to thin triples."
  [thin-triples]
  (group-by #(.getSubject %) thin-triples)) 

(defn thin-2-thick-raw
  ([triples]
   """Given a set of thin triples, return the corresponding set of (raw) thick triples."""
   (let [blank-node-encoding (encode-blank-nodes triples) 
         subject-2-thin-triples (map-subject-2-thin-triples blank-node-encoding)
         root-triples (root-triples blank-node-encoding) 
         thick-triples (map #(thin-2-thick-raw % subject-2-thin-triples) root-triples)]
     thick-triples))
  ([triple subject-2-thin-triples]
  """Given a thin triple t and a map from subjects to thin triples,
    return t as a (raw) thick triple."""
  (let [s (.getSubject triple)
        p (.getPredicate triple)
        o (.getObject triple) 
        subject (node-2-thick-map s subject-2-thin-triples)
        predicate (node-2-thick-map p subject-2-thin-triples)
        object (node-2-thick-map o subject-2-thin-triples)]
    {:subject subject, :predicate predicate, :object object, :datatype (get-datatype o)}))) 

(defn is-meta-statement
  [rawThickTriple]
  (let [predicate (:predicate rawThickTriple)]
    (if (.isURI predicate)
      (let [uri (.getURI predicate)]
        (or (= uri "http://www.w3.org/2002/07/owl#Annotation")
            (= uri "http://www.w3.org/2002/07/owl#Axiom")
            (= uri "http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement")))
      nil)))

(defn thin-2-thick
  [triples]
  (let [raw-thick-triples (thin-2-thick-raw triples)
        annotations (map #(if (is-meta-statement %)
                              (ann/encode-raw-annotation-map (:object %)) ;;TODO
                              %) raw-thick-triples) 
        sorted (map sort-json annotations)
        normalised (map #(cs/parse-string (cs/generate-string %)) sorted)];TODO: stringify keys - this is a (probably an inefficient?) workaround 
    normalised))


(defn -main
  "Currently only used for manual testing."
  [& args]
  ;testing
  (let [rdf-path (first args) 
        is (new FileInputStream rdf-path)
        it (RDFDataMgr/createIteratorTriples is Lang/RDFXML "base")
        windowsize 50]
    (loop [backlog '()]
      (when (.hasNext it) 
        (let [[thin kept thick] (parse/parse-window it windowsize backlog)
              ;encoded (map encode-blank-nodes thick) 
              ;root-triples (map root-triples encoded)
              raw (map #(first (thin-2-thick-raw %)) thick)
              ex (:subject (first raw))
              ]
          ;(when ex
          ;  (when (.isURI ex)
          ;    (println (.getURI ex))))
          ;(when ex
          ;  (when (some is-meta-statement raw) ;TODO: can't just look at the firs tone (need a map
          ;    (println raw)))
          (println raw)

          (recur kept)))))


)



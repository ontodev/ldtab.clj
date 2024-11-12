(ns ldtab.thin2thick
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [ldtab.annotation-handling :as ann]
            [ldtab.rdf-list-handling :as rdf-list]
            [ldtab.gci-handling :as gci]
            [cheshire.core :as cs])
  (:import [org.apache.jena.graph NodeFactory Triple Node])
           ;[org.apache.jena.rdf.model ModelFactory Model StmtIterator Resource Property RDFNode Statement])
  (:gen-class))

(declare node-2-thick-map)

(defn is-wiring-blanknode
  [input]
  (and (string? input)
       (str/starts-with? input "<wiring:blanknode")))

(defn hash-existential-subject-blanknode
  [triple]
  (if (is-wiring-blanknode (:subject triple))
    (assoc triple
           :subject
           (str  "<wiring:blanknode:" (hash (:object triple)) ">"))
    triple))

;TODO: add support for user input prefixes (using prefix table)
(defn curify
  [^String s]
  (let [owl (str/replace s #"http://www.w3.org/2002/07/owl#" "owl:")
        rdf (str/replace owl #"http://www.w3.org/1999/02/22-rdf-syntax-ns#" "rdf:")
        rdfs (str/replace rdf #"http://www.w3.org/2000/01/rdf-schema#" "rdfs:")]
    rdfs))

(defn curify-with
  [^String uri iri2prefix]
  (let [matches (filter #(str/starts-with? uri (:base %)) iri2prefix)
        sorted (sort-by #(count (:base %)) matches) ;sort by length
        found (last sorted)] ;get longest match
    (if found
      (str/replace uri (:base found) (str (:prefix found) ":"))
      (str "<" uri ">"))))

(defn map-on-hash-map-vals
  "Given a hashmap m and a function f, 
  apply f to all values of m.
  Example:
  Given m = {:a 1, :b 2}  and f = (fn [x] (inc x)),
  then (map-on-hash-map-vals f m) = {:a 2, :b 3}"
  [f m]
  (zipmap (keys m) (map f (vals m))))

(defn map-on-hash-map-keys
  "Given a hashmap m and a function f, 
  apply f to all keys of m."
  [f m]
  (zipmap (map f (keys m)) (vals m)))

(defn is-rdf-type?
  [^String string]
  (or (= string "rdf:type")
      (= string "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")
      (= string "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")))

(defn get-type
  [triples]
  (let [typing-triples (filter (fn [^Triple x] (is-rdf-type? (.getURI (.getPredicate x)))) triples)
        number-of-types (count typing-triples)]
    (cond
      (= number-of-types 0) (NodeFactory/createURI "unknown")
      (= number-of-types 1) (.getObject ^Triple (first typing-triples))
      :else (NodeFactory/createURI "ambiguous"))))

;TODO handling of 'existential blank nodes' appearing as objects
;TODO rename to 'skolemise-existential-blank-nodes'
(defn encode-blank-nodes
  "Given a set of triples,
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
    So, we artificially create these triples by introducing 'dummy blank nodes' (that are not treated as blank nodes by the implementation)."
  [triples]
  (let [subject-to-triples (group-by (fn [^Triple x] (.getSubject x)) triples)
        subjects (set (map (fn [^Triple x] (.getSubject x)) triples))
        objects (set (map (fn [^Triple x] (.getObject x)) triples))
        root (set/difference subjects objects)
        blank-roots (filter (fn [^Node x] (.isBlank x)) root)
        ;TODO blank-leaves also need to be skolemised:
        ;for a given blank-leaf [s p _b:leaf] 
        ;we need to add the triple [_b:leaf rdf:type wiring:blanknode]
        ;so that we collapse the blank node into it's skolem form

        additions (map (fn [^Node x] (new Triple (NodeFactory/createURI (str "wiring:blanknode:" (gensym)))
                                    ;(NodeFactory/createURI "wiring:blanknode") 
                                          (get-type (get subject-to-triples x))
                                          x)) blank-roots)]

    (concat triples additions)))

(defn get-datatype
  ([^Node node]
   (cond
     (.isBlank node) "_JSONMAP"
     (.isURI node) "_IRI"
    ;NB: Jena can't identify plain literals
     (.isLiteral node) (let [datatype (.getLiteralDatatypeURI node)
                             language (.getLiteralLanguage node)]
                         (if-not (= language "")
                           (str "@" language)
                           datatype))
     :else "ERROR"))
  ([^Node node iri2prefix]
   (cond
     (.isBlank node) "_JSONMAP"
     (.isURI node) "_IRI"
     ;NB: Jena can't identify plain literals
     (.isLiteral node) (let [datatype (curify-with (.getLiteralDatatypeURI node) iri2prefix)
                             language (.getLiteralLanguage node)]
                         (if-not (= language "")
                           (str "@" language)
                           datatype))
     :else "ERROR")))

(defn encode-object
  "Given a triple t = [s p o] and a map from subject nodes to its triples,
  returns predicate map for the o"
  ([^Triple triple subject-2-thin-triples]
   (hash-map :object (node-2-thick-map (.getObject triple) subject-2-thin-triples),
             :datatype (get-datatype (.getObject triple))))
  ([^Triple triple subject-2-thin-triples iri2prefix]
   (hash-map :object (node-2-thick-map (.getObject triple) subject-2-thin-triples iri2prefix),
             :datatype (get-datatype (.getObject triple) iri2prefix))))

(defn encode-node
  "Given a Jena Node, return String for 
  1. URIs 
  2. Literal Value for Literals"
  ([^Node node]
   (cond
     (.isURI node) (curify (.getURI node))
     (.isLiteral node) (.getLiteralLexicalForm node)
     :else "ERROR"))
  ([^Node node iri2prefix]
   (cond
     (.isURI node) (curify-with (.getURI node) iri2prefix)
     (.isLiteral node) (.getLiteralLexicalForm node)
     :else "ERROR")))

(defn node-2-thick-map
  "Given a node and a map from subject nodes to its triples,
  returns a predicate map if its a blank node
  and itself otherwise"
  ([^Node node subject-2-thin-triples]
   (if (.isBlank node)
     (let [triples (get subject-2-thin-triples node)
           predicates (group-by (fn [^Triple x] (.getPredicate x)) triples)
           predicates (map-on-hash-map-keys encode-node predicates)]
       (map-on-hash-map-vals ;encode objects recursively
        #(vec (map (fn [^Triple x] (encode-object x subject-2-thin-triples)) %))
        predicates))
     (encode-node node)))
  ([^Node node subject-2-thin-triples iri2prefix]
   (if (.isBlank node)
     (let [triples (get subject-2-thin-triples node)
           predicates (group-by (fn [^Triple x] (.getPredicate x)) triples)
           predicates (map-on-hash-map-keys #(encode-node % iri2prefix) predicates)]
       (map-on-hash-map-vals ;encode objects recursively
        #(vec (map (fn [^Triple x] (encode-object x subject-2-thin-triples iri2prefix)) %))
        predicates))
     (encode-node node iri2prefix))))

(defn root-triples
  "Given a set of thin triples,
    return all triples that are at the root of a blank node dependency chain, i.e.,
    a triple s.t. its subject is not a blank node that
    occurs as an object in another triple."
  [triples]
  (let [subjects (set (map (fn [^Triple x] (.getSubject x)) triples))
        objects (map (fn [^Triple x] (.getObject x)) triples)
        object-blanknode (set (filter (fn [^Node x] (.isBlank x)) objects))
        root (set/difference subjects object-blanknode)
        root-triples (filter (fn [^Triple x] (contains? root (.getSubject x))) triples)]
    root-triples))

;NB: sorting transfoms keywords to strings 
(defn sort-json
  "Given a JSON value, return a lexicographically ordered representation."
  [m]
  (cond
    ; sort RDF lists
    (and (map? m)
         (contains? m :datatype)
         (= (:datatype m) "_JSONLIST"))
    (let [sorted-list {:datatype "_JSONLIST", :object (map sort-json (:object m))}]
      (if (contains? m :subject) ; top-level RDF list
        (into (sorted-map) (merge sorted-list
                                  {:subject (sort-json (:subject m))
                                   :predicate (:predicate m)
                                   :graph (:graph m)
                                   :assertion (:assertion m)
                                   :retraction (:retraction m)
                                   :annotation (:annotation m)}))
        (into (sorted-map) sorted-list))); nested RDF list

    (map? m)
    (into (sorted-map) (map-on-hash-map-vals sort-json m)) ; sort by key

    (coll? m)
    (vec (map cs/parse-string ; sort by string comparison
              (sort (map #(cs/generate-string (sort-json %)) m))))

    :else
    m))

(defn map-subject-2-thin-triples
  "Given a set of thin triples,
    return a map from subjects to thin triples."
  [thin-triples]
  (group-by (fn [^Triple x] (.getSubject x)) thin-triples))

(defn thin-2-thick-triple-raw
  "Given a root thin triple t (see function root-triples) and a map from subjects to thin triples in an RDF graph G,
    return the (raw) thick triple of t in G."
  ([^Triple triple subject-2-thin-triples]
   (let [s (.getSubject triple)
         p (.getPredicate triple)
         o (.getObject triple)
         subject (node-2-thick-map s subject-2-thin-triples)
         predicate (node-2-thick-map p subject-2-thin-triples)
         object (node-2-thick-map o subject-2-thin-triples)]
     {:subject subject, :predicate predicate, :object object, :datatype (get-datatype o)}))
  ([^Triple triple subject-2-thin-triples iri2prefix]
   (let [s (.getSubject triple)
         p (.getPredicate triple)
         o (.getObject triple)
         subject (node-2-thick-map s subject-2-thin-triples iri2prefix)
         predicate (node-2-thick-map p subject-2-thin-triples iri2prefix)
         object (node-2-thick-map o subject-2-thin-triples iri2prefix)]
     {:subject subject, :predicate predicate, :object object, :datatype (get-datatype o iri2prefix)})))

(defn thin-2-thick-raw
  "Given a set of thin triples, collapse blank nodes into RDF Thick Triples (with datatypes)."
  ([triples]
   (let [blank-node-encoding (encode-blank-nodes triples)
         subject-2-thin-triples (map-subject-2-thin-triples blank-node-encoding)
         root-triples (root-triples blank-node-encoding)
         thick-triples (map (fn [^Triple x] (thin-2-thick-triple-raw x subject-2-thin-triples)) root-triples)]
     thick-triples))
  ([triples iri2prefix]
   (let [blank-node-encoding (encode-blank-nodes triples)
         subject-2-thin-triples (map-subject-2-thin-triples blank-node-encoding)
         root-triples (root-triples blank-node-encoding)
         thick-triples (map (fn [^Triple x] (thin-2-thick-triple-raw x subject-2-thin-triples iri2prefix)) root-triples)]
     thick-triples)))

(defn thin-2-thick
  ([triples]
   (let [raw-thick-triples (thin-2-thick-raw triples)
        ;TODO I am requiring the use of CURIEs for owl, rdf, and rdfs
         gcis (map gci/encode-raw-gci-map raw-thick-triples)
         annotations (map #(if (or (= (:predicate %) "owl:Annotation")
                                   (= (:predicate %) "owl:Axiom");NOTE: this states a triple
                                   (= (:predicate %) "rdf:Statement"))
                             (ann/encode-raw-annotation-map (:object %))
                             %) gcis)
         rdf-lists (map rdf-list/encode-rdf-list annotations)
         sorted (map sort-json rdf-lists)
         hashed (map hash-existential-subject-blanknode sorted)
         normalised (map #(cs/parse-string (cs/generate-string %)) hashed)];TODO: stringify keys - this is a (probably an inefficient?) workaround 
     normalised))
  ([triples iri2prefix]
   (let [raw-thick-triples (thin-2-thick-raw triples iri2prefix)
         ;TODO I am requiring the use of CURIEs for owl, rdf, and rdfs
         ;TODO: check for annotated GCIs
         gcis (map gci/encode-raw-gci-map raw-thick-triples)
         annotations (map #(if (or (= (:predicate %) "owl:Annotation")
                                   (= (:predicate %) "owl:Axiom")
                                   (= (:predicate %) "rdf:Statement"))
                             (ann/encode-raw-annotation-map (:object %))
                             %) gcis)
         rdf-lists (map rdf-list/encode-rdf-list annotations)
         sorted (map sort-json rdf-lists)
         hashed (map hash-existential-subject-blanknode sorted)
         normalised (map #(cs/parse-string (cs/generate-string %)) hashed)];TODO: stringify keys - this is a (probably an inefficient?) workaround 
     normalised)))

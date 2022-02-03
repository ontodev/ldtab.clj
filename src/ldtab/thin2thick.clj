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



(defn is-rdf-literal?
  [string]
  (when (string? string)
    (re-matches #"^\".*\".*$" string))) 

;TODO do this with jena
(defn get-literal-string
  [string]
 (second (re-matches #"^\"(.*)\".*$" string)))

(defn has-language-tag?
  [literal]
  (re-matches #"^\"(.*)\"(@.*)$" literal))

(defn has-datatype?
  [literal]
  (re-matches #"^\"(.*)\"(\^\^.*)$" literal))


(declare node-2-thick-map)

(defn map-on-hash-map-vals
  "Given a hashmap m and a function f, 
  apply f to all values of m.
  Example:
  Given m = {:a 1, :b 2}  and f = (fn [x] (inc x)),
  then (map-on-hash-map-vals f m) = {:a 2, :b 3}"
  [f m]
  (zipmap (keys m) (map f (vals m)))) 


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
                                    (NodeFactory/createURI "wiring:blanknode") 
                                    %) blank-roots)] 

    (concat triples additions)))

(defn encode-object
  "Given a triple t = [s p o] and a map from subject nodes to its triples,
  returns predicate map for the o" 
  [triple subject-2-thin-triples]
  (hash-map :object (node-2-thick-map (.getObject triple) subject-2-thin-triples)))

(defn node-2-thick-map
  "Given a node and a map from subject nodes to its triples,
    returns a predicate map if its a blank node
    and itself otherwise" 
  [node subject-2-thin-triples]
  ;(println node)
  (if (.isBlank node)
    (let [triples (get subject-2-thin-triples node)
          predicates (group-by #(.getPredicate %) triples)]; create predicate map 
      ;(println predicates)
      (map-on-hash-map-vals ;encode objects recursively
        #(into [] (map (fn [x] (encode-object x subject-2-thin-triples)) %))
        predicates)) 
    node)) 

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
    {:subject subject, :predicate predicate, :object object}))) 


(declare encode-literals)

(defn encode-literal
  "Encode a single literal as described by 'encode-literals'"
  [predicate-map] 
  (let [object (:object predicate-map)
        literal-value (get-literal-string object)
        language-tag (has-language-tag? object)
        datatype (has-datatype? object)
        has-thick-datatype (or language-tag datatype)]
    (if has-thick-datatype
      (assoc predicate-map :object literal-value
             :datatype (nth has-thick-datatype 2))
      (assoc predicate-map :object literal-value
                           :datatype "_plain"))))

(defn handle-object
  "Given a predicate map, update its JSON structure by
  (1) encoding an RDF literal if it occurs as an :object,  
  (2) recurse if :object is an JSON object or array 
  (3) return the predicate-map otherwise."
  [predicate-map]
    (let [object (:object predicate-map)]
      (cond (is-rdf-literal? object) (encode-literal predicate-map) 
            ;recurse
            (map? object) (update predicate-map :object encode-literals) 
            (coll? object) (update predicate-map :object #(map encode-literals %))
            ;base case
            :else predicate-map)))

;NB thick-triple has to be passed around instead of (:object thick-triple)
;because we need to modify the map 'thick-triple' and not just (:object thick-triple)
(defn encode-literals
  "Given a thick-triple,
  search for RDF literals that appear as :object in the thick-triple,
  extract their datatypes and language tags (if present), and
  encode them as :datatype in the corresponding predicate map.
  Example:
  {:subject a:Peter, :predicate rdfs:label, :object \"Peter Griffin\"@en}
  will be encoded as 
  {:subject a:Peter, :predicate rdfs:label, :object \"Peter Griffin\" :datatype @en}."
  [thick-triple]
  (if (map? thick-triple)
    ;predicate maps are only changed in case an :object contains an RDF literal
    ;otherwise, the algorithm only needs to traverse the whole JSON structure
    (let [object-handled (handle-object thick-triple)
          rest-handled (map-on-hash-map-vals #(cond ;JSON traversal
                                                (map? %) (encode-literals %)
                                                (coll? %) (into [] (map encode-literals %))
                                                :else %)
                                             (dissoc thick-triple :object))]
      (if (:object object-handled)
        (let [encoding (assoc rest-handled :object (:object object-handled))]
          (if (contains? object-handled :datatype)
            (assoc encoding :datatype (:datatype object-handled))
            encoding))
        rest-handled))
    thick-triple)) 

(defn encode-json-objects
  "Given a predicate map m, associate :objects with _json datatypes"
  [m]
    (cond
      (map? (:object m)) (map-on-hash-map-vals encode-json-objects (assoc m :datatype "_json")) 
      (map? m) (map-on-hash-map-vals encode-json-objects m) 
      (coll? m) (map encode-json-objects m) 
      :else m))

;TODO: this should be unnecessary when working with Jena
(defn encode-iris
  "Given a predicate map m, associate :objects with _iri datatypes"
  [m]
    (cond
      (and
        (map? m)
        (contains? m :object)
        (not (contains? m :datatype))) (map-on-hash-map-vals encode-iris (assoc m :datatype "_iri")) 
      (map? m) (map-on-hash-map-vals encode-iris m) 
      (coll? m) (map encode-iris m) 
      :else m))

(defn thin-2-thick
  [triples]
  (let [raw-thick-triples (thin-2-thick-raw triples)
        annotations (map #(if (or (= (:predicate %) "owl:Annotation")
                                    (= (:predicate %) "owl:Axiom")
                                    (= (:predicate %) "rdf:Statement"))
                              (ann/encode-raw-annotation-map (:object %)) 
                              %) raw-thick-triples)
        literals (map encode-literals annotations)
        json-maps (map encode-json-objects literals)
        iris (map encode-iris json-maps)
        sorted (map sort-json iris)
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
              
              ]
          (println raw)

          (recur kept)))))


)



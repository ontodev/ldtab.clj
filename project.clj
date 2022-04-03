(defproject ldtab "0.1.0-SNAPSHOT"
  :description "LDTab is a tool for working with RDF Datasets and OWL using SQL databases."
  :url "https://github.com/ontodev/ldtab.clj"
  :license {:name "BSD 3-Clause License"
            :url "https://opensource.org/licenses/BSD-3-Clause"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.clojure/data.csv "1.0.0"] 
                 [org.clojure/tools.cli "1.0.206"]
                 [cheshire "5.10.0"]
                 [org.xerial/sqlite-jdbc "3.36.0"]
                 [org.apache.jena/jena-core "4.4.0"]
                 [org.apache.jena/jena-arq "4.4.0"]
                 [org.apache.jena/jena-iri "4.4.0"]]
  :plugins [[lein-cljfmt "0.7.0"]
            [lein-kibit "0.1.8"]] 
  :main ^:skip-aot ldtab.cli
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})

(defproject wiring "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "BSD 3-Clause License"
            :url "https://opensource.org/licenses/BSD-3-Clause"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.clojars.chriskind/wiring "0.1.0-SNAPSHOT"]
                 [org.xerial/sqlite-jdbc "3.36.0"]
                 [org.apache.jena/jena-core "3.2.0"]
                 [org.apache.jena/jena-arq "3.2.0"]
                 [org.apache.jena/jena-iri "3.2.0"]
                 ;[org.apache.jena/jena-tdb "3.2.0"] 
                 [org.clojure/data.csv "1.0.0"] 
                 [org.clojure/tools.cli "1.0.206"] 
                 ]
  :plugins [[lein-cljfmt "0.7.0"]] 
  ;:main ^:skip-aot ldtab.core
  ;:main ^:skip-aot ldtab.init
  ;:main ^:skip-aot ldtab.prefix
  :main ^:skip-aot ldtab.cli
  ;:main ^:skip-aot ldtab.jena
  ;:main ^:skip-aot ldtab.parse-alternative
  ;:main ^:skip-aot ldtab.parse
  ;:main ^:skip-aot ldtab.import
  ;:main ^:skip-aot ldtab.thin2thick
  ;:main ^:skip-aot wiring.thick2ofn.axiomTranslation.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})

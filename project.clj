(defproject kafka-component "0.6.3"
  :description "A kafka component to consume from Kafka"
  :url "https://github.com/Mayvenn/kafka-component"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.stuartsierra/component "0.2.2"]
                 [io.weft/gregor "0.6.0"]
                 [org.clojure/core.async "0.4.474"]]
  :codox {:source-paths ["src"]
          :source-uri "http://github.com/Mayvenn/kafka-component/blob/master/{filepath}#L{line}"
          :metadata {:doc/format :markdown}
          :doc-files ["README.md"]}
  :profiles
  {:uberjar {:aot :all}
   :dev {:source-paths ["dev"]
         :dependencies [[diff-eq "0.2.2"]
                        [org.clojure/tools.namespace "0.2.9"]
                        [embedded-kafka "0.6.0"]]
         :plugins [[lein-cljfmt "0.3.0"]
                   [lein-codox "0.10.2"]]
         :injections [(require 'diff-eq.core)
                      (diff-eq.core/diff!)]}})

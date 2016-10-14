(defproject kafka-component "0.5.0"
  :description "A kafka component to consume from Kafka"
  :url "https://github.com/Mayvenn/kafka-component"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.stuartsierra/component "0.2.2"]
                 [io.weft/gregor "0.5.1"]
                 [org.clojure/core.async "0.2.391"]]
  :profiles
  {:uberjar {:aot :all}
   :dev {:source-paths ["dev"]
         :dependencies [[diff-eq "0.2.2"]
                        [standalone-test-server "0.5.0"]
                        [org.clojure/tools.namespace "0.2.9"]
                        [embedded-kafka "0.4.0"]]
         :plugins [[lein-cljfmt "0.3.0"]]
         :injections [(require 'diff-eq.core)
                      (diff-eq.core/diff!)]}})

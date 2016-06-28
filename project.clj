(defproject kafka-component "0.2.2"
  :description "A kafka component to consume from Kafka"
  :url "https://github.com/Mayvenn/kafka-component"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.stuartsierra/component "0.2.2"]
                 [clj-kafka "0.3.2"]])

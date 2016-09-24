(ns kafka-component.core-test
  (:require [kafka-component.core :refer :all]
            [embedded-kafka.core :as ek]
            [com.stuartsierra.component :as component]
            [clojure.test :refer :all]
            [gregor.core :as gregor]))

(defn test-system []
  (let [messages (promise)]
    (component/system-using
     (component/system-map
      :messages messages
      :message-consumer {:consumer (partial deliver messages)}
      :producer-component (->KafkaProducerComponent ek/kafka-config nil)
      :consumer-component (->KafkaConsumerPool
                           {:kafka-consumer-config ek/kafka-config
                            :topics-or-regex ["test-topic"]
                            :pool-size 1}
                           {}
                           println
                           println
                           nil))
     {:consumer-component {:consumer-component :message-consumer}})))

(defmacro with-resource
  [bindings close-fn & body]
  `(let ~bindings
     (try
       ~@body
       (finally
         (~close-fn ~(bindings 0))))))

(defmacro with-test-system
  [sys & body]
  `(ek/with-test-broker producer# consumer#
     (with-resource [system# (component/start (test-system))]
       component/stop
       (let [~sys system#]
         ~@body))))

(deftest sending-and-receiving-messages-using-kafka
  (with-test-system {:keys [messages producer-component]}
    (.get (gregor/send (:producer producer-component) "test-topic" "key" "yolo")) 
    (is (= {:topic "test-topic" :partition 0 :key "key" :offset 0 :value "yolo"}
           (deref messages 10000 [])))))

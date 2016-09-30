# kafka-component

Provides a Kafka consumption pool component to work with Stuart Sierra's
component.

The consumer component will manually commit offsets after each message has been
processed, instead of relying on `auto.commit.enable`. To avoid a thread
committing another thread's offsets, each thread receives its own kafka
consumer.

# Installation

Add to your dependencies in your `project.clj`:

```clojure
[kafka-component "0.2.7"]
```

# Usage

Use `KafkaProducer` and `KafkaConsumerPool`:

```clojure
(ns myapp
  (:require [kafka-component.core :refer [->KafkaProducer ->KafkaConsumerPool make-default-producer make-default-consumer]]
            [gregor.core :as gregor]))

;; Producing messages

(def producer (->KafkaProducer {"metadata.broker.list" "localhost:9092"} make-default-producer))

(gregor/send producer "topic" "message-key" "message-body")

;; Consuming messages

(def config
  {:shutdown-grace-period 4
   :kafka-consumer-config {"group.id" "myapp"
                           "auto.offset.reset" "latest"
                           "auto.commit.enable" "false"}})

(defrecord ConsumerComponent [consumer])

(def consumer (map->KafkaConsumerPool {:config config
                                       :pool-size 2
                                       :topic "topic"
                                       :consumer-component (->ConsumerComponent (fn [kafka-msg] (println "Received message")))
                                       :logger (fn [level msg] (println level msg))
                                       :exception-handler (fn [e] (.printStackTrace e))
                                       ;; change this to mock-consumer for tests
                                       :make-kafka-consumer make-default-consumer}))

```

# Testing

You can also use specically designed mock implementations for tests. Use
`mock-producer` and `mock-consumer`. Mocks emulate kafka in-memory without the
large startup overhead.

```clojure
(ns myapp.tests
  (:require [kafka-component.mock :refer [mock-producer mock-consumer fixture-restart-broker get-messages]]
            [clojure.test :refer :all]))
            
(use-fixture :each fixture-restart-broker)
            
(deftest test
  (let [producer (mock-producer {})
        consumer (mock-consumer {})]

    ;; ...use mocks in app...

    ;; tell mock consumer to subscribe to topic
    (.subscribe consumer ["topic"])

    ;; tell mock producer to send a message on a kafka queue
    @(.send producer (producer-record "topic" "key" "value"))

    ;; read from the mock consumer
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
            (get-messages consumer timeout)))))
```

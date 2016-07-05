# kafka-component

Provides a Kafka consumption pool component to work with Stuart Sierra's component.

The consumer component will manually commit offsets after each message has been processed, instead of relying on `auto.commit.enable`. To avoid a thread committing another thread's offsets, each thread receives its own kafka consumer.

# Installation

Add to your dependencies in your `project.clj`:

```clojure
[kafka-component "0.2.6"]
```

# Usage

Use `KafkaProducer` and `KafkaConsumerPool`:

```clojure
(ns myapp
  (:require [kafka-component.core :refer [->KafkaProducer ->KafkaConsumerPool]]
            [clj-kafka.producer :refer [send-message message]]))

;; Producing messages

(def producer (->KafkaProducer {"metadata.broker.list" "localhost:9092"}))

(send-message producer (message "topic" "message-key" "message-body"))

;; Consuming messages

(def config
  {:shutdown-grace-period 4
   :kafka-consumer-config {"group.id" "myapp"
                           "auto.offset.reset" "largest"
                           "auto.commit.enable" "false"}})

(defrecord ConsumerComponent [consumer])

(def consumer (map->KafkaConsumerPool {:config config
                                       :pool-size 2
                                       :topic "topic"
                                       :consumer-component (->ConsumerComponent (fn [kafka-msg] (println "Received message")))
                                       :logger (fn [level msg] (println level msg))
                                       :exception-handler (fn [e] (.printStackTrace e))}))

```

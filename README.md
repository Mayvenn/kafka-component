# kafka-component

Provides a Kafka consumption pool component to work with Stuart Sierra's
component.

The consumer component will manually commit offsets after each batch of messages
has been processed, instead of relying on `auto.commit.enable`. This library takes
special care to make sure that messages have actually been processed before committing
which the default `auto.commit.enable` doesn't have as strong semantics. To avoid a
thread committing another thread's offsets, each thread receives its own kafka consumer.

# Installation

Add to your dependencies in your `project.clj`:

[![Clojars Project](https://img.shields.io/clojars/v/kafka-component.svg)](https://clojars.org/kafka-component)

# Usage

Use `KafkaWriter` and `KafkaReader` in a system:

```clojure
(ns myapp
  (:require [kafka-component.core :as kafka-component]
            [com.stuartsierra.component :as component])

(def config
  {:writer-config {:native-producer-overrides {"bootstrap.servers" "localhost:9092"}}
   :reader-config {:shutdown-timeout          4 ; default
                   :concurrency-level         2
                   :topics                    ["topic_one" "or_more"]
                   :native-consumer-overrides {"bootstrap.servers" "localhost:9092"
                                               "group.id"           "myapp"
                                               "auto.offset.reset" "largest"}}})

(defn create-system [config]
  (component/system-using
    (component/system-map
      :logger            println
      :exception-handler (fn [e] (.printStackTrace e))
      :record-processor  {:process (fn [{:keys [key value]}] (println "Received message: " value))}
      :reader            (kafka-component/map->KafkaReader (config :reader-config))
      :writer            (kafka-component/map->KafkaWriter (config :writer-config)))))
    {:reader [:logger :exception-handler :record-processor]}))
```

## Produce a message

```clojure
(let [{:keys [writer]} (component/start (create-system config))]
  (kafka-component/write writer "topic_one" "message-key" "message-body"))
```

It is also possible to `kakfka-component/write-async`.

## Consume a mesage

Because reader is listening to "topic_one", it will deliver a message to the record-processor:

```
Received message: message-body
```

# Testing

You can use specically designed mock implementations for tests. The mocks
emulate kafka, in-memory without the large startup overhead.

```clojure
(ns myapp.tests
  (:require [kafka-component.mock :as kafka-mock]
            [clojure.test :refer :all]))
            
(deftest basic-produce-consume
  (kafka-mock/with-test-producer-consumer producer consumer
    ;; tell mock producer to send a message on a kafka queue
    (kafka-mock/send producer "topic" "key" "value")

    ;; tell mock consumer to subscribe to topic
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (kafka-mock/accumulate-messages consumer "topic" {:timeout 1000})))))

(deftest filtering-messages
  (mock/with-test-producer-consumer producer consumer
    (dotimes [n 100]
      (mock/send-async producer "topic" "key" (str n)))

    ;; transform and filter messages
    (is (= [1 3]
           (take 2 (mock/accumulate-messages consumer "topic" {:timeout    1000
                                                               :format-fn  (comp inc #(Integer/parseInt %) :value)
                                                               :filter-fn  odd?
                                                               :at-least-n 2}))))))
```

It is also possible to `kafka-mock/send-async`.

The producer and consumer created by `with-test-producer-consumer` run outside of
a system. To use the mocks *inside* a system, modify the system config:

```clojure
(def test-config
  (-> myapp/config
      (assoc-in [:writer-config :native-producer-type] :mock)
      (assoc-in [:reader-config :native-consumer-type] :mock)
      (assoc-in [:reader-config :native-consumer-overrides "auto.offset.reset"] "earliest")))
```

Usually, you will want to do both.
* If your system has a `reader`, create messages outside of the system with a
  `producer`, then test that the system reads and processes the messages
  correctly.
* If your system has a `writer`, poke your system to produce a message, then
  test that an external `consumer` can read the message.

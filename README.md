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
[kafka-component "0.4.1"]
```

# Usage

Use `KafkaWriter` and `KafkaReader`:

```clojure
(ns myapp
  (:require [kafka-component.core :refer [map->KafkaWriter map->KafkaReader] :as kafka-component])

;; Producing messages

(def producer-config {:native-producer-overrides {"bootstrap.servers" "localhost:9092"}})

(def writer (->KafkaWriter producer-config))

(kafka-component/write writer "topic" "message-key" "message-body")

;; Consuming messages

(def consumer-config
  {:shutdown-timeout 4
   :concurrency-level 1
   :topics ["one" "or_more"]
   :native-consumer-overrides {"group.id" "myapp"
                               "auto.offset.reset" "largest"}})

(def record-processor-component {:process (fn [kafka-msg] (println "Received message"))})

(def consumer (map->KafkaReader (merge consumer-config
                                       {:logger (fn [level msg] (println level msg))
                                        :exception-handler (fn [e] (.printStackTrace e))
                                        :record-processor record-processor-component})))

```

# Testing

You can also use specically designed mock implementations for tests. Use
`mock-producer` and `mock-consumer`. Mocks emulate kafka in-memory without the
large startup overhead.

```clojure
(ns myapp.tests
  (:require [kafka-component.mock :refer [with-test-producer-consumer]]
            [clojure.test :refer :all]))
            
(deftest test
  (with-test-producer-consumer producer consumer

    ;; ...use mocks in app...

    ;; tell mock consumer to subscribe to topic
    (.subscribe consumer ["topic"])

    ;; tell mock producer to send a message on a kafka queue
    @(.send producer (producer-record "topic" "key" "value"))

    ;; read from the mock consumer
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
            (get-messages consumer timeout)))))
```

You can enable mocking in a running system by passing `{:native-consumer-type
:mock}` to the Reader, and `{:native-producer-type :mock}` to the Writer.

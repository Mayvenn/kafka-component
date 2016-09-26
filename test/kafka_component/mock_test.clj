(ns kafka-component.mock-test
  (:require [kafka-component.mock :refer :all]
            [clojure.test :refer :all])
  (:import [org.apache.kafka.clients.producer Producer ProducerRecord RecordMetadata Callback]
           [org.apache.kafka.clients.consumer Consumer ConsumerRecord ConsumerRecords]
           [org.apache.kafka.common TopicPartition]
           [java.util Collection]
           [java.util.regex Pattern]))

(use-fixtures :each fixture-reset-broker-state!)

(def timeout 200)

(defn producer-record
  ([] (producer-record "topic" "key" "value"))
  ([topic k v] (ProducerRecord. topic k v))
  ([topic k v partition] (ProducerRecord. topic (int partition) k v)))

(defn reify-send-callback [cb]
  (reify Callback
    (onCompletion [this metadata ex]
      (cb metadata ex))))

(deftest send-on-producer-returns-a-future-of-RecordMetadata
  (let [producer (mock-producer {})
        res @(.send producer (producer-record "topic" "key" "value"))]
    (is (= RecordMetadata (type res)))
    (is (= "topic" (.topic res)))
    (is (= 0 (.partition res)))
    (is (= 0 (.offset res)))))

(deftest send-on-producer-increments-offset
  (let [producer (mock-producer {})
        res (repeatedly 2 #(.send producer (producer-record)))]
    (is (= [0 1] (map (comp #(.offset %) deref) res)))))

(deftest send-on-producer-with-callback-calls-the-callback
  (let [producer (mock-producer {})
        cb-res (promise)
        cb #(deliver cb-res [%1 %2])
        _ (.send producer (producer-record "topic" "key" "value")
                 (reify-send-callback cb))
        [res ex] (deref cb-res timeout [])]
    (is (= RecordMetadata (type res)))
    (is (= "topic" (.topic res)))
    (is (= 0 (.partition res)))
    (is (= 0 (.offset res)))))

(defn record->clj [record]
  {:value     (.value record)
   :key       (.key record)
   :partition (.partition record)
   :topic     (.topic record)
   :offset    (.offset record)})

(defn records->clj
  ([consumer-records]
   (if consumer-records
     (map record->clj (iterator-seq (.iterator consumer-records)))
     [])))

(defn get-messages [consumer timeout]
  (records->clj (.poll consumer timeout)))

(defn create-mocks []
  [(mock-producer {}) (mock-consumer {})])

(deftest consumer-can-receive-message-sent-after-subscribing
  (let [[producer consumer] (create-mocks)
        _ (.subscribe consumer ["topic"])
        _ (.send producer (producer-record "topic" "key" "value"))
        messages (get-messages consumer timeout)]
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           messages))))

(deftest consumer-can-receive-message-from-different-partitions
  (let [[producer consumer] (create-mocks)
        _ (.subscribe consumer ["topic"])
        _ (.send producer (producer-record "topic" "key" "value" 0))
        _ (.send producer (producer-record "topic" "key" "value" 1))
        messages (get-messages consumer timeout)]
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value" :key "key" :partition 1 :topic "topic" :offset 0}]
           messages))))

(deftest consumer-can-limit-number-of-messages-polled
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "1"})
        _ (.subscribe consumer ["topic"])
        _ (.send producer (producer-record "topic" "key" "value"))
        _ (.send producer (producer-record "topic" "key2" "value2"))]
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (get-messages consumer timeout)))
    (is (= [{:value "value2" :key "key2" :partition 0 :topic "topic" :offset 1}]
           (get-messages consumer timeout)))))

(comment
  (deftest consumer-can-receive-message-sent-before-subscribing
    (let [producer (mock-producer {})
          consumer (mock-consumer {"auto.offset.reset" "earliest"})
          _ (.send producer (producer-record "topic" "key" "value"))
          _ (.subscribe consumer ["topic"])
          messages (get-messages consumer timeout)]
      (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
             messages)))))

(deftest consumer-can-receive-messages-from-multiple-topics
  (let [[producer consumer] (create-mocks)
        _ (.subscribe consumer ["topic" "topic2"])
        _ (.send producer (producer-record "topic" "key" "value"))
        _ (.send producer (producer-record "topic2" "key2" "value2"))
        messages (get-messages consumer timeout)]
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value2" :key "key2" :partition 0 :topic "topic2" :offset 0}]
           messages))))

(deftest consumer-can-unsubscribe-from-topics
  (let [[producer consumer] (create-mocks)
        _ (.subscribe consumer ["topic" "topic2"])
        _ (.send producer (producer-record "topic" "key" "value"))
        _ (.send producer (producer-record "topic2" "key2" "value2"))
        subscribed-messages (get-messages consumer timeout)

        _ (.unsubscribe consumer)

        _ (.send producer (producer-record "topic" "key" "value"))
        _ (.send producer (producer-record "topic2" "key2" "value2"))
        unsubscribed-messages (get-messages consumer timeout)]
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value2" :key "key2" :partition 0 :topic "topic2" :offset 0}]
           subscribed-messages))
    (is (= [] unsubscribed-messages))))

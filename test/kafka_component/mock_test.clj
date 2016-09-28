(ns kafka-component.mock-test
  (:require [kafka-component.mock :refer :all]
            [clojure.test :refer :all]
            [kafka-component.core-test :refer [with-resource]]
            [com.stuartsierra.component :as component])
  (:import [org.apache.kafka.clients.producer Producer ProducerRecord RecordMetadata Callback]
           [org.apache.kafka.clients.consumer Consumer ConsumerRecord ConsumerRecords]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.errors WakeupException]
           [java.util Collection]
           [java.util.regex Pattern]))

(use-fixtures :each fixture-reset-state!)

(def timeout 500)

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
  (let [[producer consumer] (create-mocks)]
    (.subscribe consumer ["topic"])
    @(.send producer (producer-record "topic" "key" "value"))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (get-messages consumer timeout)))))

(deftest consumer-can-receive-message-from-different-partitions
  (let [[producer consumer] (create-mocks)]
    (.subscribe consumer ["topic"])
    @(.send producer (producer-record "topic" "key" "value" 0))
    @(.send producer (producer-record "topic" "key" "value" 1))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value" :key "key" :partition 1 :topic "topic" :offset 0}]
           (sort-by :partition (get-messages consumer timeout))))))

(deftest consumer-can-limit-number-of-messages-polled
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "1"})]
    (.subscribe consumer ["topic"])
    @(.send producer (producer-record "topic" "key" "value"))
    @(.send producer (producer-record "topic" "key2" "value2"))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (get-messages consumer timeout)))
    (is (= [{:value "value2" :key "key2" :partition 0 :topic "topic" :offset 1}]
           (get-messages consumer timeout)))))

(deftest consumer-can-receive-message-sent-before-subscribing
  (let [producer (mock-producer {})
        consumer (mock-consumer {"auto.offset.reset" "earliest"})]
    @(.send producer (producer-record "topic" "key" "value"))
    (.subscribe consumer ["topic"])
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (get-messages consumer timeout)))))

(deftest consumer-can-use-latest-auto-offset-reset-to-skip-earlier-messages
  (let [producer (mock-producer {})
        consumer (mock-consumer {"auto.offset.reset" "latest"})]
    @(.send producer (producer-record "topic" "key" "value"))
    (.subscribe consumer ["topic"])
    @(.send producer (producer-record "topic" "key2" "value2"))
    (is (= [{:value "value2" :key "key2" :partition 0 :topic "topic" :offset 1}]
           (get-messages consumer timeout)))))

(deftest consumer-can-receive-messages-from-multiple-topics
  (let [[producer consumer] (create-mocks)]
    (.subscribe consumer ["topic" "topic2"])
    @(.send producer (producer-record "topic" "key" "value"))
    @(.send producer (producer-record "topic2" "key2" "value2"))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value2" :key "key2" :partition 0 :topic "topic2" :offset 0}]
           (sort-by :topic (get-messages consumer timeout))))))

(deftest consumer-waits-for-new-messages-to-arrive
  (let [[producer consumer] (create-mocks)
        msg-promise (promise)]
    (.subscribe consumer ["topic"])
    (future (deliver msg-promise (get-messages consumer (* 4 timeout))))
    @(.send producer (producer-record))
    (is (= 1 (count (deref msg-promise (* 8 timeout) []))))))

(deftest consumer-can-unsubscribe-from-topics
  (let [[producer consumer] (create-mocks)
        _ (.subscribe consumer ["topic" "topic2"])
        _ @(.send producer (producer-record "topic" "key" "value"))
        _ @(.send producer (producer-record "topic2" "key2" "value2"))
        subscribed-messages (get-messages consumer timeout)

        _ (.unsubscribe consumer)

        _ @(.send producer (producer-record "topic" "key" "value"))
        _ @(.send producer (producer-record "topic2" "key2" "value2"))
        unsubscribed-messages (get-messages consumer timeout)]
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value2" :key "key2" :partition 0 :topic "topic2" :offset 0}]
           (sort-by :partition subscribed-messages)))
    (is (= [] unsubscribed-messages))))

(deftest consumer-can-be-woken-up
  (let [consumer (mock-consumer {})
        woken (promise)]
    (.subscribe consumer ["topic"])
    (future
      (try
        (let [res (.poll consumer (* 2 timeout))]
          (println res))
        (catch WakeupException e
          (deliver woken "I'm awake!"))))
    (.wakeup consumer)
    (is (= "I'm awake!" (deref woken timeout nil)))))

(deftest consumer-can-be-woken-up-outside-of-poll-and-poll-still-throws-wakeup-exception
  (let [consumer (mock-consumer {})
        woken (promise)]
    (.subscribe consumer ["topic"])
    (.wakeup consumer)
    (future
      (try
        (let [res (.poll consumer timeout)]
          (println res))
        (catch WakeupException e
          (deliver woken "I'm awake!"))))
    (is (= "I'm awake!" (deref woken timeout nil)))))

(defn consume-messages [expected-message-count messages messages-promise msg]
  (locking expected-message-count
    (let [updated-messages (swap! messages conj msg)]
      (when (>= (count updated-messages) expected-message-count)
        (deliver messages-promise @messages)))))

(defn logger [& args]
  (locking println
    (apply println args)))

(defn new-mock-pool [config expected-message-count received-messages]
  (mock-consumer-pool (merge {:topics-or-regex []
                              :pool-size 1
                              :kafka-consumer-config {"auto.offset.reset" "earliest"
                                                      "group.id" "test-group"}} config)
                      {:consumer (partial consume-messages expected-message-count (atom []) received-messages)}
                      logger logger))

(deftest consumer-pool-can-be-started-to-consume-messages
  (let [received-messages (promise)]
    (with-resource [consumer-pool (component/start (new-mock-pool {:topics-or-regex ["topic"]
                                                                   :pool-size 1}
                                                                  2 received-messages))]
      component/stop
      (let [producer (mock-producer {})]
        @(.send producer (producer-record "topic" "key" "value" 0))
        @(.send producer (producer-record "topic" "key2" "value2" 1))
        (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
                {:value "value2" :key "key2" :partition 1 :topic "topic" :offset 0}]
               (sort-by :partition (deref received-messages 5000 []))))))))

(deftest multiple-consumers-in-the-same-group-share-the-messages
  (let [received-messages (promise)]
    (with-resource [consumer-pool (component/start (new-mock-pool {:topics-or-regex ["topic"]
                                                                   :pool-size 2}
                                                                  2 received-messages))]
      component/stop
      (let [producer (mock-producer {})]
        @(.send producer (producer-record "topic" "key" "value" 0))
        @(.send producer (producer-record "topic" "key2" "value2" 1))
        (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
                {:value "value2" :key "key2" :partition 1 :topic "topic" :offset 0}]
               (sort-by :partition (deref received-messages 5000 []))))))))

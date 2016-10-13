(ns kafka-component.mock-test
  (:require [kafka-component.mock :as mock]
            [clojure.test :refer :all]
            [kafka-component.core-test :refer [with-resource] :as core-test]
            [com.stuartsierra.component :as component]
            [kafka-component.core :as core]
            [gregor.core :as gregor])
  (:import [org.apache.kafka.clients.producer Producer ProducerRecord RecordMetadata Callback]
           [org.apache.kafka.clients.consumer Consumer ConsumerRecord ConsumerRecords]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.errors WakeupException InvalidOffsetException]
           [java.util Collection]
           [java.util.regex Pattern]))

(use-fixtures :each mock/fixture-restart-broker!)

(defn deep-merge
  [& maps]
  (if (every? map? maps)
    (apply merge-with deep-merge maps)
    (last maps)))

(def mock-config (deep-merge core-test/test-config
                             {:kafka-writer-config {:native-producer-type :mock}
                              :kafka-reader-config {:native-consumer-type :mock
                                                    :native-consumer-overrides mock/default-mock-consumer-opts}}))

(defmacro with-test-system
  [sys & body]
  `(with-resource [system# (component/start (core-test/test-system mock-config))]
     component/stop
     (let [~sys system#]
       ~@body)))

(deftest sending-and-receiving-messages-using-mock
  (with-test-system {:keys [messages writer]}
    (core/write writer "test_events" "key" "yolo")
    (is (= {:topic "test_events" :partition 0 :key "key" :offset 0 :value "yolo"}
           (deref messages 500 [])))))

(def timeout 500)

(defn mock-consumer [overrides]
  (core/make-consumer :mock [] (merge mock/default-mock-consumer-opts
                                      {"bootstrap.servers" "localhost.fake"}
                                      overrides)))

(defn mock-producer [overrides]
  (core/make-producer :mock overrides))

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

(deftest consumer-can-receive-message-sent-after-subscribing
  (mock/shutdown!)
  (mock/with-test-producer-consumer producer consumer
    (.subscribe consumer ["topic"])
    @(.send producer (producer-record "topic" "key" "value"))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (mock/get-messages consumer timeout))))
  (mock/start!))

(deftest consumer-can-receive-message-from-different-partitions
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "2"})]
    (.subscribe consumer ["topic"])
    @(.send producer (producer-record "topic" "key" "value" 0))
    @(.send producer (producer-record "topic" "key" "value" 1))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value" :key "key" :partition 1 :topic "topic" :offset 0}]
           (sort-by :partition (mock/get-messages consumer timeout))))))

(deftest consumer-can-limit-number-of-messages-polled
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "1"})]
    (.subscribe consumer ["topic"])
    @(.send producer (producer-record "topic" "key" "value"))
    @(.send producer (producer-record "topic" "key2" "value2"))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (mock/get-messages consumer timeout)))
    (is (= [{:value "value2" :key "key2" :partition 0 :topic "topic" :offset 1}]
           (mock/get-messages consumer timeout)))))

(deftest consumer-can-receive-message-sent-before-subscribing
  (let [producer (mock-producer {})
        consumer (mock-consumer {"auto.offset.reset" "earliest"})]
    @(.send producer (producer-record "topic" "key" "value"))
    (.subscribe consumer ["topic"])
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (mock/get-messages consumer timeout)))))

(deftest consumer-can-use-latest-auto-offset-reset-to-skip-earlier-messages
  (let [producer (mock-producer {})
        consumer (mock-consumer {"auto.offset.reset" "latest"})]
    @(.send producer (producer-record "topic" "key" "value"))
    (.subscribe consumer ["topic"])
    (is (= [] (mock/get-messages consumer timeout)))))

(deftest consumer-can-receive-messages-from-multiple-topics
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "2"})]
    (.subscribe consumer ["topic" "topic2"])
    @(.send producer (producer-record "topic" "key" "value"))
    @(.send producer (producer-record "topic2" "key2" "value2"))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value2" :key "key2" :partition 0 :topic "topic2" :offset 0}]
           (sort-by :topic (mock/get-messages consumer timeout))))))

(deftest consumer-waits-for-new-messages-to-arrive
  (mock/shutdown!)
  (mock/with-test-producer-consumer producer consumer
    (let [msg-promise (promise)]
      (.subscribe consumer ["topic"])
      (future (deliver msg-promise (mock/get-messages consumer (* 4 timeout))))
      @(.send producer (producer-record))
      (is (= 1 (count (deref msg-promise (* 8 timeout) []))))))
  (mock/start!))

(deftest consumer-can-unsubscribe-from-topics
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "2"})]
    (.subscribe consumer ["topic" "topic2"])
    @(.send producer (producer-record "topic" "key" "value"))
    @(.send producer (producer-record "topic2" "key2" "value2"))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value2" :key "key2" :partition 0 :topic "topic2" :offset 0}]
           (sort-by :partition (mock/get-messages consumer timeout))))

    (.unsubscribe consumer)

    @(.send producer (producer-record "topic" "key" "value"))
    @(.send producer (producer-record "topic2" "key2" "value2"))
    (is (= [] (mock/get-messages consumer timeout)))))

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

(deftest mock-system-can-be-started-to-consume-messages
  (let [received-messages (promise)]
    (core-test/with-transformed-test-system
      (deep-merge mock-config
                  {:kafka-reader-config {:topics                    ["topic"]
                                         :concurrency-level         1
                                         :native-consumer-overrides {"auto.offset.reset" "earliest"
                                                                     "group.id"          "test"}}})
      (fn [system-map]
        (assoc-in system-map [:test-event-record-processor :process]
                  (partial consume-messages 2 (atom []) received-messages)))
      sys
      (let [producer (mock-producer {})]
        @(.send producer (producer-record "topic" "key" "value" 0))
        @(.send producer (producer-record "topic" "key2" "value2" 1))
        (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
                {:value "value2" :key "key2" :partition 1 :topic "topic" :offset 0}]
               (sort-by :partition (deref received-messages 5000 []))))))))

(deftest multiple-consumers-in-the-same-group-share-the-messages
  (let [received-messages (promise)]
    (core-test/with-transformed-test-system
      (deep-merge mock-config
                  {:kafka-reader-config {:topics                    ["topic"]
                                         :concurrency-level         2
                                         :native-consumer-overrides {"auto.offset.reset" "earliest"
                                                                     "group.id"          "test"}}})
      (fn [system-map]
        (assoc-in system-map [:test-event-record-processor :process]
                  (partial consume-messages 2 (atom []) received-messages)))
      sys
      (let [producer (mock-producer {})]
        @(.send producer (producer-record "topic" "key" "value" 0))
        @(.send producer (producer-record "topic" "key2" "value2" 1))
        (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
                {:value "value2" :key "key2" :partition 1 :topic "topic" :offset 0}]
               (sort-by :partition (deref received-messages 5000 []))))))))

(deftest multiple-consumers-in-the-same-group-process-each-message-only-once
  (let [message-count (atom 0)]
    (core-test/with-transformed-test-system
      (deep-merge mock-config
                  {:kafka-reader-config {:topics                    ["topic"]
                                         :concurrency-level         2
                                         :native-consumer-overrides {"auto.offset.reset" "earliest"
                                                                     "group.id"          "test-group"}}})
      (fn [system-map]
        (assoc-in system-map [:test-event-record-processor :process]
                  (fn [msg] (swap! message-count inc))))
      sys
      (let [producer (mock-producer {})]
        @(.send producer (producer-record "topic" "key" "value" 0))
        @(.send producer (producer-record "topic" "key2" "value2" 1))
        (Thread/sleep (* 4 timeout))
        (is (= 2 @message-count))))))

(deftest multiple-consumers-in-multiple-groups-share-the-messages-appropriately
  (let [group-1-received-messages (promise)
        group-2-received-messages (promise)]
    (core-test/with-transformed-test-system
      (deep-merge mock-config
                  {:kafka-reader-config {:topics                    ["topic"]
                                         :concurrency-level         2
                                         :native-consumer-overrides {"auto.offset.reset" "earliest"
                                                                     "group.id"          "group1"}}})
      (fn [system-map]
        (assoc-in system-map [:test-event-record-processor :process]
                  (partial consume-messages 2 (atom []) group-1-received-messages)))
      sys1
      (core-test/with-transformed-test-system
        (deep-merge mock-config
                    {:kafka-reader-config {:topics                    ["topic"]
                                           :concurrency-level         2
                                           :native-consumer-overrides {"auto.offset.reset" "earliest"
                                                                       "group.id"          "group2"}}})
        (fn [system-map]
          (assoc-in system-map [:test-event-record-processor :process]
                    (partial consume-messages 2 (atom []) group-2-received-messages)))
        sys2
        (let [producer (mock-producer {})]
          @(.send producer (producer-record "topic" "key" "value" 0))
          @(.send producer (producer-record "topic" "key2" "value2" 1))
          (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
                  {:value "value2" :key "key2" :partition 1 :topic "topic" :offset 0}]
                 (sort-by :partition (deref group-1-received-messages 5000 []))))
          (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
                  {:value "value2" :key "key2" :partition 1 :topic "topic" :offset 0}]
                 (sort-by :partition (deref group-2-received-messages 5000 [])))))))))

(deftest producers-can-be-closed-by-gregor
  (let [writer (mock-producer {})]
    (component/start writer)
    (component/stop writer)
    (is true "true to avoid cider's no assertion error")))

(deftest producers-fail-when-broker-is-not-started
  (mock/shutdown!)
  (try
    (mock-producer {})
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "Broker is not running! Did you mean to call 'start!' first?")
          (str "Got: " (.getMessage e)))))
  (mock/start!))

(deftest consumers-fail-when-broker-is-not-started
  (mock/shutdown!)
  (try
    (mock-consumer {})
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "Broker is not running! Did you mean to call 'start!' first?")
          (str "Got: " (.getMessage e)))))
  (mock/start!))

(deftest consumers-fail-when-auto-offset-reset-is-invalid
  (try
    (mock-consumer {"auto.offset.reset" "a-cat"})
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "\"auto.offset.reset\" should be set to one of #{\"latest\" \"earliest\" \"none\"}")
          (str "Got: " (.getMessage e))))))

(deftest consumers-fail-when-bootstrap-servers-is-missing
  (try
    (core/make-consumer :mock [] mock/default-mock-consumer-opts)
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "\"bootstrap.servers\" must be provided in the config")
          (str "Got: " (.getMessage e))))))

(deftest consumers-fail-when-group-id-is-missing
  (try
    (core/make-consumer :mock [] (-> mock/default-mock-consumer-opts
                                     (merge {"bootstrap.servers" "localhost:fake"})
                                     (dissoc "group.id")))

    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "\"group.id\" must be provided in the config")
          (str "Got: " (.getMessage e))))))

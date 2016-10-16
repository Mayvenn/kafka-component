(ns kafka-component.mock-test
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [kafka-component
             [core :as core]
             [core-test :as core-test :refer [with-resource]]
             [mock :as mock]])
  (:import [org.apache.kafka.clients.producer Callback ProducerRecord RecordMetadata]
           org.apache.kafka.common.errors.WakeupException))

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
  [config sys & body]
  `(with-resource [system# (component/start (core-test/test-system (deep-merge mock-config ~config)))]
     component/stop
     (let [~sys system#]
       ~@body)))

(deftest sending-and-receiving-messages-using-mock
  (with-test-system {} {:keys [messages writer]}
    (core/write writer "test_events" "key" "yolo")
    (is (= {:topic "test_events" :partition 0 :key "key" :offset 0 :value "yolo"}
           (deref messages 500 [])))))

(def timeout 500)

(defn mock-consumer [overrides]
  (core/make-consumer :mock [] (merge mock/standalone-mock-consumer-opts overrides)))

(defn mock-producer [overrides]
  (core/make-producer :mock overrides))

(defn producer-record
  ([topic k v] (ProducerRecord. topic k v))
  ([topic k v partition] (ProducerRecord. topic (int partition) k v)))

(defn reify-send-callback [cb]
  (reify Callback
    (onCompletion [this metadata ex]
      (cb metadata ex))))

(deftest send-on-producer-returns-a-future-of-RecordMetadata
  (let [producer (mock-producer {})
        res (mock/send producer "topic" "key" "value")]
    (is (= RecordMetadata (type res)))
    (is (= "topic" (.topic res)))
    (is (= 0 (.partition res)))
    (is (= 0 (.offset res)))))

(deftest send-on-producer-increments-offset
  (let [producer (mock-producer {})
        res (repeatedly 2 #(mock/send-async producer "topic" "key" "value"))]
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
    (mock/send producer "topic" "key" "value")
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (mock/get-messages consumer timeout))))
  (mock/start!))

(deftest consumer-can-receive-message-from-different-partitions
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "2"})]
    @(.send producer (producer-record "topic" "key" "value" 0))
    @(.send producer (producer-record "topic" "key" "value" 1))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value" :key "key" :partition 1 :topic "topic" :offset 0}]
           (sort-by :partition (mock/get-messages consumer "topic" timeout))))))

(deftest consumer-can-limit-number-of-messages-polled
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "1"})]
    (.subscribe consumer ["topic"])
    (mock/send producer "topic" "key" "value")
    (mock/send producer "topic" "key2" "value2")
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (mock/get-messages consumer timeout)))
    (is (= [{:value "value2" :key "key2" :partition 0 :topic "topic" :offset 1}]
           (mock/get-messages consumer timeout)))))

(deftest consumer-can-receive-message-sent-before-subscribing
  (let [producer (mock-producer {})
        consumer (mock-consumer {"auto.offset.reset" "earliest"})]
    (mock/send producer "topic" "key" "value")
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (mock/get-messages consumer "topic" timeout)))))

(deftest consumer-can-use-latest-auto-offset-reset-to-skip-earlier-messages
  (let [producer (mock-producer {})
        consumer (mock-consumer {"auto.offset.reset" "latest"})]
    (mock/send producer "topic" "key" "value")
    (is (= [] (mock/get-messages consumer "topic" timeout)))))

(deftest consumer-can-receive-messages-from-multiple-topics
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "2"})]
    (.subscribe consumer ["topic" "topic2"])
    (mock/send producer "topic" "key" "value")
    (mock/send producer "topic2" "key2" "value2")
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value2" :key "key2" :partition 0 :topic "topic2" :offset 0}]
           (sort-by :topic (mock/get-messages consumer timeout))))))

(deftest consumer-waits-for-new-messages-to-arrive
  (mock/shutdown!)
  (mock/with-test-producer-consumer producer consumer
    (let [msg-promise (promise)]
      (future (deliver msg-promise (mock/get-messages consumer "topic" (* 4 timeout))))
      (mock/send producer "topic" "key" "value")
      (is (= 1 (count (deref msg-promise (* 8 timeout) []))))))
  (mock/start!))

(deftest consumer-can-unsubscribe-from-topics
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "2"})]
    (.subscribe consumer ["topic" "topic2"])
    (mock/send producer "topic" "key" "value")
    (mock/send producer "topic2" "key2" "value2")
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value2" :key "key2" :partition 0 :topic "topic2" :offset 0}]
           (sort-by :partition (mock/get-messages consumer timeout))))

    (.unsubscribe consumer)

    (mock/send producer "topic" "key" "value")
    (mock/send producer "topic2" "key2" "value2")
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

(deftest producers-can-be-closed
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
    (core/make-consumer :mock [] (dissoc mock/standalone-mock-consumer-opts "bootstrap.servers"))
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "\"bootstrap.servers\" must be provided in the config")
          (str "Got: " (.getMessage e))))))

(deftest consumers-fail-when-group-id-is-missing
  (try
    (core/make-consumer :mock [] (dissoc mock/standalone-mock-consumer-opts "group.id"))

    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "\"group.id\" must be provided in the config")
          (str "Got: " (.getMessage e))))))

(deftest reader-fail-when-request-timeout-invalid
  (let [mock-config (assoc-in mock-config [:kafka-reader-config :native-consumer-overrides "request.timeout.ms"] "12")]
    (is (thrown? Exception
                 (with-test-system mock-config sys)))))

(deftest reader-fail-when-given-non-string-values
  (let [mock-config (assoc-in mock-config [:kafka-reader-config :native-consumer-overrides "request.timeout.ms"] 30000)]
    (is (thrown? Exception
                 (with-test-system mock-config sys)))))


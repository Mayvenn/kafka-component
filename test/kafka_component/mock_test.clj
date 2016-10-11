(ns kafka-component.mock-test
  (:require [kafka-component.mock :refer :all]
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

(use-fixtures :each fixture-restart-broker!)

(def mock-config {:kafka-producer-opts {}
                  :kafka-consumer-opts default-mock-consumer-opts})

(defn inject-mock-producer-consumer [system]
  (-> system
      (assoc :test-event-consumer-task-factory (map->MockConsumerTaskFactory {}))
      (assoc :producer-component (map->MockProducerComponent {}))))

(defmacro with-test-system
  [sys & body]
  `(with-resource [system# (component/start (core-test/test-system (merge core-test/test-config
                                                                          mock-config)
                                                                   inject-mock-producer-consumer))]
     component/stop
     (let [~sys system#]
       ~@body)))

(deftest sending-and-receiving-messages-using-mock
  (with-test-system {:keys [messages producer-component]}
    @(gregor/send (:producer producer-component) "test_events" "key" "yolo")
    (is (= {:topic "test_events" :partition 0 :key "key" :offset 0 :value "yolo"}
           (deref messages 500 [])))))

(def timeout 500)

(defn mock-consumer-task-factory
  ([kafka-consumer-opts consume-fn]
   (map->MockConsumerTaskFactory {:logger println
                                  :exception-handler println
                                  :kafka-consumer-opts (merge default-mock-consumer-opts kafka-consumer-opts)
                                  :consumer-component {:consumer consume-fn}}))
  ([logger exception-handler kafka-consumer-opts consume-fn]
   (map->MockConsumerTaskFactory {:logger logger
                                  :exception-handler exception-handler
                                  :kafka-consumer-opts (merge default-mock-consumer-opts kafka-consumer-opts)
                                  :consumer-component {:consumer consume-fn}})))

(defn mock-consumer-pool
  ([pool-config consumer-task-factory]
   (core/map->ConsumerPoolComponent {:logger println
                                     :exception-handler println
                                     :pool-config pool-config
                                     :consumer-task-factory consumer-task-factory}))
  ([logger exception-handler pool-config consumer-task-factory]
   (core/map->ConsumerPoolComponent {:logger logger
                                     :exception-handler exception-handler
                                     :pool-config pool-config
                                     :consumer-task-factory consumer-task-factory})))

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

(defn create-mocks []
  [(mock-producer {}) (mock-consumer {"auto.offset.reset" "earliest"
                                      "group.id" "test"
                                      "bootstrap.servers" "localhost:fake"})])

(deftest consumer-can-receive-message-sent-after-subscribing
  (let [[producer consumer] (create-mocks)]
    (.subscribe consumer ["topic"])
    @(.send producer (producer-record "topic" "key" "value"))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (get-messages consumer timeout)))))

(deftest consumer-can-receive-message-from-different-partitions
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "2"
                                 "bootstrap.servers" "localhost:fake"
                                 "group.id" "test"
                                 "auto.offset.reset" "earliest"})]
    (.subscribe consumer ["topic"])
    @(.send producer (producer-record "topic" "key" "value" 0))
    @(.send producer (producer-record "topic" "key" "value" 1))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value" :key "key" :partition 1 :topic "topic" :offset 0}]
           (sort-by :partition (get-messages consumer timeout))))))

(deftest consumer-can-limit-number-of-messages-polled
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "1"
                                 "bootstrap.servers" "localhost:fake"
                                 "group.id" "test"
                                 "auto.offset.reset" "earliest"})]
    (.subscribe consumer ["topic"])
    @(.send producer (producer-record "topic" "key" "value"))
    @(.send producer (producer-record "topic" "key2" "value2"))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (get-messages consumer timeout)))
    (is (= [{:value "value2" :key "key2" :partition 0 :topic "topic" :offset 1}]
           (get-messages consumer timeout)))))

(deftest consumer-can-receive-message-sent-before-subscribing
  (let [producer (mock-producer {})
        consumer (mock-consumer {"auto.offset.reset" "earliest"
                                 "group.id" "test"
                                 "bootstrap.servers" "localhost:fake"})]
    @(.send producer (producer-record "topic" "key" "value"))
    (.subscribe consumer ["topic"])
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}]
           (get-messages consumer timeout)))))

(deftest consumer-can-use-latest-auto-offset-reset-to-skip-earlier-messages
  (let [producer (mock-producer {})
        consumer (mock-consumer {"auto.offset.reset" "latest"
                                 "group.id" "test"
                                 "bootstrap.servers" "localhost:fake"})]
    @(.send producer (producer-record "topic" "key" "value"))
    (.subscribe consumer ["topic"])
    (is (= [] (get-messages consumer timeout)))))

(deftest consumer-can-receive-messages-from-multiple-topics
  (let [producer (mock-producer {})
        consumer (mock-consumer {"max.poll.records" "2"
                                 "group.id" "test"
                                 "bootstrap.servers" "localhost:fake"
                                 "auto.offset.reset" "earliest"})]
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
  (let [producer (mock-producer {})
        consumer (mock-consumer {"auto.offset.reset" "earliest"
                                 "group.id" "test"
                                 "bootstrap.servers" "localhost:fake"
                                 "max.poll.records" "2"})]
    (.subscribe consumer ["topic" "topic2"])
    @(.send producer (producer-record "topic" "key" "value"))
    @(.send producer (producer-record "topic2" "key2" "value2"))
    (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
            {:value "value2" :key "key2" :partition 0 :topic "topic2" :offset 0}]
           (sort-by :partition (get-messages consumer timeout))))

    (.unsubscribe consumer)

    @(.send producer (producer-record "topic" "key" "value"))
    @(.send producer (producer-record "topic2" "key2" "value2"))
    (is (= [] (get-messages consumer timeout)))))

(deftest consumer-can-be-woken-up
  (let [consumer (mock-consumer {"auto.offset.reset" "earliest"
                                 "bootstrap.servers" "localhost:fake"
                                 "group.id" "test"})
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
  (let [consumer (mock-consumer {"auto.offset.reset" "earliest"
                                 "bootstrap.servers" "localhost:fake"
                                 "group.id" "test"})
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

(defn new-mock-pool [pool-config kafka-consumer-opts expected-message-count received-messages]
  (mock-consumer-pool pool-config
                      (mock-consumer-task-factory kafka-consumer-opts
                                                  (partial consume-messages expected-message-count (atom []) received-messages))))

(deftest consumer-pool-can-be-started-to-consume-messages
  (let [received-messages (promise)]
    (with-resource [consumer-pool (component/start (new-mock-pool {:topics-or-regex ["topic"]
                                                                   :pool-size 1}
                                                                  {"auto.offset.reset" "earliest"
                                                                   "group.id" "test"
                                                                   "bootstrap.servers" "localhost:fake"}
                                                                  2
                                                                  received-messages))]
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
                                                                  {"auto.offset.reset" "earliest"
                                                                   "group.id" "test"
                                                                   "bootstrap.servers" "localhost:fake"}
                                                                  2 received-messages))]
      component/stop
      (let [producer (mock-producer {})]
        @(.send producer (producer-record "topic" "key" "value" 0))
        @(.send producer (producer-record "topic" "key2" "value2" 1))
        (is (= [{:value "value" :key "key" :partition 0 :topic "topic" :offset 0}
                {:value "value2" :key "key2" :partition 1 :topic "topic" :offset 0}]
               (sort-by :partition (deref received-messages 5000 []))))))))

(deftest multiple-consumers-in-the-same-group-process-each-message-only-once
  (let [message-count (atom 0)]
    (with-resource [consumer-pool (component/start (mock-consumer-pool {:topics-or-regex ["topic"]
                                                                        :pool-size 2}
                                                                       (mock-consumer-task-factory {"auto.offset.reset" "earliest"
                                                                                                    "bootstrap.servers" "localhost:fake"
                                                                                                    "group.id" "test-group"}
                                                                                                   (fn [msg] (swap! message-count inc)))))]
      component/stop
      (let [producer (mock-producer {})]
        @(.send producer (producer-record "topic" "key" "value" 0))
        @(.send producer (producer-record "topic" "key2" "value2" 1))
        (Thread/sleep (* 4 timeout))
        (is (= 2 @message-count))))))

(deftest multiple-consumers-in-multiple-groups-share-the-messages-appropriately
  (let [group-1-received-messages (promise)
        group-2-received-messages (promise)]
    (with-resource [consumer-pool (component/start (new-mock-pool {:topics-or-regex ["topic"]
                                                                   :pool-size 2}
                                                                  {"auto.offset.reset" "earliest"
                                                                   "bootstrap.servers" "localhost:fake"
                                                                   "group.id" "group1"}
                                                                  2 group-1-received-messages))]
      component/stop
      (with-resource [consumer-pool2 (component/start (new-mock-pool {:topics-or-regex ["topic"]
                                                                      :pool-size 2}
                                                                     {"auto.offset.reset" "earliest"
                                                                      "bootstrap.servers" "localhost:fake"
                                                                      "group.id" "group2"}
                                                                     2 group-2-received-messages))]
        component/stop
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
  (with-resource [producer-component (component/start
                                      (->MockProducerComponent {}))]
    component/stop
    (is true "true to avoid cider's no assertion error")))

(deftest producers-fail-when-broker-is-not-started
  (shutdown!)
  (try
    (mock-producer {})
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "Broker is not running! Did you mean to call 'start!' first?")
          (str "Got: " (.getMessage e)))))
  (start!))

(deftest consumers-fail-when-broker-is-not-started
  (shutdown!)
  (try
    (mock-consumer {"auto.offset.reset" "none"})
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "Broker is not running! Did you mean to call 'start!' first?")
          (str "Got: " (.getMessage e)))))
  (start!))

(deftest consumers-fail-when-auto-offset-reset-is-invalid
  (try
    (mock-consumer {"auto.offset.reset" "a-cat"})
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "\"auto.offset.reset\" should be one of #{\"latest\" \"earliest\" \"none\"}")
          (str "Got: " (.getMessage e))))))

(deftest consumers-fail-when-bootstrap-servers-is-missing
  (try
    (mock-consumer {"auto.offset.reset" "none"})
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "\"bootstrap.servers\" must be provided in config")
          (str "Got: " (.getMessage e))))))

(deftest consumers-fail-when-group-id-is-missing
  (try
    (mock-consumer {"auto.offset.reset" "none"
                    "bootstrap.servers" "localhost:fake"})
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "\"group.id\" must be provided in config")
          (str "Got: " (.getMessage e))))))

(deftest consumers-fail-when-shutdown-grace-period-is-zero
  (try
    (component/start (new-mock-pool {:shutdown-grace-period 0}
                                    {}
                                    0
                                    (promise)))
    (is false "expected exception to be raised")
    (catch Throwable e
      (is (.contains (.getMessage e) "\"shutdown-grace-period\" must not be zero")
          (str "Got: " (.getMessage e))))))

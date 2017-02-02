(ns kafka-component.core-test
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [embedded-kafka.core :as ek]
            [kafka-component.core :refer :all]
            [gregor.core :as gregor]))

(def test-config {:kafka-reader-config {:concurrency-level         1
                                        :topics                    ["test_events"]
                                        :native-consumer-overrides ek/kafka-config}
                  :kafka-writer-config {:native-producer-overrides ek/kafka-config}})

(defn test-system
  ([config]
   (test-system config identity))
  ([config transform]
   (let [messages (promise)]
     (component/system-using
      (transform (component/system-map
                  :logger println
                  :exception-handler println
                  :messages messages
                  :test-event-record-processor {:process (juxt (partial deliver messages)
                                                               (partial prn "Message consumed: "))}
                  :test-event-reader (map->KafkaReader (:kafka-reader-config config))
                  :writer (map->KafkaWriter (:kafka-writer-config config))))
      {:test-event-reader {:logger            :logger
                           :exception-handler :exception-handler
                           :record-processor  :test-event-record-processor}}))))

(defmacro with-resource
  [bindings close-fn & body]
  `(let ~bindings
     (try
       ~@body
       (finally
         (~close-fn ~(bindings 0))))))

(defmacro with-test-system
  [config sys & body]
  `(with-resource [system# (component/start (test-system ~config))]
     component/stop
     (let [~sys system#]
       ~@body)))

(defmacro with-transformed-test-system
  [config transform sys & body]
  `(with-resource [system# (component/start (test-system ~config ~transform))]
     component/stop
     (let [~sys system#]
       ~@body)))

(deftest sending-and-receiving-messages-using-kafka-with-message-commits
  (ek/with-test-broker producer consumer
    (with-test-system test-config {:keys [messages writer]}
      (write writer "test_events" "key" "yolo")
      (is (= {:topic "test_events" :partition 0 :key "key" :offset 0 :value "yolo"}
             (deref messages 10000 {})))
      (testing "it should commit offsets to message offset + 1"
        (is (= 1 (:offset (gregor/committed consumer "test_events" 0))))))))

(deftest reader-fail-when-auto-offset-reset-is-invalid
  (let [test-config (assoc-in test-config [:kafka-reader-config :native-consumer-overrides "auto.offset.reset"] "smallest")]
    (is (thrown? Exception
                 (with-test-system test-config sys)))))

(deftest reader-fail-when-bootstrap-servers-is-missing
  (let [test-config (update-in test-config [:kafka-reader-config :native-consumer-overrides] dissoc "bootstrap.servers")]
    (is (thrown? Exception
                 (with-test-system test-config sys)))))

(deftest reader-fail-when-group-id-is-missing
  (let [test-config (update-in test-config [:kafka-reader-config :native-consumer-overrides] dissoc "group.id")]
    (is (thrown? Exception
                 (with-test-system test-config sys)))))

(deftest reader-fail-when-shutdown-grace-period-is-zero
  (let [test-config (assoc-in test-config [:kafka-reader-config :shutdown-timeout] 0)]
    (is (thrown? Exception
                 (with-test-system test-config sys)))))

(deftest reader-fails-when-given-a-record-processor-without-process
  (let [test-transform (fn [system]
                         (assoc system
                                :test-event-record-processor
                                {:not-the-right-key (fn [record])}))]
    (is (thrown? Exception
                 (with-transformed-test-system test-config test-transform sys)))))

(deftest reader-fails-when-given-a-record-processor-with-bad-process
  (let [test-transform (fn [system]
                         (assoc system
                                :test-event-record-processor
                                {:process "not a function"}))]
    (is (thrown? Exception
                 (with-transformed-test-system test-config test-transform sys)))))


(deftest reader-can-be-stopped-before-being-started
  (let [system (test-system test-config)]
    (component/stop system)
    (is true "The real test is that the above does not throw an exception. This is just to appease clojure.test, which expects at least one 'is'.")))

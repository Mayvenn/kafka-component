(ns kafka-component.core-test
  (:require [kafka-component.core :refer :all]
            [embedded-kafka.core :as ek]
            [com.stuartsierra.component :as component]
            [clojure.test :refer :all]
            [gregor.core :as gregor]))

(def test-config {:kafka-consumer-opts ek/kafka-config
                  :kafka-producer-opts ek/kafka-config
                  :test-event-consumer-pool-config {:pool-size 1
                                                    :topics-or-regex ["test_events"]}})

(defn test-system
  ([config]
   (test-system config identity))
  ([config transform]
   (let [messages (promise)]
     (component/system-using
      (transform (component/system-map
                  :kafka-producer-opts (:kafka-producer-opts config)
                  :kafka-consumer-opts (:kafka-consumer-opts config)
                  :logger println
                  :exception-handler println
                  :messages messages
                  :message-consumer {:consumer (juxt (partial deliver messages) (partial prn "Message consumed: "))}
                  :producer-component (map->ProducerComponent {})
                  :test-event-consumer-task-factory (map->AlwaysCommitTaskFactory {})
                  :test-event-consumer-pool (map->ConsumerPoolComponent {:pool-config (:test-event-consumer-pool-config config)})))
      {:producer-component [:kafka-producer-opts]
       :test-event-consumer-task-factory {:logger :logger
                                          :kafka-consumer-opts :kafka-consumer-opts
                                          :exception-handler :exception-handler
                                          :consumer-component :message-consumer}
       :test-event-consumer-pool {:logger :logger
                                  :exception-handler :exception-handler
                                  :consumer-task-factory :test-event-consumer-task-factory}}))))

(defmacro with-resource
  [bindings close-fn & body]
  `(let ~bindings
     (try
       ~@body
       (finally
         (~close-fn ~(bindings 0))))))

(defmacro with-test-system
  [config sys & body]
  `(ek/with-test-broker producer# consumer#
     (with-resource [system# (component/start (test-system (merge test-config ~config)))]
       component/stop
       (let [~sys system#]
         ~@body))))

(deftest sending-and-receiving-messages-using-kafka
  (with-test-system {} {:keys [messages producer-component]}
    (.get (gregor/send (:producer producer-component) "test_events" "key" "yolo"))
    (is (= {:topic "test_events" :partition 0 :key "key" :offset 0 :value "yolo"}
           (deref messages 2000 [])))))

(deftest consumers-fail-when-auto-offset-reset-is-invalid
  (let [test-config (-> test-config
                        (assoc-in [:kafka-consumer-opts "auto.offset.reset"] "smallest"))]
    (is (thrown? Exception
                 (with-test-system test-config {:keys [messages producer-component]})))))

(deftest consumers-fail-when-bootstrap-servers-is-missing
  (let [test-config (-> test-config
                        (update :kafka-consumer-opts dissoc "bootstrap.servers"))]
    (is (thrown? Exception
                 (with-test-system test-config {:keys [messages producer-component]})))))

(deftest consumers-fail-when-group-id-is-missing
  (let [test-config (-> test-config
                        (update :kafka-consumer-opts dissoc "group.id"))]
    (is (thrown? Exception
                 (with-test-system test-config {:keys [messages producer-component]})))))

(deftest consumers-fail-when-shutdown-grace-period-is-zero
  (let [test-config (assoc-in test-config [:test-event-consumer-pool-config :shutdown-grace-period] 0)]
    (is (thrown? Exception
                 (with-test-system test-config {:keys [messages producer-component]})))))

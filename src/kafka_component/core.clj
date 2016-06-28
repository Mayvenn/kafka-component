(ns kafka-component.core
  (:require [com.stuartsierra.component :as component]
            [clj-kafka.consumer.zk :refer [consumer shutdown messages]]
            [clj-kafka.producer :refer [producer]])
  (:import [java.util.concurrent Executors TimeUnit]))

(defn ^:private consume-messages-task
  [logger exception-handler message-consumer topic thread-id kafka-consumer]
  (fn []
    (logger :info (str "consumer thread " thread-id " starting"))
    (doseq [m (messages kafka-consumer topic)]
      (try
        (do
          (logger :info (str "thread " thread-id " received message with key: " (String. (:key m))))
          (message-consumer m)
          (.commitOffsets kafka-consumer))
        (catch Exception e
          (do (logger :error (str "error in consumer thread " thread-id))
              (logger :error e)
              (exception-handler e)))))
    (logger :info (str "consumer thread " thread-id " exiting"))))

(defrecord KafkaConsumerPool [config pool-size topic consumer-component logger exception-handler]
  component/Lifecycle
  (start [c]
    (logger :info (str "starting " topic " consumption"))
    (let [thread-pool (Executors/newFixedThreadPool pool-size)
          kafka-consumers (repeatedly pool-size #(consumer (config :kafka-consumer-config)))
          tasks (map (partial consume-messages-task logger exception-handler (:consumer consumer-component) topic)
                     (map (partial str topic "-") (range))
                     kafka-consumers)]
      (doseq [t tasks] (.submit thread-pool t))
      (logger :info (str "started " topic " consumption"))
      (merge c {:thread-pool thread-pool :kafka-consumers kafka-consumers})))
  (stop [{:keys [logger thread-pool kafka-consumers] :or {logger :noop} :as c}]
    (logger :info (str "stopping " topic " consumption"))
    ;; shutting down consumers in parallel seems to cause less rebalances and shuts down more quickly
    ;; eventually, when offset committing is more granular, it'll be way nicer to only have one consumer
    ;; for all the threads
    (doall (pmap shutdown kafka-consumers))
    (when thread-pool
      (.shutdown thread-pool)
      (.awaitTermination thread-pool (config :shutdown-grace-period) TimeUnit/SECONDS)
    (logger :info (str "stopped " topic " consumption"))
    c)))

(def kafka-producer-config
  {"serializer.class" "kafka.serializer.StringEncoder"
   "request.required.acks" "-1"
   "partitioner.class" "kafka.producer.DefaultPartitioner"})

(defrecord KafkaProducer [config]
  component/Lifecycle
  (start [c]
    (assoc c :producer (producer (merge kafka-producer-config config))))
  (stop [c]
    (when-let [p (:producer c)]
      (.close p)
      (dissoc c :producer))))

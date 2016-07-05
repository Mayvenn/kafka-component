(ns kafka-component.core
  (:require [com.stuartsierra.component :as component]
            [clj-kafka.consumer.zk :refer [consumer shutdown messages]]
            [clj-kafka.producer :refer [producer]])
  (:import [java.util.concurrent Executors TimeUnit]))

(defn ^:private consume-messages-task
  [logger exception-handler shutting-down message-consumer topic thread-id kafka-consumer]
  (fn []
    (logger :info (str "consumer thread " thread-id " starting"))
    (doseq [m (messages kafka-consumer topic)]
      (logger :info (str "thread " thread-id " received message with key: " (String. (:key m))))
      (try
        (message-consumer m)

        ;; separate try/catch for committing offsets since the kafka consumer could have been shut down
        ;; and when the kafka consumer shuts down, it nils out its internal ZK client which causes null pointers
        ;; and other related problems. Thus, we specially catch this exception and only report it if we're not
        ;; shutting down. This solution is a bit simpler/less complex albeit not necessarily as clean as if we
        ;; ourselves polled for messages and only shut down consumers after the thread has cleanly exited.
        (try
          (.commitOffsets kafka-consumer)
          (catch Exception e
            (when-not @shutting-down
              (logger :error (str "failed to commit offsets in thread " thread-id))
              (logger :error e)
              (exception-handler e))))

        (catch Exception e
          (logger :error (str "error in consumer thread " thread-id))
          (logger :error e)
          (exception-handler e))))
    (logger :info (str "consumer thread " thread-id " exiting"))))

(defrecord KafkaConsumerPool [config pool-size topic consumer-component logger exception-handler]
  component/Lifecycle
  (start [c]
    (logger :info (str "starting " topic " consumption"))
    (let [shutting-down (atom false)
          thread-pool (Executors/newFixedThreadPool pool-size)
          kafka-consumers (repeatedly pool-size #(consumer (config :kafka-consumer-config)))
          tasks (map (partial consume-messages-task logger exception-handler shutting-down (:consumer consumer-component) topic)
                     (map (partial str topic "-") (range))
                     kafka-consumers)]
      (doseq [t tasks] (.submit thread-pool t))
      (logger :info (str "started " topic " consumption"))
      (merge c {:thread-pool thread-pool :kafka-consumers kafka-consumers :shutting-down shutting-down})))
  (stop [{:keys [logger thread-pool kafka-consumers shutting-down] :or {logger :noop} :as c}]
    (reset! shutting-down true)
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

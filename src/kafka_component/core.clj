(ns kafka-component.core
  (:require [com.stuartsierra.component :as component]
            [gregor.core :as gregor]
            [kafka-component.config :as config])
  (:import [java.util.concurrent Executors TimeUnit]
           org.apache.kafka.clients.consumer.CommitFailedException
           org.apache.kafka.common.errors.WakeupException))

(defn- latest-offsets [records]
  (->> records
       (reduce (fn [key->offset {:keys [topic partition offset]}]
                 (update key->offset [topic partition] (fnil max 0) offset))
               {})
       (mapv (fn [[[topic partition] offset]]
               {:topic topic
                :partition partition
                :offset (inc offset)}))))

(defmulti make-consumer
  (fn [type topics overrides]
    (config/assert-consumer-opts overrides)
    type))

(defmulti make-producer
  (fn [type overrides]
    (config/assert-producer-opts overrides)
    type))

(defmethod make-consumer :default [_ topics overrides]
  (gregor/consumer (overrides "bootstrap.servers")
                   (overrides "group.id")
                   topics
                   (merge config/default-consumer-config overrides)))

(defmethod make-producer :default [_ overrides]
  (gregor/producer (overrides "bootstrap.servers")
                   (merge config/default-producer-config overrides)))

(defmacro with-err-str [& body]
  `(let [wr# (java.io.StringWriter.)]
     (binding [*err* wr#]
       ~@body)
     (str wr#)))

(defn panic! []
  (System/exit 63))

(defmacro ^{:style/indent 2} try-or-panic [task-id panic-msg & body]
  `(try
     ~@body
     (catch Throwable t#
       (println (str "source=kafka-consumer action=exception notice=a restart may be required to continue processing kafka partitions msg=" ~panic-msg " task-id=" ~task-id " exception=" (with-err-str (.printStackTrace t#))))
       (panic!))))

(defn make-task [logger exception-handler process-record poll-interval make-kafka-consumer task-id]
  (let [kafka-consumer (make-kafka-consumer)
        log-exception  (fn log-exception [e message]
                         (try-or-panic task-id "failed to use logger to log exception"
                                       (logger :event-error message))
                         (try-or-panic task-id "failed to use exception-handler to log exception"
                           (exception-handler e)))
        log            (fn log [level msg]
                         (try-or-panic task-id (format "failed to log %s %s" level (pr-str msg))
                           (logger level msg)))]
    (reify
      java.lang.Runnable
      (run [_]
        (try
          (log :event {:event {:name :kafka.consumer.task/started}})

          (while true
            (let [records (gregor/poll kafka-consumer (or poll-interval 100))]
              (doseq [{:keys [topic partition key] :as record} records]
                (log :event {:event {:name :kafka.consumer.task/received
                                     :data {:task-id task-id
                                            :topic topic
                                            :partition partition
                                            :key key}}})
                (try
                  (process-record record)
                  (catch WakeupException e (throw e))
                  (catch Throwable e
                    (log-exception e {:event {:name :kafka.consumer.task/erred-while-processing-message
                                              :data {:task-id task-id
                                                     :topic topic
                                                     :partition partition
                                                     :key key
                                                     :error e}}}))))
              (try
                (gregor/commit-offsets! kafka-consumer (latest-offsets records))
                (catch CommitFailedException e
                  (log-exception e {:event {:name :kafka.consumer.task/erred-saving-offsets
                                            :data {:task-id task-id
                                                   :error e}}})))))
          (log :event {:event {:name :kafka.consumer.task/exiting
                               :data {:task-id task-id}}})
          (catch WakeupException _
            (log :event {:event {:name :kafka.consumer.task/woke-up
                                 :data {:task-id task-id}}}))
          (catch Throwable e
            (log-exception e {:event {:name :kafka.consumer.task/erred-in-task-runnable
                                      :data {:error e
                                             :task-id task-id}}}))
          (finally
            (log :event {:event {:name :kafka.consumer.task/closed
                                 :data {:task-id task-id}}})
            (gregor/close kafka-consumer))))
      java.io.Closeable
      (close [_]
        (gregor/wakeup kafka-consumer)))))

(defn init-and-start-task-pool [make-task pool-id concurrency-level]
  (let [native-pool (Executors/newFixedThreadPool concurrency-level)
        task-ids    (map (partial str pool-id "-") (range concurrency-level))
        tasks       (map make-task task-ids)]
    (doseq [t tasks] (.submit native-pool t))
    {:native-pool native-pool
     :tasks       tasks}))

(defn stop-task-pool [{:keys [native-pool tasks]} shutdown-timeout]
  (doseq [t tasks] (.close t))
  (when native-pool
    (.shutdown native-pool)
    (.awaitTermination native-pool shutdown-timeout TimeUnit/SECONDS)))

(defrecord KafkaReader [logger exception-handler record-processor concurrency-level poll-interval shutdown-timeout topics native-consumer-type native-consumer-overrides]
  component/Lifecycle
  (start [this]
    (assert (not= shutdown-timeout 0) "\"shutdown-timeout\" must not be zero")
    (assert (ifn? (:process record-processor)) "record-processor does not have a function :process")
    (let [make-native-consumer (partial make-consumer native-consumer-type topics native-consumer-overrides) ; a thunk, does not need more args
          make-task            (partial make-task logger exception-handler (:process record-processor) poll-interval make-native-consumer)
                                        ;will get task-id when pool is started
          pool-id              (pr-str topics)
          log-data             {:concurrency-level         concurrency-level
                                :shutdown-timeout          shutdown-timeout
                                :topics                    topics
                                :poll-interval             poll-interval
                                :native-consumer-overrides native-consumer-overrides
                                :native-consumer-type      native-consumer-type
                                :pool-id                   pool-id}]

      (logger :event {:event {:name :kafka.consumer/initialized
                              :data log-data}})
      (let [running-pool (init-and-start-task-pool make-task pool-id concurrency-level)]
        (logger :event {:event {:name :kafka.consumer/started
                                :data log-data}})
        (merge this
               {:pool     running-pool
                :log-data log-data}))))
  (stop [{:keys [pool log-data] :as this}]
    (when pool
      (logger :event {:event {:name :kafka.consumer/started-to-stop
                              :data log-data}})
      (stop-task-pool pool (or shutdown-timeout 4))
      (logger :event {:event {:name :kafka.consumer/stopped
                              :data log-data}}))
    (dissoc this :pool)))

(defrecord KafkaWriter [logger native-producer-type native-producer-overrides]
  component/Lifecycle
  (start [this]
    (assoc this :native-producer (make-producer native-producer-type native-producer-overrides)))
  (stop [this]
    (when-let [p (:native-producer this)]
      (gregor/close p 2)) ;; 2 seconds to wait to send remaining messages, should this be configurable?
    (dissoc this :native-producer)))

(defn write-async [writer topic key val]
  (gregor/send (:native-producer writer) topic key val))

(defn write [writer topic key val]
  (try
    @(write-async writer topic key val)
    (catch Throwable t
      (let [outer-exception (ex-info "Unable to write to kafka"
                                     {:cause t
                                      :topic topic
                                      :key   key}
                                     t)]
        (when (:logger writer)
          ((:logger writer) :event-error {:event {:name :kafka.producer/erred
                                                  :data {:error outer-exception
                                                         :topic topic
                                                         :key   key}}}))
        (throw outer-exception)))))

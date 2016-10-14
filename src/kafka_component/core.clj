(ns kafka-component.core
  (:require [com.stuartsierra.component :as component]
            [gregor.core :as gregor]
            [kafka-component.config :as config])
  (:import [java.util.concurrent Executors TimeUnit]
           org.apache.kafka.common.errors.WakeupException))

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

(defn make-task [logger exception-handler process-record make-kafka-consumer task-id]
  (let [kafka-consumer (make-kafka-consumer)
        log-exception  (fn log-exception [e & msg]
                         (logger :error (apply str "task-id=" task-id " " msg))
                         (logger :error e)
                         (exception-handler e))
        log            (fn log [& msg]
                         (logger :info (apply str "task-id=" task-id " " msg)))]
    (reify
      java.lang.Runnable
      (run [_]
        (try
          (log "action=starting")

          (while true
            (doseq [{:keys [topic partition offset key] :as record} (gregor/poll kafka-consumer)]
              (log "action=receiving topic=" topic " partition=" partition " key=" key)
              (try
                (process-record record)
                (gregor/commit-offsets! kafka-consumer [{:topic topic :partition partition :offset (inc offset)}])
                (catch WakeupException e (throw e))
                (catch Exception e
                  (log-exception e "msg=error in message consumer")))))

          (log "action=exiting")
          (catch WakeupException e (log "action=woken-up"))
          (catch Exception e (log-exception e "msg=error in kafka consumer task runnable"))
          (finally
            (log "action=closing-kafka-consumer")
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

(defrecord KafkaReader [logger exception-handler record-processor concurrency-level shutdown-timeout topics native-consumer-type native-consumer-overrides]
  component/Lifecycle
  (start [this]
    (assert (not= shutdown-timeout 0) "\"shutdown-timeout\" must not be zero")
    (assert (ifn? (:process record-processor)) "record-processor does not have a function :process")
    (let [make-native-consumer (partial make-consumer native-consumer-type topics native-consumer-overrides) ; a thunk, does not need more args
          make-task            (partial make-task logger exception-handler (:process record-processor) make-native-consumer)
                                        ;will get task-id when pool is started
          pool-id              (pr-str topics)
          log-action           (fn [& msg] (logger :info (apply str "pool-id=" pool-id " action=" msg)))]
      (log-action "starting-consumption concurrency-level=" concurrency-level " shutdown-timeout=" shutdown-timeout " topics=" topics)
      (let [running-pool (init-and-start-task-pool make-task pool-id concurrency-level)]
        (log-action "started-consumption")
        (merge this
               {:pool       running-pool
                :log-action log-action}))))
  (stop [{:keys [pool log-action] :as this}]
    (log-action "stopping-consumption")
    (stop-task-pool pool (or shutdown-timeout 4))
    (log-action "stopped-consumption")
    (dissoc this :pool)))

(defrecord KafkaWriter [native-producer-type native-producer-overrides]
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
  @(write-async writer topic key val))

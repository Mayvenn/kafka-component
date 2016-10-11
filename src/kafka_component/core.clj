(ns kafka-component.core
  (:require [com.stuartsierra.component :as component]
            [gregor.core :as gregor]
            [kafka-component.config :as config])
  (:import [java.util.concurrent Executors TimeUnit]
           org.apache.kafka.common.errors.WakeupException))

(defprotocol ConsumerTaskFactory
  (build-task [this topics-or-regex task-id]))

(def default-consumer-config
  {"enable.auto.commit"	"false"
   "max.poll.records" "1"})

(defn make-default-consumer [topics-or-regex kafka-config]
  (gregor/consumer (kafka-config "bootstrap.servers") (kafka-config "group.id")
                   topics-or-regex (merge default-consumer-config kafka-config)))

(def default-producer-config
  {"acks" "all"
   "retries" "3"
   "max.in.flight.requests.per.connection" "1"})

(defn make-default-producer [config]
  (gregor/producer (config "bootstrap.servers")
                   (merge default-producer-config config)))

(defrecord ConsumerAlwaysCommitTask [logger exception-handler message-consumer kafka-config make-kafka-consumer consumer-atom task-id]
  java.lang.Runnable
  (run [_]
    (try
      (let [kafka-consumer (make-kafka-consumer kafka-config)
            group-id       (kafka-config "group.id")
            log-exception  (fn log-exception [e & msg]
                             (logger :error (apply str "group=" group-id " task-id=" task-id " " msg))
                             (logger :error e)
                             (exception-handler e))
            log            (fn log [& msg]
                             (logger :info (apply str "group=" group-id " task-id=" task-id " " msg)))]

        (reset! consumer-atom kafka-consumer)
        (try
          (log "action=starting")

          (while true
            (doseq [{:keys [topic partition offset key] :as record} (gregor/poll kafka-consumer)]
              (log "action=receiving topic=" topic " partition=" partition " key=" key)
              (try
                (message-consumer record)
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
      (catch Exception e
        (logger :error e)
        (exception-handler e)
        (throw e))))
  java.io.Closeable
  (close [_]
    (when-let [consumer @consumer-atom]
      (gregor/wakeup consumer))))

(defn init-and-start-task-pool [{:keys [pool-config pool-id consumer-task-factory] :as task-pool}]
  (let [pool-size   (:pool-size pool-config)
        thread-pool (Executors/newFixedThreadPool pool-size)
        task-ids    (map (partial str pool-id "-") (range pool-size))
        tasks       (map (partial build-task consumer-task-factory (:topics-or-regex pool-config)) task-ids)]
    (doseq [t tasks] (.submit thread-pool t))
    (merge task-pool {:thread-pool thread-pool
                      :tasks tasks})))

(defn stop-task-pool [{:keys [pool-config thread-pool tasks] :as task-pool}]
  (doseq [t tasks] (.close t))
  (when thread-pool
    (.shutdown thread-pool)
    (.awaitTermination thread-pool (pool-config :shutdown-grace-period 4) TimeUnit/SECONDS))
  (dissoc task-pool :thread-pool :tasks))

(defn log [logger pool-id pool-config & msg]
  (logger :info (apply str "pool-id=" pool-id " pool-config=" pool-config " " msg)))

(defrecord ConsumerPoolComponent [logger exception-handler pool-config consumer-task-factory]
  component/Lifecycle
  (start [this]
    (let [pool-id (pr-str (:topics-or-regex pool-config))
          log-action (partial log logger pool-id pool-config " action=")
          this (assoc this :log-action log-action :pool-id pool-id)]
      (assert (not= (:shutdown-grace-period pool-config) 0) "\"shutdown-grace-period\" must not be zero")
      (log-action "starting-consumption")
      (let [initialized-component (init-and-start-task-pool this)]
        (log-action "started-consumption")
        initialized-component)))

  (stop [{:keys [logger log-action pool-id] :as this}]
    (log-action "stopping-consumption")
    (let [deinitialized-component (stop-task-pool this)]
      (log-action "stopped-consumption")
      deinitialized-component)))

(defrecord AlwaysCommitTaskFactory [logger exception-handler consumer-component kafka-consumer-opts]
  ConsumerTaskFactory
  (build-task [_ topics-or-regex task-id]
    (assert kafka-consumer-opts "Kafka-consumer-opts cannot be nil")
    (config/assert-required-consumer-keys kafka-consumer-opts)
    (config/assert-non-nil-values kafka-consumer-opts)
    (->ConsumerAlwaysCommitTask logger
                                exception-handler
                                (:consumer consumer-component)
                                kafka-consumer-opts
                                (partial make-default-consumer topics-or-regex)
                                (atom nil)
                                task-id)))

(defrecord ProducerComponent [kafka-producer-opts]
  component/Lifecycle
  (start [c]
    (assoc c :producer (make-default-producer kafka-producer-opts)))
  (stop [c]
    (when-let [p (:producer c)]
      (gregor/close p 2)) ;; 2 seconds to wait to send remaining messages, should this be configurable?
    (dissoc c :producer)))

(ns kafka-component.core
  (:require [com.stuartsierra.component :as component]
            [gregor.core :as gregor])
  (:import [java.util.concurrent Executors TimeUnit]
           org.apache.kafka.common.errors.WakeupException))

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
    (let [kafka-consumer ((or make-kafka-consumer make-default-consumer) kafka-config)
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
          (gregor/close kafka-consumer)))))

  java.io.Closeable
  (close [_]
    (when-let [consumer @consumer-atom]
      (gregor/wakeup consumer))))

(defn make-default-task [{:keys [logger exception-handler consumer-component config]} task-id]
  (->ConsumerAlwaysCommitTask logger
                              exception-handler
                              (:consumer consumer-component)
                              (config :kafka-consumer-config)
                              (partial make-default-consumer (config :topics-or-regex))
                              (atom nil)
                              task-id))

(defn create-task-pool [{:keys [config make-consumer-task] :as c} pool-id]
  (let [pool-size (:pool-size config)
        thread-pool (Executors/newFixedThreadPool pool-size)
        task-ids    (map (partial str pool-id "-") (range pool-size))
        tasks       (map (partial (or make-consumer-task make-default-task) c) task-ids)]
    (doseq [t tasks] (.submit thread-pool t))
    (merge c {:thread-pool thread-pool :tasks tasks})))

(defn stop-task-pool [{:keys [config thread-pool tasks] :as c}]
  (doseq [t tasks] (.close t))
  (when thread-pool
    (.shutdown thread-pool)
    (.awaitTermination thread-pool (config :shutdown-grace-period 4) TimeUnit/SECONDS))
  (dissoc c :thread-pool :tasks))

(defrecord KafkaConsumerPool [config consumer-component logger exception-handler make-consumer-task]
  component/Lifecycle
  (start [c]
    (let [pool-id (pr-str (:topics-or-regex config))
          log     (fn log [& msg]
                    (logger :info (apply str "pool-id=" pool-id " config=" (:kafka-consumer-config config) " " msg)))]
      (log "action=starting-consumption")
      (let [initialized-component (create-task-pool c pool-id)]
        (log "action=started-consumption")
        initialized-component)))

  (stop [{:keys [logger] :as c}]
    (let [pool-id (pr-str (:topics-or-regex config))
          log     (fn log [& msg]
                    (logger :info (apply str "pool-id=" pool-id " config=" (:kafka-consumer-config config) " " msg)))]
      (log "action=stopping-consumption")
      (let [deinitialized-component (stop-task-pool c)]
        (log "action=stopped-consumption")
        deinitialized-component))))

(defrecord KafkaProducerComponent [config make-kafka-producer]
  component/Lifecycle
  (start [c]
    (assoc c :producer ((or make-kafka-producer make-default-producer) config)))
  (stop [c]
    (when-let [p (:producer c)]
      (gregor/close p 2) ;; 2 seconds to wait to send remaining messages, should this be configurable?
      (dissoc c :producer))))

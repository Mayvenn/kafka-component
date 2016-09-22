(ns kafka-component.core
  (:require [com.stuartsierra.component :as component]
            [gregor.core :as gregor])
  (:import [java.util.concurrent Executors TimeUnit]
           [org.apache.kafka.common.errors WakeupException]))

(def default-consumer-config
  {"enable.auto.commit"	"false"
   "max.poll.records" "1"})

(def default-producer-config
  {"acks" "all"
   "retries" "3"
   "max.in.flight.requests.per.connection" "1"})

(defrecord ConsumerTask [logger exception-handler message-consumer kafka-config topics-or-regex consumer-atom task-id]
  java.lang.Runnable
  (run [_]
    (let [kafka-consumer (gregor/consumer (kafka-config "bootstrap.servers")
                                          (kafka-config "group.id")
                                          topics-or-regex
                                          kafka-config)
          log-exception  (fn log-exception [e & msg]
                           (logger :error (apply str "task-id=" task-id " msg=" msg))
                           (logger :error e)
                           (exception-handler e))
          log            (fn log [& msg]
                           (logger :info (apply str "task-id=" task-id " msg=" msg)))]

      (reset! consumer-atom kafka-consumer)
      (try
        (log "starting")

        (doseq [{:keys [topic partition offset key] :as record} (gregor/records kafka-consumer)]
          (log "received message with key: " key)
          (try
            (message-consumer record)
            (gregor/commit-offsets! kafka-consumer [{:topic topic :partition partition :offset (inc offset)}])
            (catch Exception e
              (log-exception e "error in message consumer"))))

        (log "exiting")
        (catch WakeupException e (log "woken up"))
        (catch Exception e (log-exception e "error in kafka consumer task runnable"))
        (finally
          (log "closing kafka consumer")
          (gregor/close kafka-consumer)))))

  java.io.Closeable
  (close [_]
    (gregor/wakeup @consumer-atom)))

(defn create-task-pool [{:keys [config pool-size topics-or-regex consumer-component logger exception-handler] :as c}]
  (let [thread-pool (Executors/newFixedThreadPool pool-size)
        task-ids    (map (partial str pool-id "-") (range pool-size))
        create-task (partial ->ConsumerTask logger exception-handler (:consumer consumer-component)
                              (config :kafka-consumer-config) topics-or-regex (atom nil))
        tasks       (map create-task task-ids)]
    (doseq [t tasks] (.submit thread-pool t))
    (merge c {:thread-pool thread-pool :tasks tasks})))

(defn stop-task-pool [{:keys [thread-pool tasks] :as c}]
  (doseq [t tasks] (.close t))
  (when thread-pool
    (.shutdown thread-pool)
    (.awaitTermination thread-pool (config :shutdown-grace-period 4) TimeUnit/SECONDS))
  (dissoc c :thread-pool :tasks))

(defrecord KafkaCommitAlwaysPool [config pool-size topics-or-regex consumer-component logger exception-handler]
  component/Lifecycle
  (start [c]
    (let [pool-id (pr-str topics-or-regex)]
      (logger :info (str "pool-id=" pool-id " msg=starting consumption"))
      (let [initialized-component (create-task-pool c)]
        (logger :info (str "pool-id=" pool-id " msg=started consumption"))
        initialized-component)))

  (stop [{:keys [logger] :as c}]
    (let [pool-id (pr-str topics-or-regex)]
      (logger :info (str "pool-id=" pool-id " msg=stopping consumption"))
      (let [deinitialized-component (stop-task-pool c)]
        (logger :info (str "pool-id=" pool-id " msg=stopped consumption"))
        deinitialized-component))))

(defrecord KafkaProducer [config]
  component/Lifecycle
  (start [c]
    (assoc c :producer (gregor/producer (config "bootstrap.servers") (merge default-producer-config config))))
  (stop [c]
    (when-let [p (:producer c)]
      (gregor/close p 2) ;; 2 seconds to wait to send remaining messages, should this be configurable?
      (dissoc c :producer))))

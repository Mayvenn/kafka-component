(ns kafka-component.mock
  (:require [com.stuartsierra.component :as component]
            [gregor.core :as gregor]
            [clojure.core.async :as async :refer [>!! <!! chan alt!! timeout admix mix unmix poll!]]
            [kafka-component.core :as core])
  (:import [org.apache.kafka.clients.producer Producer ProducerRecord RecordMetadata Callback]
           [org.apache.kafka.clients.consumer Consumer ConsumerRecord ConsumerRecords]
           [org.apache.kafka.common TopicPartition]
           [java.lang Integer]
           [java.util Collection]
           [java.util.regex Pattern]))

;; structure of broker-state:
;; {"sample-topic" {:messages []
;;                  :registered-consumers []}}
(def broker-state (atom {}))
(def broker-lock (Object.))
(def buffer-size 20)
(def default-num-partitions 2)

(defn reset-broker-state! []
  (locking broker-lock
    (reset! broker-state {})))

(defn fixture-reset-broker-state! [f]
  (reset-broker-state!)
  (f))

(defn create-topic
  ([] (create-topic default-num-partitions))
  ([num-partitions]
   (into [] (repeatedly num-partitions (constantly {:messages [] :registered-subscribers []})))))

(defn ensure-topic [broker-state topic]
  (if (broker-state topic)
    broker-state
    (assoc broker-state topic (create-topic))))

(defn close-mock [state]
  (assoc state :conn-open? false))

(defn add-subscriber-to-partition [subscriber partition]
  (update partition :registered-subscribers conj subscriber))

(defn remove-subscriber-from-partition [subscriber partition]
  (update partition :registered-subscribers remove subscriber))

(defn add-subscriber-in-broker-state [state topic subscriber]
  (-> state
      (ensure-topic topic)
      ;; TODO: Add subscriber to every partition for now, needs to rebalance according to consumer group
      (update topic #(into [] (map (partial add-subscriber-to-partition subscriber) %)))))

(defn remove-subscriber-from-topic [state subscriber topic]
  (update state topic #(into [] (map (partial remove-subscriber-from-partition subscriber) %))))

(defn record->topic-partition [record]
  (TopicPartition. (.topic record) (.partition record)))

(defn slurp-messages [msg-chan max-messages messages]
  (if (< (count messages) max-messages)
    (if-let [msg (poll! msg-chan)]
      (slurp-messages msg-chan max-messages (conj messages msg))
      messages)
    messages))

(defn ->consumer-records [messages]
  (ConsumerRecords. (group-by record->topic-partition messages)))

(defn max-poll-records [config]
  (if-let [max-poll-records-str (config "max.poll.records")]
    (do
      (assert String (type max-poll-records-str))
      (Integer/parseInt max-poll-records-str))
    Integer/MAX_VALUE))

;; TODO: implement missing methods
;; TODO: validate config?
;; TODO: instead of registered topics, it may be easier to have topicpartitions for pause
(defrecord MockConsumer [consumer-state config]
  Consumer
  (assign [_ partitions])
  (close [_])
  (commitAsync [_])
  (commitAsync [_ offsets cb])
  (commitAsync [_ cb])
  (commitSync [_])
  (commitSync [_ offsets])
  (committed [_ partition])
  (listTopics [_])
  (metrics [_])
  (partitionsFor [_ topic])
  (pause [_ partitions])
  (paused [_])
  (poll [_ max-timeout]
    ;; TODO: wakeup on poll
    ;; TODO: on timeout is it empty ConsumerRecords or nil? assuming nil for now
    ;; TODO: use consumer offset and reset largest/smallest to figure out records
    ;; TODO: what does kafka do if not subscribed to any topics? currently assuming nil
    (alt!!
      (:msg-chan @consumer-state) ([msg] (->> [msg]
                                              (slurp-messages (:msg-chan @consumer-state)
                                                              (max-poll-records config))
                                              ->consumer-records))
      (timeout max-timeout) ([_] nil)))
  (position [_ partition])
  (resume [_ partitions])
  (seek [_ partition offset])
  (seekToBeginning [_ partitions])
  (seekToEnd [_ partitions])
  (subscribe [_ topics]
    ;; TODO: what if already subscribed, what does Kafka do?
    (locking broker-lock
      (doseq [topic topics]
        (let [topic-chan (chan buffer-size)]
          (admix (:msg-mixer @consumer-state) topic-chan)
          (swap! consumer-state update :subscribed-topics conj {:topic topic :chan topic-chan})
          (swap! broker-state add-subscriber-in-broker-state topic topic-chan)))))
  (unsubscribe [_]
    (locking broker-lock
      (doseq [{:keys [topic topic-chan] :as subscription} (:subscribed-topics @consumer-state)]
        (swap! broker-state remove-subscriber-from-topic topic-chan topic)
        (unmix (:msg-mixer @consumer-state) topic-chan))
      (swap! consumer-state assoc :subscribed-topics [])))
  (wakeup [_]))

(defn mock-consumer
  ([config] (mock-consumer [] config))
  ([auto-subscribe-topics config]
   (let [msg-chan  (chan buffer-size)
         mock-consumer (->MockConsumer (atom {:msg-mixer (mix msg-chan)
                                              :msg-chan  msg-chan
                                              :subscribed-topics []})
                                       (or config {}))]
     (when (seq auto-subscribe-topics)
       (.subscribe mock-consumer auto-subscribe-topics))
     mock-consumer)))

(defn mock-consumer-task [{:keys [config logger exception-handler consumer-component]} task-id]
  (core/->ConsumerAlwaysCommitTask logger exception-handler (:consumer consumer-component)
                                   (config :kafka-consumer-config) (partial mock-consumer (config :topics-or-regex))
                                   (atom nil) task-id))

(defn mock-consumer-pool
  ([config]
   (core/map->KafkaConsumerPool {:config config
                                 :make-consumer-task mock-consumer-task}))
  ([config consumer-component logger exception-handler]
   (core/->KafkaConsumerPool config consumer-component logger exception-handler mock-consumer-task)))

;; TODO: assertions
(defn assert-proper-config [config])
(defn assert-proper-record [record])
(defn assert-producer-not-closed [producer-state])

(defn producer-record->consumer-record [offset record]
  (ConsumerRecord. (.topic record) (or (.partition record) 0) offset (.key record) (.value record)))

(defn add-record-in-broker-state [state consumer-record]
  (let [topic (.topic consumer-record)]
    (-> state
        (ensure-topic topic)
        (update-in [topic (.partition consumer-record) :messages] conj consumer-record))))

(defn save-record! [record]
  (locking broker-lock
    (let [topic (.topic record)
          offset (count (get-in @broker-state [topic (or (.partition record) 0) :messages]))
          consumer-record (producer-record->consumer-record offset record)
          state-with-record (swap! broker-state add-record-in-broker-state consumer-record)]
      (doseq [subscriber (get-in state-with-record [topic (.partition consumer-record) :registered-subscribers])]
        (>!! subscriber consumer-record))
      consumer-record)))

(def noop-cb
  (reify
    Callback
    (onCompletion [this record-metadata e])))

(defn committed-record-metadata [record]
  (RecordMetadata. (record->topic-partition record) 0 (.offset record)
                   (.timestamp record) (.checksum record)
                   (.serializedKeySize record) (.serializedValueSize record)))


(defrecord MockProducer [producer-state config]
  Producer
  (close [_] (swap! producer-state close-mock))
  (close [_ timeout time-unit] (swap! producer-state close-mock))
  (flush [_])
  (metrics [_] (throw (UnsupportedOperationException.)))
  (partitionsFor [_ topic] (throw (UnsupportedOperationException.)))
  (send [this record]
    (.send this record noop-cb))
  (send [_ producer-record cb]
    (assert-proper-config config)
    (assert-proper-record producer-record)
    (assert-producer-not-closed producer-state)
    (let [consumer-record (save-record! producer-record)
          record-metadata (committed-record-metadata consumer-record)]
      (.onCompletion cb record-metadata nil)
      (future record-metadata))))

(defn mock-producer [config]
  (->MockProducer (atom nil) config))

(defn mock-producer-component [config]
  (core/->KafkaProducerComponent config mock-producer))

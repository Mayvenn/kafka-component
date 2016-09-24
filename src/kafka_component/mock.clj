(ns kafka-component.mock
  (:require [com.stuartsierra.component :as component]
            [gregor.core :as gregor]
            [clojure.core.async :as async :refer :all]
            [kafka-component.core :as core])
  (:import [org.apache.kafka.clients.producer Producer ProducerRecord RecordMetadata Callback]
           [org.apache.kafka.clients.consumer Consumer ConsumerRecord]
           [org.apache.kafka.common TopicPartition]
           [java.util Collection]
           [java.util.regex Pattern]))

;; structure of broker-state:
;; {"sample-topic" {:messages []
;;                  :registered-consumers []}}
(def broker-state (atom {}))
(def broker-lock (Object.))

(defn reset-broker-state! []
  (locking broker-lock
    (reset! broker-state {})))

(defn initial-topic-with-subscriber [initial-subscriber]
  {:messages []
   :registered-subscribers [initial-subscriber]})

(defn initial-topic-with-message [initial-message]
  {:messages [initial-message]
   :registered-subscribers []})

(defn close-mock [state]
  (assoc state :conn-open? false))

(defn add-subscriber-in-broker-state [state topic subscriber]
  (if (state topic)
    (update-in state [topic :registered-subscribers] conj subscriber)
    (assoc state topic (initial-topic-with-subscriber subscriber))))


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
    (alt!!
      (:msg-mix @consumer-state) ([msg] [msg])
      (timeout max-timeout) ([_] nil)
      ))
  (position [_ partition])
  (resume [_ partitions])
  (seek [_ partition offset])
  (seekToBeginning [_ partitions])
  (seekToEnd [_ partitions])
  (subscribe [_ topics]
    (doseq [topic topics]
      (let [msg-chan (chan 10)]
        (admix (:msg-mix @consumer-state) msg-chan)
        (add-subscriber-in-broker-state broker-state topic msg-chan))))
  (unsubscribe [_])
  (wakeup [_]))

(defn make-mock-kafka-consumer [config]
  (->MockConsumer (atom {:msg-mix (mix (chan 10))}) config))

(defn mock-consumer-task [{:keys [config logger exception-handler consumer-component]} task-id]
  (core/->ConsumerAlwaysCommitTask logger exception-handler (:consumer consumer-component)
                                   (config :kafka-consumer-config) (config :topics-or-regex)
                                   make-mock-kafka-consumer (atom nil) task-id))

(defn mock-consumer-pool [config]
  (core/map->KafkaConsumerPool {:config config
                                :make-consumer-task mock-consumer-task}))

;; TODO: assertions
(defn assert-proper-config [config])
(defn assert-proper-record [record])
(defn assert-producer-not-closed [producer-state])

(defn producer-record->consumer-record [offset record]
  (ConsumerRecord. (.topic record) (or (.partition record) 0) offset (.key record) (.value record)))

(defn add-record-in-broker-state [state consumer-record]
  (let [topic (.topic consumer-record)]
    (if (state topic)
      (update-in state [topic :messages] conj consumer-record)
      (assoc state topic (initial-topic-with-message consumer-record)))))

(defn save-record! [record]
  (locking broker-lock
    (let [topic (.topic record)
          offset (count (get-in @broker-state [topic :messages]))
          consumer-record (producer-record->consumer-record offset record)
          state-with-record (swap! broker-state add-record-in-broker-state consumer-record)]
      (doseq [subscriber (:registered-subscribers state-with-record)]
        (>!! subscriber consumer-record))
      consumer-record)))

(def noop-cb
  (reify
    Callback
    (onCompletion [this record-metadata e])))

(defn committed-record-metadata [record]
  (let [topic-partition (TopicPartition. (.topic record) (.partition record))]
    (RecordMetadata. topic-partition 0 (.offset record)
                     (.timestamp record) (.checksum record)
                     (.serializedKeySize record) (.serializedValueSize record))))


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

(defn mock-producer-component [config]
  (core/->KafkaProducerComponent config (partial ->MockProducer (atom nil))))

(comment
  (def producer (->MockProducer {} (atom nil)))

  (def res (gregor/send producer "test-topic" "key" "value2"))

  @res

  @broker-state


  )

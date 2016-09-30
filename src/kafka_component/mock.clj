(ns kafka-component.mock
  (:require [clojure.core.async :refer [<! >! >!! alt! alt!! chan close! go poll! sliding-buffer timeout]]
            [kafka-component.core :as core])
  (:import java.lang.Integer
           java.util.Collection
           java.util.regex.Pattern
           [org.apache.kafka.clients.consumer Consumer ConsumerRebalanceListener ConsumerRecord ConsumerRecords]
           [org.apache.kafka.clients.producer Callback Producer ProducerRecord RecordMetadata]
           [org.apache.kafka.common.errors InvalidOffsetException WakeupException]
           org.apache.kafka.common.TopicPartition))

;; TODO: update README for new consumer config/constructors
;; TODO: pull out some of the timeouts as constants so it's easier to see that all the timeouts make sense together

;; structure of broker-state:
;; {"sample-topic" [partition-state *]}
;; where partition-state is:
;; {:messages [first-msg second-msg] :watchers chan-of-interested-consumers}
;; TODO: where should all the random comm chans go, they are siblings of topics in broker state right now, weird
(def broker-state (atom {}))

;; structure of committed-offsets:
;; {[group-id topic-partition] 10}
(def committed-offsets (atom {}))

(def buffer-size 20)
(def default-num-partitions 2)
(def consumer-backoff 20)

(defn logger [& args]
  (locking println
    (apply println args)))

(defn reset-state! []
  ;; TODO: wait until everyone is shutdown before clearing these
  (reset! broker-state {})
  (reset! committed-offsets {}))

(defn ->topic-partition [topic partition]
  (TopicPartition. topic partition))

(defn record->topic-partition [record]
  (TopicPartition. (.topic record) (.partition record)))

(defn broker-create-topic
  ([] (broker-create-topic default-num-partitions))
  ([num-partitions]
   (into [] (repeatedly num-partitions (constantly {:messages [] :watchers (chan (sliding-buffer buffer-size))})))))

(defn broker-ensure-topic [broker-state topic]
  (if (broker-state topic)
    broker-state
    (assoc broker-state topic (broker-create-topic))))

(defn producer-record->consumer-record [offset record]
  (ConsumerRecord. (.topic record) (.partition record) offset (.key record) (.value record)))

(defn add-record-to-topic [state producer-record]
  (let [topic (.topic producer-record)
        offset (count (get-in state [topic (.partition producer-record) :messages]))
        consumer-record (producer-record->consumer-record offset producer-record)]
    (update-in state [topic (.partition consumer-record) :messages] conj consumer-record)))

(defn add-record-in-broker-state [state producer-record]
  (let [topic (.topic producer-record)]
    (-> state
        (broker-ensure-topic topic)
        (add-record-to-topic producer-record))))

(defmacro goe
  {:style/indent 0}
  [& body]
  `(go
     (try ~@body
          (catch Exception e#
            (do (logger e#)
                (throw e#))))))

(defmacro goe-loop
  {:style/indent 1}
  [& body]
  `(goe (loop ~@body)))

(defn close-all-from [ch]
  ;; the consumers back off to avoid flooding this channel but in some malicious
  ;; scenarios this could loop forever. Could add a max watchers param if we wanted
  ;; keep this under control or send over some sort of communication as to which
  ;; tick this occurred in to try to know when to stop? Or an entirely different design.
  ;; It is less of a problem now that the watchers are a sliding buffer, though
  ;; technically they could fill faster than we can drain
  (loop []
    (when-let [o (poll! ch)]
      (close! o)
      (recur))))

(defn committed-record-metadata [record]
  (RecordMetadata. (record->topic-partition record) 0 (.offset record)
                   (.timestamp record) (.checksum record)
                   (.serializedKeySize record) (.serializedValueSize record)))

(defn broker-save-record! [state record]
  (let [topic (.topic record)
        record-with-partition (ProducerRecord. topic (or (.partition record) (int 0)) (.key record) (.value record))
        state-with-record (swap! state add-record-in-broker-state record-with-partition)
        partition (get-in state-with-record [topic (.partition record-with-partition)])]
    (close-all-from (:watchers partition))
    (committed-record-metadata (last (:messages partition)))))

(defn broker-receive-messages [state msg-ch close-ch]
  (goe-loop []
    (alt!
      msg-ch ([[res-ch msg]]
              (when res-ch
                (>! res-ch (broker-save-record! state msg))
                (close! res-ch)
                (recur)))
      close-ch nil)))

(defprotocol IRebalance
  (all-topics [this])
  (apply-pending-topics [this topics])
  (clean-up-subscriptions [this]))

(defn generate-partition-assignments [broker-state consumers participants participants-ch complete-ch]
  (let [broker-state @broker-state
        topics (distinct (mapcat all-topics consumers))

        participants->assignments
        (apply merge-with concat {}
               (for [topic topics
                     :let [subscribed-participants (filterv #(contains? (set (all-topics %)) topic) participants)]
                     partition (range (count (broker-state topic)))
                     :let [participant (get subscribed-participants (mod partition (max (count participants) 1)))]
                     :when participant]
                 {participant [(->topic-partition topic partition)]}))]
    (doseq [consumer consumers]
      (let [assignments (participants->assignments consumer)]
        (.assign consumer (or assignments []))))
    (close! participants-ch)
    (close! complete-ch)))

(defn perform-rebalance [broker-state consumers participants-ch complete-ch]
  (goe-loop [participants []]
    (alt!
      participants-ch ([participant]
                       (when participant
                         (let [participants' (conj participants participant)]
                           (if (>= (count participants') (count consumers))
                             (generate-partition-assignments broker-state consumers participants' participants-ch complete-ch)
                             (recur participants')))))
      (timeout 1000) (generate-partition-assignments broker-state consumers participants participants-ch complete-ch))))

(defn rebalance-consumers [relevant-consumers broker-state rebalance-complete-ch]
  (let [rebalance-participants-ch (chan buffer-size)]
    (doseq [c relevant-consumers]
      (>!! (:rebalance-control-ch c) [rebalance-participants-ch rebalance-complete-ch]))
    (perform-rebalance broker-state relevant-consumers rebalance-participants-ch rebalance-complete-ch)))

(defn consumer-coordinator [state broker-state join-ch leave-ch close-ch]
  (goe-loop []
    (alt!
      join-ch ([[consumer topics]]
               (when consumer
                 (let [group-id (get-in consumer [:config "group.id"] "")
                       rebalance-complete-ch (chan)]
                   (apply-pending-topics consumer topics)
                   (rebalance-consumers (get (swap! state update group-id (fnil conj #{}) consumer)
                                             group-id)
                                        broker-state
                                        rebalance-complete-ch)
                   (<! rebalance-complete-ch)
                   (recur))))
      leave-ch ([consumer]
                (when consumer
                  (let [group-id (get-in consumer [:config "group.id"] "")
                        rebalance-complete-ch (chan)]
                    (clean-up-subscriptions consumer)
                    (rebalance-consumers (get (swap! state update group-id disj consumer)
                                              group-id)
                                         broker-state
                                         rebalance-complete-ch)
                    (<! rebalance-complete-ch)
                    (recur))))
      close-ch nil)))

(defn start! []
  (let [msg-ch (chan buffer-size)
        shutdown-ch (chan)
        join-ch (chan)
        leave-ch (chan)
        ;; {"group1" #{consumer-1 consumer-2}}
        consumer-coordinator-state (atom {})]
    (broker-receive-messages broker-state msg-ch shutdown-ch)
    (consumer-coordinator consumer-coordinator-state broker-state join-ch leave-ch shutdown-ch)
    (reset! broker-state {:msg-ch msg-ch :shutdown-ch shutdown-ch :join-ch join-ch :leave-ch leave-ch})))

(defn shutdown! []
  (let [{:keys [shutdown-ch msg-ch join-ch leave-ch]} @broker-state]
    (close! shutdown-ch)
    (close! msg-ch)
    (close! join-ch)
    (close! leave-ch)
    (reset-state!)))

(defn fixture-restart-broker! [f]
  (start!)
  (f)
  (shutdown!))

(defn close-mock [state]
  (assoc state :conn-open? false))

(defn read-offsets [grouped-messages]
  (into {} (for [[topic-partition msg-list] grouped-messages]
             [topic-partition (inc (.offset (last msg-list)))])))

(defn max-poll-records [config]
  (if-let [max-poll-records-str (config "max.poll.records")]
    (do
      (assert String (type max-poll-records-str))
      (Integer/parseInt max-poll-records-str))
    Integer/MAX_VALUE))

(defn get-offset [broker-state topic partition config]
  (let [group-id (config "group.id" "")]
    (if-let [committed-offset (get @committed-offsets [group-id (->topic-partition topic partition)])]
      committed-offset
      (case (config "auto.offset.reset")
        "earliest" 0
        "latest"   (count (get-in broker-state [topic partition :messages]))
        "none"     (throw (InvalidOffsetException. (str "auto.offset.reset=none, no existing offset for group " group-id " topic " topic " partition " partition)))))))

(defn assert-proper-consumer-config [config]
  (let [auto-offset (config "auto.offset.reset")]
    (when-not (#{"earliest" "latest" "none"} auto-offset)
      (throw (InvalidOffsetException. (str "auto.offset.reset=" auto-offset ", invalid configuration"))))))

(defn ^:private desires-repoll [state subscribed-topic-partitions]
  (let [poll-chan (chan buffer-size)]
    (doseq [[topic-partition _] subscribed-topic-partitions]
      (>!! (get-in state [(.topic topic-partition) (.partition topic-partition) :watchers]) poll-chan))
    poll-chan))

(defn ^:private read-messages [state subscribed-topic-partitions config]
  ;; TODO: round robin across topic-partitions? seems not that necessary right now
  ;; TODO: what happens if you try to read partitions you don't "own"
  (let [messages (mapcat (fn unread-messages [[topic-partition read-offset]]
                           (let [topic (.topic topic-partition)
                                 partition (.partition topic-partition)
                                 messages (get-in state [topic partition :messages])]
                             (when (< read-offset (count messages))
                               (subvec messages read-offset))))
                         subscribed-topic-partitions)]
    (->> messages
         (take (max-poll-records config))
         (group-by record->topic-partition))))

;; TODO: implement missing methods
;; TODO: validate config?
(defrecord MockConsumer [consumer-state wakeup-ch rebalance-control-ch join-ch leave-ch logger config]
  IRebalance
  (all-topics [_] (:subscribed-topics @consumer-state))
  (apply-pending-topics [_ topics]
    (swap! consumer-state assoc :subscribed-topics topics))
  (clean-up-subscriptions [_]
    (swap! consumer-state assoc :subscribed-topics [] :subscribed-topic-partitions {}))
  Consumer
  (assign [_ partitions]
    (let [broker-state @broker-state]
      (swap! consumer-state assoc :subscribed-topic-partitions
             (reduce (fn [m topic-partition]
                       (assoc m topic-partition
                              (get-offset broker-state (.topic topic-partition) (.partition topic-partition) config)))
                     {} partitions))))
  (close [this]
    (swap! consumer-state close-mock)
    (.unsubscribe this))
  (commitAsync [_] (throw (UnsupportedOperationException.)))
  (commitAsync [_ offsets cb] (throw (UnsupportedOperationException.)))
  (commitAsync [_ cb] (throw (UnsupportedOperationException.)))
  (commitSync [_] (throw (UnsupportedOperationException.)))
  (commitSync [_ offsets]
    (let [new-commits (reduce (fn [m [topic-partition offset-and-metadata]]
                                (assoc m [(config "group.id" "") topic-partition]
                                       (.offset offset-and-metadata)))
                              {}
                              offsets)]
      (swap! committed-offsets merge new-commits)))
  (committed [_ partition] (throw (UnsupportedOperationException.)))
  (listTopics [_] (throw (UnsupportedOperationException.)))
  (metrics [_] (throw (UnsupportedOperationException.)))
  (partitionsFor [_ topic] (throw (UnsupportedOperationException.)))
  (pause [_ partitions] (throw (UnsupportedOperationException.)))
  (paused [_] (throw (UnsupportedOperationException.)))
  (poll [this max-timeout]
    ;; TODO: assert not closed
    (alt!!
      rebalance-control-ch ([[rebalance-participants-ch rebalance-complete-ch]]
                            (>!! rebalance-participants-ch this)
                            (alt!!
                              rebalance-complete-ch nil
                              (timeout 2000) (throw (Exception. "dead waiting for rebalance")))
                            (.poll this max-timeout))

      ;; Somebody outside needs to shutdown quickly, aborting the poll loop
      wakeup-ch (throw (WakeupException.))

      :default
      (let [{:keys [subscribed-topic-partitions]} @consumer-state
            ;; Tell broker that if it doesn't have messages now, but gets them
            ;; while we're waiting for the timeout, we'd like to be interupted.
            ;; This prevents excessive waiting and handles the case of an
            ;; infinite max-timeout.
            poll-chan (desires-repoll @broker-state subscribed-topic-partitions)
            ;; Need to re-read broker state immediately after setting watchers,
            ;; so that we see any messages created between the time the poll
            ;; started and when we registered. The first read of the broker
            ;; state was just to find out where to put poll-chan
            topic-partition->messages (read-messages @broker-state subscribed-topic-partitions config)]
        (if (seq topic-partition->messages)
          (do
            (swap! consumer-state update :subscribed-topic-partitions merge (read-offsets topic-partition->messages))
            (ConsumerRecords. topic-partition->messages))
          ;; Maybe we didn't actually have any messages to read
          (alt!!
            ;; We've waited too long for messages, give up
            (timeout max-timeout) ([_]
                                   ;; TODO: on timeout is it empty ConsumerRecords or nil? assuming nil for now
                                   ;; TODO: what does kafka do if not subscribed to any topics? currently assuming nil
                                   ;; TODO: read one last time, maybe with (.poll this 0),
                                   ;; but avoiding an infinite loop somehow?
                                   nil)
            ;; But, before the timeout, broker got new messsages on some
            ;; topic+partition that this consumer is interested in. It is
            ;; possible through race conditions that this signal was a
            ;; lie, that is, that we already read the messages the broker
            ;; is trying to tell us about, but it is harmless to retry as
            ;; long as we back off a little bit to avoid flooding the
            ;; watchers channel
            poll-chan ([_]
                       (Thread/sleep consumer-backoff)
                       (.poll this max-timeout))
            ;; Somebody outside needs to shutdown quickly, and is aborting
            ;; the poll loop
            wakeup-ch (throw (WakeupException.)))))))
  (position [_ partition]
    ;; Not hard, but not valuable
    (throw (UnsupportedOperationException.)))
  (resume [_ partitions] (throw (UnsupportedOperationException.)))
  (seek [_ partition offset] (throw (UnsupportedOperationException.)))
  (seekToBeginning [_ partitions] (throw (UnsupportedOperationException.)))
  (seekToEnd [_ partitions] (throw (UnsupportedOperationException.)))
  (^void subscribe [^Consumer this ^Collection topics]
   (assert-proper-consumer-config config)
   ;; TODO: what if already subscribed, what does Kafka do?
   (swap! broker-state #(reduce (fn [state topic] (broker-ensure-topic state topic)) % topics))
   (>!! join-ch [this topics]))
  (^void subscribe [^Consumer this ^Collection topics ^ConsumerRebalanceListener listener]
   (throw (UnsupportedOperationException.)))
  (^void subscribe [^Consumer this ^Pattern pattern ^ConsumerRebalanceListener listener]
   (throw (UnsupportedOperationException.)))
  (unsubscribe [this]
    (alt!!
      [[leave-ch this]] :wrote
      (timeout 5000) (throw (Exception. "dead waiting to unsubscribe"))))
  (wakeup [_]
    (close! wakeup-ch)))

(defn mock-consumer
  ([config] (mock-consumer [] config))
  ([auto-subscribe-topics config]
   (let [mock-consumer (->MockConsumer (atom {:subscribed-topic-partitions {}})
                                       (chan)
                                       (chan buffer-size)
                                       (:join-ch @broker-state)
                                       (:leave-ch @broker-state)
                                       logger
                                       (merge core/default-consumer-config config))]
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

(def noop-cb
  (reify
    Callback
    (onCompletion [this record-metadata e])))

(defrecord MockProducer [producer-state msg-ch config]
  Producer
  (close [_] (swap! producer-state close-mock))
  (close [_ timeout time-unit] (swap! producer-state close-mock))
  (flush [_] (throw (UnsupportedOperationException.)))
  (metrics [_] (throw (UnsupportedOperationException.)))
  (partitionsFor [_ topic] (throw (UnsupportedOperationException.)))
  (send [this record]
    (.send this record noop-cb))
  (send [_ producer-record cb]
    (assert-proper-config config)
    (assert-proper-record producer-record)
    (assert-producer-not-closed producer-state)
    (let [res-ch (chan 1)
          rtn-promise (promise)]
      (goe
        (>! msg-ch [res-ch producer-record])
        (let [committed-record-metadata (<! res-ch)]
          (.onCompletion cb committed-record-metadata nil)
          (deliver rtn-promise committed-record-metadata)))
      (future @rtn-promise))))

(defn mock-producer [config]
  (->MockProducer (atom nil) (:msg-ch @broker-state) (merge core/default-producer-config config)))

(defn mock-producer-component [config]
  (core/->KafkaProducerComponent config mock-producer))

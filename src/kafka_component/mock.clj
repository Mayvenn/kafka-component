(ns kafka-component.mock
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as async :refer [>! <! alt! >!! <!! chan alt!! timeout close! poll! go go-loop sliding-buffer]]
            [kafka-component.core :as core])
  (:import [org.apache.kafka.clients.producer Producer ProducerRecord RecordMetadata Callback]
           [org.apache.kafka.clients.consumer Consumer ConsumerRecord ConsumerRecords ConsumerRebalanceListener]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.errors WakeupException]
           [java.lang Integer]
           [java.util Collection]
           [java.util.regex Pattern]))

;; TODO: where should all the random comm chans go, they are siblings of topics in broker state right now, weird
;; TODO: update README for new consumer config/constructors
;; TODO: pull out some of the timeouts as constants so it's easier to see that all the timeouts make sense together

;; structure of broker-state:
;; {"sample-topic" [partition-state *]}
;; where partition-state is:
;; {:messages [first-msg second-msg] :watchers chan-of-interested-consumers}
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
              (>! res-ch (broker-save-record! state msg))
              (close! res-ch)
              (recur))
      close-ch nil)))

(defprotocol IRebalance
  (prepare-for-rebalance [this rebalance-participants-ch rebalance-complete-ch])
  (all-topics [this])
  (apply-pending-topics [this topics])
  (clean-up-subscriptions [this]))

(defn generate-partition-assignments [broker-state consumers participants participants-ch complete-ch]
  (let [broker-state @broker-state
        topics (distinct (mapcat all-topics consumers))
        topic->participants (reduce (fn [m topic]
                                      (assoc m topic
                                             (filter #((into #{} (all-topics %)) topic) participants)))
                                    {} topics)
        participants->assignments (reduce (fn [m [topic participants]]
                                            (let [partition-count (count (broker-state topic))
                                                  ;; TODO: what to do if there are no participants in this topic? I.e. is the (max 1) safe?
                                                  partition-breakdowns (partition-all (/ partition-count (max (count participants) 1)) (range partition-count))]
                                              (merge-with concat m (into {} (map vector participants
                                                                                 (map (fn [partition-breakdown]
                                                                                        (map (partial ->topic-partition topic) partition-breakdown))
                                                                                      partition-breakdowns))))))
                                          {}
                                          topic->participants)]
    (doseq [consumer consumers]
      (let [assignments (participants->assignments consumer)]
        (.assign consumer (or assignments []))))
    (close! participants-ch)
    (close! complete-ch)))

(defn perform-rebalance [broker-state consumers participants-ch complete-ch]
  (goe-loop [participants []]
    (alt!
      participants-ch ([participant]
                       (let [participants' (conj participants participant)]
                         (if (>= (count participants') (count consumers))
                           (generate-partition-assignments broker-state consumers participants' participants-ch complete-ch)
                           (recur participants'))))
      (timeout 1000) (generate-partition-assignments broker-state consumers participants participants-ch complete-ch))))

(defn rebalance-consumers [relevant-consumers broker-state rebalance-complete-ch]
  (let [rebalance-participants-ch (chan buffer-size)]
    (doseq [c relevant-consumers]
      (prepare-for-rebalance c rebalance-participants-ch rebalance-complete-ch))
    (perform-rebalance broker-state relevant-consumers rebalance-participants-ch rebalance-complete-ch)))

(defn consumer-coordinator [state broker-state join-ch leave-ch close-ch]
  (goe-loop []
    (alt!
      join-ch ([[consumer topics]]
               (let [group-id (get-in consumer [:config "group.id"] "")
                     rebalance-complete-ch (chan)]
                 (apply-pending-topics consumer topics)
                 (rebalance-consumers (get (swap! state update group-id (fnil conj #{}) consumer)
                                           group-id)
                                      broker-state
                                      rebalance-complete-ch)
                 (<! rebalance-complete-ch)
                 (recur)))
      leave-ch ([consumer]
                (let [group-id (get-in consumer [:config "group.id"] "")
                      rebalance-complete-ch (chan)]
                  (clean-up-subscriptions consumer)
                  (rebalance-consumers (get (swap! state update group-id disj consumer)
                                            group-id)
                                       broker-state
                                       rebalance-complete-ch)
                  (<! rebalance-complete-ch)
                  (recur)))
      close-ch nil)))

(defn start! []
  (let [msg-ch (chan buffer-size)
        shutdown-ch (chan)
        join-ch (chan)
        leave-ch (chan)
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

;; TODO: support "none"
;; TODO: anything other than earliest, latest, none is to throw an exception
(defn get-offset [broker-state topic partition config]
  (let [group-id (config "group.id" "")]
    (if-let [committed-offset (get @committed-offsets [group-id (->topic-partition topic partition)])]
      committed-offset
      (let [latest-offset (count (get-in broker-state [topic partition :messages]))]
        (case (config "auto.offset.reset")
          "earliest" 0
          "latest" latest-offset
          "none" (throw (UnsupportedOperationException.))
          latest-offset)))))

;; TODO: implement missing methods
;; TODO: validate config?
(defrecord MockConsumer [consumer-state join-ch leave-ch logger config]
  IRebalance
  (prepare-for-rebalance [_ rebalance-participants-ch rebalance-complete-ch]
    (swap! consumer-state assoc
           :rebalance-participants-ch rebalance-participants-ch
           :rebalance-complete-ch rebalance-complete-ch))
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
  (commitAsync [_])
  (commitAsync [_ offsets cb])
  (commitAsync [_ cb])
  (commitSync [_])
  (commitSync [_ offsets]
    (let [new-commits (reduce (fn [m [topic-partition offset-and-metadata]]
                                (assoc m [(config "group.id" "") topic-partition]
                                       (.offset offset-and-metadata)))
                              {}
                              offsets)]
      (swap! committed-offsets merge new-commits)))
  (committed [_ partition])
  (listTopics [_] (throw (UnsupportedOperationException.)))
  (metrics [_] (throw (UnsupportedOperationException.)))
  (partitionsFor [_ topic])
  (pause [_ partitions])
  (paused [_])
  (poll [this max-timeout]
    ;; TODO: on timeout is it empty ConsumerRecords or nil? assuming nil for now
    ;; TODO: what does kafka do if not subscribed to any topics? currently assuming nil
    ;; TODO: round robin across topic-partitions? seems not that necessary right now
    ;; TODO: assert not closed
    ;; TODO: what happens if you try to read partitions you don't "own"
    ;; TODO: it seems like we can have a long running rebalance-control-ch that sends [rebalance-participants-ch rebalance-complete-ch] and then switch the if conditions around rebalancing and waking up to an alt!! which would clean this up a bit and remove quite a few keys in the consumer-state
    (let [state @broker-state
          {:keys [subscribed-topic-partitions wakeup-chan woken-up? rebalance-participants-ch rebalance-complete-ch]} @consumer-state
          poll-chan (chan buffer-size)]
      (if rebalance-participants-ch
        (do
          (>!! rebalance-participants-ch this)
          (alt!!
            rebalance-complete-ch nil
            (timeout 2000) (throw (Exception. "dead waiting for rebalance")))
          (swap! consumer-state dissoc :rebalance-participants-ch :rebalance-complete-ch)
          (.poll this max-timeout))
        (if woken-up?
          (throw (WakeupException.))
          (do
            ;; Tell broker that if it doesn't have messages now, but gets them
            ;; while we're waiting for the timeout, we'd like to be interupted.
            ;; This prevents excessive waiting and handles the case of an
            ;; infinite max-timeout.
            (doseq [[topic-partition _] subscribed-topic-partitions]
              (>!! (get-in state [(.topic topic-partition) (.partition topic-partition) :watchers]) poll-chan))
            ;; Need to re-read broker state immediately after setting watchers,
            ;; so that we see any messages created between the time the poll
            ;; started and when we registered. The first read of the broker
            ;; state was just to find out where to put poll-chan
            (let [state @broker-state
                  messages (mapcat (fn [[topic-partition read-offset]]
                                     (let [topic (.topic topic-partition)
                                           partition (.partition topic-partition)
                                           messages (get-in state [topic partition :messages])]
                                       (when (< read-offset (count messages))
                                         (subvec messages read-offset))))
                                   subscribed-topic-partitions)
                  read-messages (take (max-poll-records config) messages)
                  grouped-messages (group-by record->topic-partition read-messages)
                  new-read-offsets (read-offsets grouped-messages)]
              (if (> (count read-messages) 0)
                (do
                  (swap! consumer-state update :subscribed-topic-partitions merge new-read-offsets)
                  (ConsumerRecords. grouped-messages))
                ;; Maybe we didn't actually have any messages to read
                (alt!!
                  ;; We've waited too long for messages, give up
                  (timeout max-timeout) nil
                  ;; But, before the timeout, broker got new messsages on some
                  ;; topic+partition that this consumer is interested in. It is
                  ;; possible through race conditions that this signal was a
                  ;; lie, that is, that we already read the messages the broker
                  ;; is trying to tell us about, but it is harmless to retry as
                  ;; long as we back off a little bit to avoid flooding the
                  ;; message channel
                  poll-chan ([_]
                             (Thread/sleep consumer-backoff)
                             (.poll this max-timeout))
                  ;; Somebody outside needs to shutdown quickly, and is aborting
                  ;; the poll loop
                  wakeup-chan (throw (WakeupException.))))))))))
  (position [_ partition])
  (resume [_ partitions])
  (seek [_ partition offset])
  (seekToBeginning [_ partitions])
  (seekToEnd [_ partitions])
  (^void subscribe [^Consumer this ^Collection topics]
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
    (swap! consumer-state assoc :woken-up? true)
    (close! (:wakeup-chan @consumer-state))))

(defn mock-consumer
  ([config] (mock-consumer [] config))
  ([auto-subscribe-topics config]
   (let [mock-consumer (->MockConsumer (atom {:subscribed-topic-partitions {}
                                              :wakeup-chan (chan)})
                                       (:join-ch @broker-state)
                                       (:leave-ch @broker-state)
                                       logger
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

(def noop-cb
  (reify
    Callback
    (onCompletion [this record-metadata e])))

(defrecord MockProducer [producer-state msg-ch config]
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
    (let [res-ch (chan 1)
          rtn-promise (promise)]
      (goe
        (>! msg-ch [res-ch producer-record])
        (let [committed-record-metadata (<! res-ch)]
          (.onCompletion cb committed-record-metadata nil)
          (deliver rtn-promise committed-record-metadata)))
      (future @rtn-promise))))

(defn mock-producer [config]
  (->MockProducer (atom nil) (:msg-ch @broker-state) config))

(defn mock-producer-component [config]
  (core/->KafkaProducerComponent config mock-producer))

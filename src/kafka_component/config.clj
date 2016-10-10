(ns kafka-component.config)

(def default-consumer-config
  {"enable.auto.commit"	"false"
   "max.poll.records" "1"})

(def default-producer-config
  {"acks" "all"
   "retries" "3"
   "max.in.flight.requests.per.connection" "1"})

(defn assert-non-nil-values [m]
  (doseq [[k v] m]
    (assert v (format "%s cannot be nil" k))))

(defn assert-required-consumer-keys [config]
  (assert (#{"latest" "earliest" "none"} (get config "auto.offset.reset")) "\"auto.offset.reset\" should be one of #{\"latest\" \"earliest\" \"none\"}")
  (assert (get config "bootstrap.servers") "\"bootstrap.servers\" must be provided in config")
  (assert (get config "group.id") "\"group.id\" must be provided in config"))


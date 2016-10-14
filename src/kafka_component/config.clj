(ns kafka-component.config)

(def default-consumer-config
  {"enable.auto.commit"	"false"
   "max.poll.records"   "1"})

(def default-producer-config
  {"acks"                                  "all"
   "retries"                               "3"
   "max.in.flight.requests.per.connection" "1"})

(defn assert-non-nil-values [m]
  (doseq [[k v] m]
    (assert v (format "%s cannot be nil in the config" k))))

(defn assert-contains-keys [m & ks]
  (doseq [k ks]
    (assert (contains? m k) (format "\"%s\" must be provided in the config" k))))

(defn assert-string-values [m]
  (doseq [[k v] m]
    (assert (string? v) (format "%s must be a string" k))))

(defn assert-request-timeout-valid [opts]
  (let [request-timeout (. Integer parseInt (get opts "request.timeout.ms" "40000"))
        session-timeout (. Integer parseInt (get opts "session.timeout.ms" "30000"))
        fetch-max-wait  (. Integer parseInt (get opts "fetch.max.wait.ms" "500"))]
    (assert (and (> request-timeout session-timeout)
                 (> request-timeout fetch-max-wait)))))

(defn assert-consumer-opts [opts]
  (try
    (assert opts "Kafka consumer options cannot be nil")
    (assert (#{"latest" "earliest" "none"} (get opts "auto.offset.reset"))
            "\"auto.offset.reset\" should be set to one of #{\"latest\" \"earliest\" \"none\"}")
    (assert-contains-keys opts "bootstrap.servers" "group.id")
    (assert-non-nil-values opts)
    (assert-string-values opts)
    (assert-request-timeout-valid opts)
    (catch Throwable e
      (throw (Exception. e)))))

(defn assert-producer-opts [opts]
  (try
    (assert opts "Kafka producer options cannot be nil")
    (assert-non-nil-values opts)
    (catch Exception e
      (throw e))))

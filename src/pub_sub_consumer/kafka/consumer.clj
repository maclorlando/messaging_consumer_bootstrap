(ns pub-sub-consumer.kafka.consumer
  "Kafka consumer adapter wich provides a polling in loop to dispatch messages to the pedestal interceptors chain.
   It also provides a MockConsumer with some functions to help tests."
  (:require [cheshire.core :as json]
            [clojure.java.io :as jio]
            [clojure.stacktrace :as stacktrace]
            [clojure.string :as str]
            [io.pedestal.log :as logger]
            [payment-webhooks.pedestal :refer [test-service?]])
  (:import [java.time Duration]
           [java.util Properties]
           [org.apache.kafka.clients.consumer
            ConsumerConfig
            ConsumerRecord
            KafkaConsumer
            MockConsumer
            OffsetResetStrategy]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.config SaslConfigs]
           [org.apache.kafka.common.errors WakeupException]))

(defn- service-map->properties [service-map]
  (let [{config-fname ::config.file
         bootstrap-servers ::bootstrap.servers
         username ::username
         password ::password
         client-id ::client.id
         group-id ::group.id} service-map]

    (with-open [config-file (jio/reader config-fname)]
      (doto (Properties.)
        (.putAll
         {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG        bootstrap-servers
          SaslConfigs/SASL_JAAS_CONFIG                   (str/join " " ["org.apache.kafka.common.security.plain.PlainLoginModule" "required"
                                                                        (str "username='" username "'")
                                                                        (str "password='" password "';")])
          ConsumerConfig/CLIENT_ID_CONFIG                (str/join "-" [client-id (str (random-uuid))])
          ConsumerConfig/GROUP_ID_CONFIG                 group-id
          ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringDeserializer"
          ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
          ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG       "false"})
        (.load config-file)))))

(defn- create-consumer [service-map]
  (if (test-service? service-map)
    (-> service-map
        (assoc ::client-id (::client.id service-map))
        (assoc ::consumer (MockConsumer. OffsetResetStrategy/EARLIEST)))

    (let [props (service-map->properties service-map)
          client-id (.getProperty props "client.id")]
      (-> service-map
          (assoc ::client-id client-id)
          (assoc ::consumer (KafkaConsumer. props))
          ;; Discard kafka credentials values from service-map
          (dissoc ::config.file ::bootstrap.servers ::username ::password)))))

(defn- pool-and-dispatch [consumer client-id topic dispatcher-fn]
  (logger/info :polling-messages client-id :topic topic)

  (let [msgs (.poll consumer (Duration/ofSeconds 5))]
    (when (< 0 (.count msgs))
      (dispatcher-fn client-id consumer msgs))))

(defn- fetch-committed-msgs [service-map]
  (let [consumer (::consumer service-map)]
    (.committed consumer (.assignment consumer))))

(defn- start-loop [consumer client-id topic dispatcher-fn service-map]
  (let [continue? (atom true)
        last-committed (atom (java.util.Collections/singletonMap (TopicPartition. "" 0) 0))
        last-exception (atom (RuntimeException.))
        completion (future
                     (try
                       (while @continue?
                         (try
                           (pool-and-dispatch consumer client-id topic dispatcher-fn)
                           (catch WakeupException _)))
                       :ok
                       (catch Throwable t
                         (reset! last-exception t)
                         (logger/error :msg "Dispatch code threw an exception"
                                       :cause t
                                       :cause-trace (with-out-str
                                                      (stacktrace/print-cause-trace t))))

                       (finally
                         (reset! last-committed (fetch-committed-msgs service-map))
                         (.unsubscribe consumer)
                         (.close consumer)
                         (logger/warn :polling-stoped client-id))))]

    {::continue? continue?
     ::completion completion
     ::last-committed last-committed
     ::last-exception last-exception}))

(def ^:private test-partition 0)
(def ^:private test-key "TEST")

(defn- init-mock [consumer topic]
  (.rebalance consumer (java.util.Collections/singletonList (TopicPartition. topic test-partition)))
  (.updateBeginningOffsets consumer (java.util.Collections/singletonMap (TopicPartition. topic test-partition) 0)))

(defn- start [service-map]
  (let [{consumer ::consumer
         client-id ::client-id
         topic ::topic
         dispatcher-fn ::dispatcher-fn} service-map]

    (logger/info :starting-kafka-consumer client-id :topic topic)
    (.subscribe consumer [topic])

    (when (test-service? service-map)
      (init-mock consumer topic))

    (start-loop consumer client-id topic dispatcher-fn service-map)))

(defn- stop [service-map]
  (let [{consumer ::consumer
         continue? ::continue?
         completion ::completion} service-map]

    (when continue?
      (reset! continue? false)
      (.wakeup consumer)
      (deref completion 5000 :timeout))))

(defn- test-msg [service-map data]
  (let [{consumer ::consumer
         topic ::topic} service-map
        offsetPosition (.position consumer (TopicPartition. topic test-partition))]

    (.addRecord consumer (ConsumerRecord. topic test-partition offsetPosition test-key (json/encode data)))))

(defn- check-committed [service-map]
  (deref (::last-committed service-map)))

(defn- check-exception [service-map]
  (deref (::last-exception service-map)))

(defn create
  [service-map]
  (let [consumer (create-consumer service-map)]
    (-> consumer
        (assoc ::start-fn (fn [s] (start s)))
        (assoc ::stop-fn  (fn [s] (stop s)))
        (cond->
         (test-service? service-map) (assoc ::test-msg-fn (fn [s d] (test-msg s d))
                                            ::check-committed (fn [s] (check-committed s))
                                            ::check-exception (fn [s] (check-exception s)))))))
(defn test-sum [a b]
  (+ a b))
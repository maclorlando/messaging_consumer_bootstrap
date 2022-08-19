(ns pub-sub-consumer.kafka.dispatcher
  "Interceptors for adapting the Kafka consumed offsets."
  (:require [cheshire.core :as json]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.interceptor.chain :as chain]
            [io.pedestal.log :as logger])
  (:import [org.apache.kafka.clients.consumer
            ConsumerRecord
            ConsumerRecords
            KafkaConsumer
            OffsetAndMetadata]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.errors WakeupException]))

(defn- kafka-message-map [^ConsumerRecord record]
  {:value (json/decode (.value record) true)
   :key (.key record)
   :topic (.topic record)
   :partition (.partition record)
   :commit-point (long (inc (.offset record)))})

(defn- commit-message-offset [consumer context]
  (let [{:keys [commit-point topic partition]} (:message context)]

    (try
      (.commitSync consumer (java.util.Collections/singletonMap
                             (TopicPartition. topic partition)
                             (OffsetAndMetadata. commit-point)))
      (catch WakeupException exception
        (assoc context ::chain/error exception)))))

(defn- enter-base-interceptor [context]
  (-> context
      (assoc :message (kafka-message-map (:record context)))))

(defn- error-base-interceptor [context exception]
  (logger/error :msg "error-base-interceptor triggered"
                :context (dissoc context :record :message))
  (assoc context ::chain/error exception))

(def base-interceptor
  "An interceptor which creates favorable pre-conditions for further
  io.pedestal.interceptors, and handles all post-conditions for
  processing an interceptor chain. It expects a context map
  with :record key.

  After entering this interceptor, the context will contain a new
  key :message, the value will be a map with ConsumerRecord fields.

  If a later interceptor in this context throws an exception which is
  not caught, this interceptor will log the error and stop the processing
  of the messages batch by setting the key :loop-continue? to false."
  (interceptor/interceptor {:name ::base-interceptor
                            :enter enter-base-interceptor
                            :leave nil
                            :error error-base-interceptor}))

(defn- interceptor-dispatcher-fn
  "Returns a function which can be used by a kafka consumer to dispatch
   kafka records. It executes the interceptors on an initial
   context map containing :consumer, :record and :continue?"
  [interceptors]
  (fn [client-id ^KafkaConsumer consumer ^ConsumerRecords records]
    (doseq [record (iterator-seq (.iterator records))]

      (try
        (let [initial-context {:client-id client-id
                               :record record}
              final-context (chain/execute initial-context interceptors)]

          (commit-message-offset consumer final-context))

        (catch Throwable t
          (logger/warn :dispatching-stopped client-id)

          (throw (ex-info "Messages dispatching stopped"
                          {:client-id client-id}
                          t)))))))

(defn kafka-interceptor-dispatcher-fn
  "Returns a function which can be used by a kafka consumer to dispatch
   kafka records. The [interceptors] are prepended to the sequence of interceptors."
  [interceptors]
  (interceptor-dispatcher-fn
   (concat [base-interceptor]
           interceptors)))

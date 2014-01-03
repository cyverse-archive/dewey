(ns dewey.amq
  "This library mananges the connection to the AMQP queue."
  (:use [slingshot.slingshot :only [try+]])
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [langohr.channel :as lch]
            [langohr.core :as rmq]
            [langohr.consumers :as lc]
            [langohr.exchange :as le]
            [langohr.queue :as lq]))


(defn- mk-handler
  [consume]
  (fn [_ {:keys [routing-key]} ^bytes payload]
    (try+
      (consume routing-key (json/parse-string (String. payload "UTF-8") true))
      (catch Object _
        (log/error (:throwable &throw-context) "MESSAGE HANDLING ERROR")))))


(defn attach-to-exchange
  "Attaches a consumer function to a given queue on a given AMQP exchange. It listens only for the
   provided topics. If no topics are provided, it listens for all topics. It is assumed that the
   messages in the queue are JSON documents.

   Parameters:
     host                 - the host of the AMQP broker
     port                 - the port the AMQP broker listends on
     user                 - the AMQP user
     password             - the AMQP user password
     exchange-name        - the name of the exchange
     exchange-durable     - a flag indicating whether or not the exchange preserves messages
     exchange-auto-delete - a flag indicating whether or not the exchange is deleted when all queues
                            have been dettached
     queue                - the name of the queue
     consumer             - the function that will receive the JSON document
     topics               - Optionally, a list of topics to listen for

   TODO handle errors"
  [host port user password exchange-name exchange-durable exchange-auto-delete queue consumer
   & topics]
  (let [conn (rmq/connect {:host host :port port :username user :password password})
        ch   (lch/open conn)]
    (le/topic ch exchange-name :durable exchange-durable :auto-delete exchange-auto-delete)
    (lq/declare ch queue)
    (if (empty? topics)
      (lq/bind ch queue exchange-name :routing-key "#")
      (doseq [topic topics]
        (lq/bind ch queue exchange-name :routing-key topic)))
    (lc/subscribe ch queue (mk-handler consumer) :auto-ack true)))

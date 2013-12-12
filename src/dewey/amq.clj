(ns dewey.amq
  "This library mananges the connection to the AMQP queue."
  (:require [cheshire.core :as json]
            [langohr.channel :as lch]
            [langohr.core :as rmq]
            [langohr.consumers :as lc]
            [langohr.exchange :as le]
            [langohr.queue :as lq]))


(defn- mk-handler
  [consume]
  (fn [_ {:keys [routing-key]} ^bytes payload]
    (consume routing-key (json/parse-string (String. payload "UTF-8") true))))


(defn attach-to-exchange
  "Attaches a consumer function to a given queue on a given AMQP exchange. It listens only for the
   provided topics. If no topics are provided, it listens for all topics. It is assumed that the
   messages in the queue are JSON documents.

   Parameters:
     host - the host of the AMQP broker
     port - the port the AMQP broker listends on
     user - the AMQP user
     password - the AMQP user password
     exchange - the name of the exchange
     queue - the name of the queue
     consumer - the function that will receive the JSON document
     topics - Optionally, a list of topics to listen for

   TODO handle errors"
  [host port user password exchange queue consumer & topics]
  (let [conn (rmq/connect {:host host :port port :username user :password password})
        ch   (lch/open conn)]
    (le/declare ch exchange "topic")
    (lq/declare ch queue)
    (if (empty? topics)
      (lq/bind ch queue exchange :routing-key "#")
      (doseq [topic topics]
        (lq/bind ch queue exchange :routing-key topic)))
    (lc/subscribe ch queue (mk-handler consumer) :auto-ack true)))

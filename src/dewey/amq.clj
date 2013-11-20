(ns dewey.amq
  (:require [cheshire.core :as json]
            [langohr.channel :as lch]
            [langohr.core :as rmq]
            [langohr.consumers :as lc]
            [langohr.exchange :as le]
            [langohr.queue :as lq]))

(def ^{:const true :private true} exchange "irods")

(defn attach-to-exchange
  [queue consume & topics]
  (let [conn    (rmq/connect)
        ch      (lch/open conn)
        handler (fn [_ {:keys [routing-key]} ^bytes payload]
                  (consume routing-key (json/parse-string (String. payload "UTF-8") true)))]
    (le/declare ch exchange "topic")
    (lq/declare ch queue)
    (if (empty? topics)
      (lq/bind ch queue exchange :routing-key "#")
      (doseq [topic topics]
        (lq/bind ch queue exchange :routing-key topic)))
    (lc/subscribe ch queue handler :auto-ack true)))

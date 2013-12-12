(ns dewey.core
  (:gen-class)
  (:require [clojurewerkz.elastisch.rest :as es]
            [clj-jargon.init :as r-init]
            [clj-jargon.lazy-listings :as r-sq]
            [dewey.amq :as amq]
            [dewey.indexing :as indexing])
  (:import [java.net URL]))


(def ^{:const true :private true} props {"dewey.amqp.exchange"          "irods"
                                         "dewey.amqp.host"              "localhost"
                                         "dewey.amqp.password"          "guest"
                                         "dewey.amqp.port"              5672
                                         "dewey.amqp.user"              "guest"
                                         "dewey.es.host"                "localhost"
                                         "dewey.es.port"                9200
                                         "dewey.irods.default-resource" "demoResc"
                                         "dewey.irods.home"             "/iplant/home/rods"
                                         "dewey.irods.host"             "localhost"
                                         "dewey.irods.password"         "rods"
                                         "dewey.irods.port"             1247
                                         "dewey.irods.user"             "rods"
                                         "dewey.irods.zone"             "iplant"})


(defn- init-es
  [props]
  (let [url (URL. "http" (get props "dewey.es.host") (get props "dewey.es.port") "")]
    (es/connect! (str url))))


(defn- init-irods
  [props]
  (let [cfg (r-init/init (get props "dewey.irods.host")
                         (str (get props "dewey.irods.port"))
                         (get props "dewey.irods.user")
                         (get props "dewey.irods.password")
                         (get props "dewey.irods.home")
                         (get props "dewey.irods.zone")
                         (get props "dewey.irods.default-resource"))]
    (r-init/with-jargon cfg [irods]
      (r-sq/define-specific-queries irods))
    cfg))


(defn -main
  [& args]
  (let [irods-cfg (init-irods props)]
    (init-es props)
    (amq/attach-to-exchange (get props "dewey.amqp.host")
                            (get props "dewey.amqp.port")
                            (get props "dewey.amqp.user")
                            (get props "dewey.amqp.password")
                            (get props "dewey.amqp.exchange")
                            "indexing"
                            (partial indexing/consume-msg irods-cfg)
                            "data-object.#"
                            "collection.#")))

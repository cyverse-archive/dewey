(ns dewey.core
  (:gen-class)
  (:require [clojurewerkz.elastisch.rest :as es]
            [clj-jargon.init :as r-init]
            [clj-jargon.lazy-listings :as r-sq]
            [dewey.amq :as amq]
            [dewey.indexing :as indexing]))

(def ^{:const true :private true} exchange "irods")
(def ^{:const true :private true} queue "indexing")


(defn- init-es
  []
  (es/connect! "http://localhost:9200"))


(defn- init-irods
  []
  (let [cfg (r-init/init "localhost" "1247" "rods" "rods" "/iplant/home/rods" "iplant" "demoResc")]
    (r-init/with-jargon cfg [irods]
      (r-sq/define-specific-queries irods))
    cfg))


(defn -main
  [& args]
  (let [irods-cfg (init-irods)]
    (init-es)
    (amq/attach-to-exchange exchange
                            queue
                            (partial indexing/consume-msg irods-cfg)
                            "data-object.#"
                            "collection.#")))

(defproject dewey "0.1-SNAPSHOT"
  :description "This is a RabbitMQ client responsible for keeping an elasticsearch index
                synchronized with an iRODS repository using messages produced by iRODS."
  :license {:url "file://LICENSE.txt"}
  :aot [dewey.core]
  :main dewey.core
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.4"]
                 [org.clojure/tools.logging "0.2.6"]
                 [cheshire "5.2.0"]
                 [clj-time "0.6.0"]
                 [clojurewerkz/elastisch "1.2.0"]
                 [com.novemberain/langohr "1.6.0"]
                 [slingshot "0.10.3"]
                 [org.iplantc/clj-jargon "0.3.1"]
                 [org.iplantc/clojure-commons "1.4.7"]]
  :resource-paths []
  :profiles {:dev {:dependencies   [[midje "1.6.0"]]
                   :resource-paths ["dev-resource"]}}
  :plugins [[org.iplantc/lein-iplant-rpm "1.4.3-SNAPSHOT"]]
  :iplant-rpm {:summary      "dewey"
               :dependencies ["iplant-services-config >= 0.1.0-5"]
               :config-files ["log4j.properties"]
               :config-path  "resources"}
  :repositories {"iplantCollaborative"
                 "http://projects.iplantcollaborative.org/archiva/repository/internal/"})

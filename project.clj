(defproject org.iplantc/dewey "0.2"
  :description "This is a RabbitMQ client responsible for keeping an elasticsearch index
                synchronized with an iRODS repository using messages produced by iRODS."
  :url "http://www.iplantcollaborative.org"
  :license {:name "BSD"
            :url "http://iplantcollaborative.org/sites/default/files/iPLANT-LICENSE.txt"}
  :scm {:connection "scm:git:git@github.com:iPlantCollaborativeOpenSource/dewey.git"
        :developerConnection "scm:git:git@github.com:iPlantCollaborativeOpenSource/dewey.git"
        :url "git@github.com:iPlantCollaborativeOpenSource/dewey.git"}
  :pom-addition [:developers
                 [:developer
                  [:url "https://github.com/orgs/iPlantCollaborativeOpenSource/teams/iplant-devs"]]]
  :aot [dewey.core]
  :main dewey.core
  :classifiers [["javadoc" :javadoc]
                ["sources" :sources]]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.4"]
                 [org.clojure/tools.logging "0.2.6"]
                 [cheshire "5.2.0"]
                 [clojurewerkz/elastisch "1.3.0"]
                 [com.novemberain/langohr "2.2.1"]
                 [liberator "0.10.0"]
                 [compojure "1.1.6"]
                 [ring/ring-core "1.2.1"]
                 [ring/ring-jetty-adapter "1.1.0"]
                 [slingshot "0.10.3"]
                 [org.iplantc/clj-jargon "0.4.2"]
                 [org.iplantc/clojure-commons "1.4.8"]]
  :resource-paths []
  :profiles {:dev {:dependencies   [[midje "1.6.0"]]
                   :resource-paths ["dev-resource"]}}
  :plugins [[org.iplantc/lein-iplant-rpm "1.4.3"]]
  :iplant-rpm {:summary      "dewey"
               :dependencies ["iplant-service-config >= 0.1.0-5" "iplant-clavin"]
               :config-files ["log4j.properties"]
               :config-path  "resources"}
  :repositories [["sonatype-nexus-snapshots"
                  {:url "https://oss.sonatype.org/content/repositories/snapshots"}]]
  :deploy-repositories [["sonatype-nexus-staging"
                         {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2/"}]])

(ns dewey.doc-prep
  "This is the logic that formats the elasticsearch index entry or entry update."
  (:require [clj-time.coerce :as t-conv]
            [clj-time.format :as t-fmt]
            [clj-jargon.permissions :as r-perm])
  (:import [java.util Date]
           [org.irods.jargon.core.query CollectionAndDataObjectListingEntry]))


(def ^{:const true} index "data")


(defn format-user
  "Formats a user identity with an elasticsearch index. The result will be of the form 'name#zone'.
   It has two form the first takes a user name and zone has separate parameters. The second takes a
   map with :name and :zone keys having the corresponding values."
  ([user] (format-user (:name user) (:zone user)))
  ([name zone] (str name \# zone)))


(defn- format-acl-entry
  [acl-entry]
  (letfn [(fmt-perm [perm] (condp = perm
                             r-perm/own-perm   :own
                             r-perm/write-perm :write
                             r-perm/read-perm  :read
                             nil))]
    {:permission (fmt-perm (.getFilePermissionEnum acl-entry))
     :user       (format-user (.getUserName acl-entry) (.getUserZone acl-entry))}))


(defn format-acl
  "Formats an ACL entry for indexing.

   Parameters:
     acl - This is a list of UserFilePermission objects.

   Returns:
   The result is an list of maps. Each map indicates a user permission and has the following form.

   {:permission :own|:read|:write
    :user       name#zone}"
  [acl]
  (remove (comp nil? :permission) (map format-acl-entry acl)))


(defmulti ^{:doc "Formats the index id an entry"} format-id type)

(defmethod format-id String
  [path]
  path)

(defmethod format-id CollectionAndDataObjectListingEntry
  [entry]
  (.getFormattedAbsolutePath entry))


(defmulti
  ^{:doc "Formats a time for indexing. The resulting form will be DATE-HOUR-MINUTE-SECOND-MS."}
  format-time type)

(defmethod format-time String
  [posix-time-ms]
  (->> posix-time-ms
    Long/parseLong
    t-conv/from-long
    (t-fmt/unparse (t-fmt/formatters :date-hour-minute-second-ms))))

(defmethod format-time Date
  [time]
  (t-fmt/unparse (t-fmt/formatters :date-hour-minute-second-ms) (t-conv/from-date time)))


(defn get-mapping-type
  "Looks up the mapping type for the entity type. The entity types are :collection and
   :data-object."
  [entity-type]
  (case entity-type
    :collection  "folder"
    :data-object "file"))

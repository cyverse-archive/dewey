(ns dewey.doc-prep
  "This is the logic that formats the elasticsearch index entry or entry update."
  (:require [clj-time.coerce :as t-conv]
            [clj-time.format :as t-fmt]
            [clj-jargon.permissions :as irods]
            [clojure-commons.file-utils :as file])
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
                             irods/own-perm   :own
                             irods/write-perm :write
                             irods/read-perm  :read
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


(defmulti
  ^{:doc "Formats the index id an entry"}
  format-id type)

(defmethod format-id String
  [path]
  path)

(defmethod format-id CollectionAndDataObjectListingEntry
  [entry]
  (.getFormattedAbsolutePath entry))


(defn- format-avu
  [avu]
  {:attribute (:attr avu)
   :value     (:value avu)
   :unit      (:unit avu)})


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


(defn get-acl
  "Retrives the ACL for an iRODS entity. The entity will be retrieved from the repository if needed.

   Returns:
     The result is an list of maps. Each map indicates a user permission and has the following form.

     {:permission :own|:read|:write
     :user       name#zone}"
  [irods type entity]
  (format-acl (.acl irods type entity)))


(defn- get-creator
  [irods type entity]
  (format-user (.creator irods type entity)))


(defn get-data-object-size
  "Retrieves the size of a data object in bytes. It will retrieve this information from the
   repository if needed."
  [irods path]
  (.data-object-size irods path))


(defn get-data-object-type
  "Retrieves the file type of a data object from the repository."
  [irods obj]
  (.data-object-type irods obj))


(defn- get-date-created
  [irods entity]
  (format-time (.date-created irods entity)))


(defn get-date-modified
  "Retrieves the time when a entity was last modified. It will retrieve this information from the
   repository if needed. The time returned will have the format DATE-HOUR-MINUTE-SECOND-MS."
  [irods path]
  (format-time (.date-modified irods path)))


(defn get-metadata
  "Retrieves the AVU metadata associated with the provided entity."
  [irods entry]
  (map format-avu (.metadata irods entry)))


(defn format-collection-doc
  "Formats an index entry for a collection."
  [irods coll & {:keys [permissions creator date-created date-modified metadata]}]
  (let [id (format-id coll)]
    {:id              id
     :label           (file/basename id)
     :userPermissions (or permissions (get-acl irods :collection coll))
     :creator         (or creator (get-creator irods :collection coll))
     :dateCreated     (or date-created (get-date-created irods coll))
     :dateModified    (or date-modified (get-date-modified irods coll))
     :metadata        (or metadata (get-metadata irods coll))}))


(defn format-data-object-doc
  "Formats an index entry for a data object"
  [irods obj
   & {:keys [permissions creator date-created date-modified metadata file-size file-type]}]
  (let [id (format-id obj)]
    {:id              id
     :label           (file/basename id)
     :userPermissions (or permissions (get-acl irods :data-object obj))
     :creator         (or creator (get-creator irods :data-object obj))
     :dateCreated     (or date-created (get-date-created irods obj))
     :dateModified    (or date-modified (get-date-modified irods obj))
     :metadata        (or metadata (get-metadata irods obj))
     :fileSize        (or file-size (get-data-object-size irods obj))
     :fileType        (or file-type (get-data-object-type irods obj))}))

(ns dewey.doc-prep
  "This is the logic that formats the elasticsearch index entry or entry update."
  (:require [clj-time.coerce :as t-conv]
            [clj-time.format :as t-fmt]
            [clj-jargon.item-info :as r-info]
            [clj-jargon.metadata :as r-meta]
            [clj-jargon.permissions :as r-perm]
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


(defn- format-metadata
  [metadata]
  {:attribute (:attr metadata)
   :value     (:value metadata)
   :unit      (:unit metadata)})


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


(derive ::collection  ::entity)
(derive ::data-object ::entity)


(defmulti get-acl #(vector %2 (type %3)))

(defmethod get-acl [::collection String]
  [irods _ path]
  (format-acl (.listPermissionsForCollection (:collectionAO irods) path)))

(defmethod get-acl [::data-object String]
  [irods _ path]
  (format-acl (.listPermissionsForDataObject (:dataObjectAO irods) path)))

(defmethod get-acl [::entity CollectionAndDataObjectListingEntry]
  [irods _ entry]
  (format-acl (.getUserFilePermission entry)))


(defmulti ^{:private true} get-creator #(vector %2 (type %3)))

(defmethod get-creator [::collection String]
  [irods _ path]
  (let [coll (r-info/collection irods path)]
    (format-user (.getCollectionOwnerName coll) (.getCollectionOwnerZone coll))))

(defmethod get-creator [::data-object String]
  [irods _ path]
  (let [obj (r-info/data-object irods path)]
    (format-user (.getDataOwnerName obj) (.getDataOwnerZone obj))))

(defmethod get-creator [::entity CollectionAndDataObjectListingEntry]
  [irods _ entry]
  (format-user (.getOwnerName entry) (.getOwnerZone entry)))


(defmulti get-data-object-size #(type %2))

(defmethod get-data-object-size String
  [irods path]
  (r-info/file-size irods path))

(defmethod get-data-object-size CollectionAndDataObjectListingEntry
  [irods obj]
  (.getDataSize obj))


(defn get-data-object-type
  [irods obj]
  (.getDataTypeName (r-info/data-object irods (format-id obj))))


(defmulti ^{:private true} get-date-created #(type %2))

(defmethod get-date-created String
  [irods path]
  (format-time (r-info/created-date irods path)))

(defmethod get-date-created CollectionAndDataObjectListingEntry
  [irods entry]
  (format-time (.getCreatedAt entry)))


(defmulti get-date-modified #(type %2))

(defmethod get-date-modified String
  [irods path]
  (format-time (r-info/lastmod-date irods path)))

(defmethod get-date-modified CollectionAndDataObjectListingEntry
  [irods entry]
  (format-time (.getModifiedAt entry)))


(defn get-metadata
  [irods entry]
  (map format-metadata (r-meta/get-metadata irods (format-id entry))))


(defn format-collection-doc
  [irods coll & {:keys [permissions creator date-created date-modified metadata]}]
  (let [id (format-id coll)]
    {:id              id
     :label           (file/basename id)
     :userPermissions (or permissions (get-acl irods ::collection coll))
     :creator         (or creator (get-creator irods ::collection coll))
     :dateCreated     (or date-created (get-date-created irods coll))
     :dateModified    (or date-modified (get-date-modified irods coll))
     :metadata        (or metadata (get-metadata irods coll))}))


(defn format-data-object-doc
  [irods obj
   & {:keys [permissions creator date-created date-modified metadata file-size file-type]}]
  (let [id (format-id obj)]
    {:id              id
     :label           (file/basename id)
     :userPermissions (or permissions (get-acl irods ::data-object obj))
     :creator         (or creator (get-creator irods ::data-object obj))
     :dateCreated     (or date-created (get-date-created irods obj))
     :dateModified    (or date-modified (get-date-modified irods obj))
     :metadata        (or metadata (get-metadata irods obj))
     :fileSize        (or file-size (get-data-object-size irods obj))
     :fileType        (or file-type (get-data-object-type irods obj))}))

(ns dewey.indexing
  (:require [clj-time.coerce :as t-conv]
            [clj-time.format :as t-fmt]
            [clojurewerkz.elastisch.query :as es-query]
            [clojurewerkz.elastisch.rest.document :as es-doc]
            [clj-jargon.init :as r-init]
            [clj-jargon.item-info :as r-info]
            [clj-jargon.lazy-listings :as r-lazy]
            [clj-jargon.metadata :as r-meta]
            [clj-jargon.permissions :as r-perm]
            [clojure-commons.file-utils :as file])
  (:import [java.util Date]
           [org.irods.jargon.core.query CollectionAndDataObjectListingEntry]))


(def ^{:private true :const true} index "data")


(defn- get-mapping-type
  [entity-type]
  (case entity-type
    ::collection  "folder"
    ::data-object "file"))


(defn- format-user
  ([user] (format-user (:name user) (:zone user)))
  ([name zone] {:username name :zone zone}))


(defn- format-acl-entry
  [acl-entry]
  (letfn [(fmt-perm [perm] (condp = perm
                             r-perm/own-perm   :own
                             r-perm/write-perm :write
                             r-perm/read-perm  :read
                             nil))]
    {:permission (fmt-perm (.getFilePermissionEnum acl-entry))
     :user       (format-user (.getUserName acl-entry) (.getUserZone acl-entry))}))


(defn- format-acl
  [acl]
  (remove (comp nil? :permission) (map format-acl-entry acl)))


(defmulti format-time type)

(defmethod format-time String
  [posix-time-ms]
  (->> posix-time-ms
    Long/parseLong
    t-conv/from-long
    (t-fmt/unparse (t-fmt/formatters :date-hour-minute-second-ms))))

(defmethod format-time Date
  [time]
  (t-fmt/unparse (t-fmt/formatters :date-hour-minute-second-ms)
                 (t-conv/from-date time)))


(defmulti ^{:private true} get-id type)

(defmethod get-id String
  [path]
  path)

(defmethod get-id CollectionAndDataObjectListingEntry
  [entry]
  (.getFormattedAbsolutePath entry))


(derive ::collection  ::entity)
(derive ::data-object ::entity)


(defmulti ^{:private true} get-acl #(vector %2 (type %3)))

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


(defmulti ^{:private true} get-data-object-size #(type %2))

(defmethod get-data-object-size String
  [irods path]
  (r-info/file-size irods path))

(defmethod get-data-object-size CollectionAndDataObjectListingEntry
  [irods obj]
  (.getDataSize obj))


(defn- get-data-object-type
  [irods obj]
  (.getDataTypeName (r-info/data-object irods (get-id obj))))


(defmulti ^{:private true} get-date-created #(type %2))

(defmethod get-date-created String
  [irods path]
  (format-time (r-info/created-date irods path)))

(defmethod get-date-created CollectionAndDataObjectListingEntry
  [irods entry]
  (format-time (.getCreatedAt entry)))


(defmulti ^{:private true} get-date-modified #(type %2))

(defmethod get-date-modified String
  [irods path]
  (format-time (r-info/lastmod-date irods path)))

(defmethod get-date-modified CollectionAndDataObjectListingEntry
  [irods entry]
  (format-time (.getModifiedAt entry)))


(defn- get-metadata
  [irods entry]
  (letfn [(fmt-metadata [metadata] {:attribute (:attr metadata)
                                    :value     (:value metadata)
                                    :unit      (:unit metadata)})]
    (map fmt-metadata (r-meta/get-metadata irods (get-id entry)))))


(defn- format-collection-doc
  [irods coll & {:keys [permissions creator date-created date-modified metadata]}]
  (let [id (get-id coll)]
    {:id              id
     :label           (file/basename id)
     :userPermissions (or permissions (get-acl irods ::collection coll))
     :creator         (or creator (get-creator irods ::collection coll))
     :dateCreated     (or date-created (get-date-created irods coll))
     :dateModified    (or date-modified (get-date-modified irods coll))
     :metadata        (or metadata (get-metadata irods coll))}))


(defn- format-data-object-doc
  [irods obj
   & {:keys [permissions creator date-created date-modified metadata file-size file-type]}]
  (let [id (get-id obj)]
    {:id              id
     :label           (file/basename id)
     :userPermissions (or permissions (get-acl irods ::data-object obj))
     :creator         (or creator (get-creator irods ::data-object obj))
     :dateCreated     (or date-created (get-date-created irods obj))
     :dateModified    (or date-modified (get-date-modified irods obj))
     :metadata        (or metadata (get-metadata irods obj))
     :fileSize        (or file-size (get-data-object-size irods obj))
     :fileType        (or file-type (get-data-object-type irods obj))}))


(defn- get-parent-path
  [path]
  (file/rm-last-slash (file/dirname path)))


(defn- index-entry
  [entity-type entry]
  (es-doc/create index (get-mapping-type entity-type) entry :id (:id entry)))


(defn- remove-entry
  [entity-type id]
  (let [mapping-type (get-mapping-type entity-type)]
    (when (es-doc/present? index mapping-type id)
      (es-doc/delete index mapping-type id))))


(defn- update-parent-modify-time
  [irods entity-path]
  (let [parent-path  (get-parent-path entity-path)
        mapping-type (get-mapping-type ::collection)
        parent-id    (get-id parent-path)]
    (when (r-info/exists? irods parent-path)
      (if (es-doc/present? index mapping-type parent-id)
        (es-doc/update-with-script index
                                   mapping-type
                                   parent-id
                                   "ctx._source.dateModified = dateModified;"
                                   {:dateModified (get-date-modified irods parent-path)})
        (index-entry mapping-type (format-collection-doc irods parent-path))))))


(defn- rename-entry
  [irods entity-type old-path new-doc]
  (remove-entry entity-type old-path)
  (index-entry entity-type new-doc)
  (update-parent-modify-time irods old-path)
  (when-not (= (get-parent-path old-path) (get-parent-path (:id new-doc)))
    (update-parent-modify-time irods (:id new-doc))))


(defn- crawl-collection
  [irods coll-path coll-op obj-op]
  (letfn [(rec-coll-op [coll]
            (coll-op irods coll)
            (crawl-collection irods (.getFormattedAbsolutePath coll) coll-op obj-op))]
    (doall (map (partial obj-op irods) (r-lazy/list-files-in irods coll-path)))
    (doall (map rec-coll-op (r-lazy/list-subdirs-in irods coll-path)))))


(defn- update-acl
  [irods entity-type doc-formatter entry]
  (let [mapping-type (get-mapping-type entity-type)
        id           (get-id entry)]
    (if (es-doc/present? index mapping-type id)
      (es-doc/update-with-script index
                                 mapping-type
                                 id
                                 "ctx._source.userPermissions = permissions"
                                 {:permissions (get-acl irods entity-type entry)})
      (index-entry entity-type (doc-formatter irods entry)))))


(defn- update-collection-acl
  [irods coll]
  (update-acl irods ::collection format-collection-doc coll))


(defn- update-data-object-acl
  [irods obj]
  (update-acl irods ::data-object format-data-object-doc obj))


(defn- index-collection-handler
  [irods msg]
  (index-entry ::collection (format-collection-doc irods (:entity msg)))
  (update-parent-modify-time irods (:entity msg)))


(defn- index-data-object-handler
  [irods msg]
  (let [doc (format-data-object-doc irods (:entity msg)
              :creator   (format-user (:creator msg))
              :file-size (:size msg)
              :file-type (:type msg))]
    (index-entry ::data-object doc)
    (update-parent-modify-time irods (:entity msg))))


(defn- reindex-collection-metadata-hander
  [irods msg]
  (let [path         (:entity msg)
        mapping-type (get-mapping-type ::collection)
        id           (get-id path)]
    (if (es-doc/present? index mapping-type id)
      (es-doc/update-with-script index
                                 mapping-type
                                 id
                                 "ctx._source.metadata = metadata"
                                 {:metadata (get-metadata irods path)})
      (index-entry ::collection (format-collection-doc irods path)))))


(defn- reindex-data-object-handler
  [irods msg]
  (let [path         (:entity msg)
        mapping-type (get-mapping-type ::data-object)
        id           (get-id path)]
    (if (es-doc/present? index mapping-type id)
      (es-doc/update-with-script index
                                 mapping-type
                                 id
                                 "ctx._source.dateModified = dateModified;
                                  ctx._source.fileSize = fileSize;"
                                 {:dateModified (get-date-modified irods path)
                                  :fileSize     (:size msg)})
      (index-entry ::data-object
                   (format-data-object-doc irods path
                     :file-size (:size msg)
                     :file-type (:type msg))))))


(defn- rename-collection-handler
  [irods msg]
  (let [old-path (:entity msg)
        new-path (:new-path msg)]
    (rename-entry irods ::collection old-path (format-collection-doc irods new-path))
    (es-doc/delete-by-query-across-all-types index (es-query/wildcard :id (str old-path "/*")))
    (crawl-collection irods
                      new-path
                      #(index-entry ::collection (format-collection-doc %1 %2))
                      #(index-entry ::data-object (format-data-object-doc %1 %2)))))


(defn- rename-data-object-handler
  [irods msg]
  (rename-entry irods ::data-object (:entity msg) (format-data-object-doc irods (:new-path msg))))


(defn- rm-collection-handler
  [irods msg]
  (remove-entry ::collection (:entity msg))
  (update-parent-modify-time irods (:entity msg)))


(defn- rm-data-object-handler
  [irods msg]
  (remove-entry ::data-object (:entity msg))
  (update-parent-modify-time irods (:entity msg)))


(defn- update-collection-acl-handler
  [irods msg]
  (when (contains? msg :permission)
    (update-collection-acl irods (:entity msg))
    (when (:recursive msg)
      (crawl-collection irods (:entity msg) update-collection-acl update-data-object-acl))))


(defn- update-data-object-acl-handler
  [irods msg]
  (update-data-object-acl irods (:entity msg)))


(defn- update-data-object-sys-meta-handler
  [irods msg]
  (let [path         (:entity msg)
        mapping-type (get-mapping-type ::data-object)
        id           (get-id path)]
    (if (es-doc/present? index mapping-type id)
      (es-doc/update-with-script index
                                 mapping-type
                                 id
                                 "ctx._source.dateModified = dateModified;
                                  ctx._source.fileSize = fileSize;
                                  ctx._source.fileType = fileType;"
                                 {:dateModified (get-date-modified irods path)
                                  :fileSize     (get-data-object-size irods path)
                                  :fileType     (get-data-object-type irods path)})
      (index-entry ::data-object (format-data-object-doc irods path)))))


(defn- resolve-consumer
  [routing-key]
  (case routing-key
    "collection.acl.mod"           update-collection-acl-handler
    "collection.add"               index-collection-handler
    "collection.metadata.add"      reindex-collection-metadata-hander
    "collection.metadata.adda"     reindex-collection-metadata-hander
    "collection.metadata.cp"       nil
    "collection.metadata.mod"      nil
    "collection.metadata.rm"       nil
    "collection.metadata.rmw"      nil
    "collection.metadata.set"      nil
    "collection.mv"                rename-collection-handler
    "collection.rm"                rm-collection-handler
    "data-object.acl.mod"          update-data-object-acl-handler
    "data-object.add"              index-data-object-handler
    "data-object.cp"               index-data-object-handler
    "data-object.metadata.add"     nil
    "data-object.metadata.adda"    nil
    "data-object.metadata.addw"    nil
    "data-object.metadata.cp"      nil
    "data-object.metadata.mod"     nil
    "data-object.metadata.rm"      nil
    "data-object.metadata.rmw"     nil
    "data-object.metadata.set"     nil
    "data-object.mod"              reindex-data-object-handler
    "data-object.mv"               rename-data-object-handler
    "data-object.rm"               rm-data-object-handler
    "data-object.sys-metadata.mod" update-data-object-sys-meta-handler
    "zone.mv"                      nil
                                   nil))


(defn consume-msg
  [irods-cfg routing-key msg]
  (try
    (when-let [consume (resolve-consumer routing-key)]
      (r-init/with-jargon irods-cfg [irods]
        (consume irods msg)))
    (catch Throwable e
      (.printStackTrace e)
      (System/exit -1))))

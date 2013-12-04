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
            [clojure-commons.file-utils :as file]))

(def ^{:private true :const true} index "data")
(def ^{:private true :const true} collection "folder")
(def ^{:private true :const true} data-object "file")

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

(defn- format-time
  [posix-time]
  (t-fmt/unparse (t-fmt/formatters :date-hour-minute-second-ms)
                 (t-conv/from-date posix-time)))

(defn- format-time-str
  [posix-time-str]
  (->> posix-time-str
    Long/parseLong
    t-conv/from-long
    (t-fmt/unparse (t-fmt/formatters :date-hour-minute-second-ms))))

(defn- get-collection-acl
  [irods path]
  (format-acl (.listPermissionsForCollection (:collectionAO irods) path)))

(defn- get-data-object-acl
  [irods path]
  (format-acl (.listPermissionsForDataObject (:dataObjectAO irods) path)))

(defn- get-collection-creator
  [irods path]
  (let [coll (r-info/collection irods path)]
    (format-user (.getCollectionOwnerName coll) (.getCollectionOwnerZone coll))))

(defn- get-data-object-creator
  [irods path]
  (let [obj (r-info/data-object irods path)]
    (format-user (.getDataOwnerName obj) (.getDataOwnerZone obj))))

(defn- get-data-object-type
  [irods path]
  (.getDataTypeName (r-info/data-object irods path)))

(defn- get-date-created
  [irods entity-path]
  (format-time-str (r-info/created-date irods entity-path)))

(defn- get-date-modified
  [irods entity-path]
  (format-time-str (r-info/lastmod-date irods entity-path)))

(defn- get-metadata
  [irods entity-path]
  (letfn [(fmt-metadata [metadata] {:attribute (:attr metadata)
                                    :value     (:value metadata)
                                    :unit      (:unit metadata)})]
    (map fmt-metadata (r-meta/get-metadata irods entity-path))))

(defn- format-collection-doc
  [irods path & {:keys [permissions creator date-created date-modified metadata]}]
  {:id              path
   :label           (file/basename path)
   :userPermissions (or permissions (get-collection-acl irods path))
   :creator         (or creator (get-collection-creator irods path))
   :dateCreated     (or date-created (get-date-created irods path))
   :dateModified    (or date-modified (get-date-modified irods path))
   :metadata        (or metadata (get-metadata irods path))})

(defn- format-data-object-doc
  [irods path
   & {:keys [permissions creator date-created date-modified metadata file-size file-type]}]
  {:id              path
   :label           (file/basename path)
   :userPermissions (or permissions (get-data-object-acl irods path))
   :creator         (or creator (get-data-object-creator irods path))
   :dateCreated     (or date-created (get-date-created irods path))
   :dateModified    (or date-modified (get-date-modified irods path))
   :metadata        (or metadata (get-metadata irods path))
   :fileSize        (or file-size (r-info/file-size irods path))
   :fileType        (or file-type (get-data-object-type irods path))})

(defn- format-coll-doc-from-irods-entry
  [irods entry]
  (format-collection-doc irods (.getFormattedAbsolutePath entry)
    :permissions  (format-acl (.getUserFilePermission entry))
    :creator      (format-user (.getOwnerName entry) (.getOwnerZone entry))
    :dateCreated  (format-time (.getCreatedAt entry))
    :dateModified (format-time (.getModifiedAt entry))))

(defn- format-data-obj-doc-from-irods-entry
  [irods entry]
  (format-data-object-doc irods (.getFormattedAbsolutePath entry)
    :permissions   (format-acl (.getUserFilePermission entry))
    :creator       (format-user (.getOwnerName entry) (.getOwnerZone entry))
    :date-created  (format-time (.getCreatedAt entry))
    :date-modified (format-time (.getModifiedAt entry))
    :file-size     (.getDataSize entry)))

(defn- get-parent-id
  [entity-id]
  (file/rm-last-slash (file/dirname entity-id)))

(defn- index-entry
  [mapping-type entry]
  (es-doc/create index mapping-type entry :id (:id entry)))

(defn- remove-entry
  [mapping-type id]
  (when (es-doc/present? index mapping-type id)
    (es-doc/delete index mapping-type id)))

(defn- update-parent-modify-time
  [irods entity-path]
  (let [parent-id (get-parent-id entity-path)]
    (if (es-doc/present? index collection parent-id)
      (es-doc/update-with-script index
                                 collection
                                 parent-id
                                 "ctx._source.dateModified = dateModified;"
                                 {:dateModified (get-date-modified irods parent-id)})
      (index-entry collection (format-collection-doc irods parent-id)))))

(defn- rename-entry
  [irods mapping-type old-path new-doc]
  (remove-entry mapping-type old-path)
  (index-entry mapping-type new-doc)
  (update-parent-modify-time irods old-path)
  (when-not (= (get-parent-id old-path) (get-parent-id (:id new-doc)))
    (update-parent-modify-time irods (:id new-doc))))

(defn- crawl-collection
  [irods coll-path coll-op obj-op]
  (letfn [(rec-coll-op [coll]
            (coll-op irods coll)
            (crawl-collection irods (.getFormattedAbsolutePath coll) coll-op obj-op))]
    (doall (map (partial obj-op irods) (r-lazy/list-files-in irods coll-path)))
    (doall (map rec-coll-op (r-lazy/list-subdirs-in irods coll-path)))))

(defn- update-acl
  [irods mapping-type acl-retriever formatter entry-id]
  (if (es-doc/present? index mapping-type entry-id)
    (es-doc/update-with-script index
                               mapping-type
                               entry-id
                               "ctx._source.userPermissions = permissions"
                               {:permissions (acl-retriever irods entry-id)})
    (index-entry mapping-type (formatter irods entry-id))))

(defn- update-collection-acl
  "Updates a collection's indexed ACL. This version retrieves the ACL from iRODS.

   Parameters:
     irods - The iRODS context map
     coll-id - The id of the collection in the index."
  [irods coll-id]
  (update-acl irods collection get-collection-acl format-collection-doc coll-id))

(defn- update-collection-acl-irods
  [irods coll]
  (let [id (.getFormattedAbsolutePath coll)]
  (if (es-doc/present? index collection id)
    (es-doc/update-with-script index
                               collection
                               id
                               "ctx._source.userPermissions = permissions"
                               {:permissions (format-acl (.getUserFilePermission coll))})
    (index-entry collection (format-coll-doc-from-irods-entry irods coll)))))

  (defn- update-data-obj-acl
  "Updates a data object's indexed ACL. This version retrieves the ACL from iRODS.

   Parameters:
     irods - The iRODS context map
     obj-id - The id of the data object in the index."
  [irods obj-id]
  (update-acl irods data-object get-data-object-acl format-data-object-doc obj-id))

(defn- update-data-object-acl-irods
  [irods obj]
  (let [id (.getFormattedAbsolutePath obj)]
    (if (es-doc/present? index data-object id)
      (es-doc/update-with-script index
                                 data-object
                                 id
                                 "ctx._source.userPermissions = permissions"
                                 {:permissions (format-acl (.getUserFilePermission obj))})
      (index-entry data-object (format-data-obj-doc-from-irods-entry irods obj)))))

(defn- index-collection-handler
  [irods msg]
  (index-entry collection (format-collection-doc irods (:entity msg)))
  (update-parent-modify-time irods (:entity msg)))

(defn- index-data-object-handler
  [irods msg]
  (let [doc (format-data-object-doc irods (:entity msg)
              :creator   (format-user (:creator msg))
              :file-size (:size msg)
              :file-type (:type msg))]
    (index-entry data-object doc)
    (update-parent-modify-time irods (:entity msg))))

(defn- reindex-data-object-handler
  [irods msg]
  (let [path (:entity msg)]
    (if (es-doc/present? index data-object path)
      (es-doc/update-with-script index
                                 data-object
                                 path
                                 "ctx._source.dateModified = dateModified;
                                  ctx._source.fileSize = fileSize;"
                                 {:dateModified (get-date-modified irods path)
                                  :fileSize     (:size msg)})
      (index-entry data-object
                   (format-data-object-doc irods path
                     :file-size (:size msg)
                     :file-type (:type msg))))))

(defn- rename-collection-handler
  [irods msg]
  (let [old-path (:entity msg)
        new-path (:new-path msg)]
    (rename-entry irods collection old-path (format-collection-doc irods new-path))
    (es-doc/delete-by-query-across-all-types index (es-query/wildcard :id (str old-path "/*")))
    (crawl-collection irods
                      new-path
                      (partial index-entry collection format-coll-doc-from-irods-entry)
                      (partial index-entry data-object format-data-obj-doc-from-irods-entry))))

(defn- rename-data-object-handler
  [irods msg]
  (rename-entry irods data-object (:entity msg) (format-data-object-doc irods (:new-path msg))))

(defn- rm-collection-handler
  [irods msg]
  (remove-entry collection (:entity msg))
  (update-parent-modify-time irods (:entity msg)))

(defn- rm-data-object-handler
  [irods msg]
  (remove-entry data-object (:entity msg))
  (update-parent-modify-time irods (:entity msg)))

(defn- update-collection-acl-handler
  [irods msg]
  (when (contains? msg :permission)
    (update-collection-acl irods (:entity msg))
    (when (:recursive msg)
      (crawl-collection irods
                        (:entity msg)
                        update-collection-acl-irods
                        update-data-object-acl-irods))))

(defn- update-data-object-acl-handler
  [irods msg]
  (update-data-obj-acl irods (:entity msg)))

(defn- resolve-consumer
  [routing-key]
  (case routing-key
    "collection.acl.mod"           update-collection-acl-handler
    "collection.add"               index-collection-handler
    "collection.metadata.add"      nil
    "collection.metadata.adda"     nil
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
    "data-object.sys-metadata.mod" nil
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

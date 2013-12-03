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

(defn- index-members
  [irods parent-path]
  (letfn [(idx-obj  [obj]
                    (index-entry data-object (format-data-obj-doc-from-irods-entry irods obj)))
          (idx-coll [coll]
                    (index-entry collection (format-coll-doc-from-irods-entry irods coll))
                    (index-members irods (.getFormattedAbsolutePath coll)))]
    (doall (map idx-obj (r-lazy/list-files-in irods parent-path)))
    (doall (map idx-coll (r-lazy/list-subdirs-in irods parent-path)))))

(defn- index-collection
  [irods msg]
  (index-entry collection (format-collection-doc irods (:entity msg)))
  (update-parent-modify-time irods (:entity msg)))

(defn- index-data-object
  [irods msg]
  (let [doc (format-data-object-doc irods (:entity msg)
              :creator   (format-user (:creator msg))
              :file-size (:size msg)
              :file-type (:type msg))]
    (index-entry data-object doc)
    (update-parent-modify-time irods (:entity msg))))

(defn- reindex-data-object
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

(defn- rename-collection
  [irods msg]
  (let [old-path (:entity msg)
        new-path (:new-path msg)]
    (rename-entry irods collection old-path (format-collection-doc irods new-path))
    (es-doc/delete-by-query-across-all-types index (es-query/wildcard :id (str old-path "/*")))
    (index-members irods new-path)))

(defn- rename-data-object
  [irods msg]
  (rename-entry irods data-object (:entity msg) (format-data-object-doc irods (:new-path msg))))

(defn- rm-collection
  [irods msg]
  (remove-entry collection (:entity msg))
  (update-parent-modify-time irods (:entity msg)))

(defn- rm-data-object
  [irods msg]
  (remove-entry data-object (:entity msg))
  (update-parent-modify-time irods (:entity msg)))

(defn- update-data-object-acl
  [irods msg]
  (let [path (:entity msg)
        perm (:permission msg)]
    (if (es-doc/present? index data-object path)
      (es-doc/update-with-script index
                                 data-object
                                 path
                                 "ctx._source.permissions = permissions"
                                 {:permissions (get-data-object-acl irods path)})
      (index-entry data-object (format-data-object-doc irods path)))))

(defn- resolve-consumer
  [routing-key]
  (case routing-key
    "collection.acl.mod"           nil
    "collection.add"               index-collection
    "collection.metadata.add"      nil
    "collection.metadata.adda"     nil
    "collection.metadata.cp"       nil
    "collection.metadata.mod"      nil
    "collection.metadata.rm"       nil
    "collection.metadata.rmw"      nil
    "collection.metadata.set"      nil
    "collection.mv"                rename-collection
    "collection.rm"                rm-collection
    "data-object.acl.mod"          update-data-object-acl
    "data-object.add"              index-data-object
    "data-object.cp"               index-data-object
    "data-object.metadata.add"     nil
    "data-object.metadata.adda"    nil
    "data-object.metadata.addw"    nil
    "data-object.metadata.cp"      nil
    "data-object.metadata.mod"     nil
    "data-object.metadata.rm"      nil
    "data-object.metadata.rmw"     nil
    "data-object.metadata.set"     nil
    "data-object.mod"              reindex-data-object
    "data-object.mv"               rename-data-object
    "data-object.rm"               rm-data-object
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

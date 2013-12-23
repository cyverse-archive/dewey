(ns dewey.indexing
  (:require [clojure.string :as string]
            [clojurewerkz.elastisch.query :as es-query]
            [clojurewerkz.elastisch.rest.document :as es-doc]
            [clj-jargon.init :as r-init]
            [clj-jargon.item-info :as r-info]
            [clj-jargon.lazy-listings :as r-lazy]
            [clojure-commons.file-utils :as file]
            [dewey.doc-prep :as prep]
            [dewey.util :as util]))


(defn- get-parent-path
  [path]
  (file/rm-last-slash (file/dirname path)))


(defn- index-entry
  [entity-type entry]
  (es-doc/create prep/index (prep/get-mapping-type entity-type) entry :id (:id entry)))


(defn- remove-entry
  [entity-type id]
  (let [mapping-type (prep/get-mapping-type entity-type)]
    (when (es-doc/present? prep/index mapping-type id)
      (es-doc/delete prep/index mapping-type id))))


(defn- update-parent-modify-time
  [irods entity-path]
  (let [parent-path  (get-parent-path entity-path)
        mapping-type (prep/get-mapping-type :collection)
        parent-id    (prep/format-id parent-path)]
    (when (r-info/exists? irods parent-path)
      (if (es-doc/present? prep/index mapping-type parent-id)
        (es-doc/update-with-script prep/index
                                   mapping-type
                                   parent-id
                                   "ctx._source.dateModified = dateModified;"
                                   {:dateModified (prep/get-date-modified irods parent-path)})
        (index-entry :collection (prep/format-collection-doc irods parent-path))))))


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


(defn- reindex-metadata
  [irods entity-type path format-doc]
  (let [mapping-type (prep/get-mapping-type entity-type)
        id           (prep/format-id path)]
    (if (es-doc/present? prep/index mapping-type id)
      (es-doc/update-with-script prep/index
                                 mapping-type
                                 id
                                 "ctx._source.metadata = metadata"
                                 {:metadata (get-metadata irods path)})
      (index-entry entity-type (format-doc irods path)))))


(defn- update-acl
  [irods entity-type doc-formatter entry]
  (let [mapping-type (prep/get-mapping-type entity-type)
        id           (prep/format-id entry)]
    (if (es-doc/present? prep/index mapping-type id)
      (es-doc/update-with-script prep/index
                                 mapping-type
                                 id
                                 "ctx._source.userPermissions = permissions"
                                 {:permissions (prep/get-acl irods entity-type entry)})
      (index-entry entity-type (doc-formatter irods entry)))))


(defn- update-collection-acl
  [irods coll]
  (update-acl irods :collection prep/format-collection-doc coll))


(defn- update-data-object-acl
  [irods obj]
  (update-acl irods :data-object prep/format-data-object-doc obj))


(defn- index-collection-handler
  [irods msg]
  (index-entry :collection (prep/format-collection-doc irods (:entity msg)))
  (update-parent-modify-time irods (:entity msg)))


(defn- index-data-object-handler
  [irods msg]
  (let [doc (prep/format-data-object-doc irods (:entity msg)
              :creator   (prep/format-user (:creator msg))
              :file-size (:size msg)
              :file-type (:type msg))]
    (index-entry :data-object doc)
    (update-parent-modify-time irods (:entity msg))))


(defn- reindex-collection-metadata-handler
  [irods msg]
  (reindex-metadata irods :collection (:entity msg) prep/format-collection-doc))

(defn- reinidex-coll-dest-metadata-handler
  [irods msg]
  (reindex-metadata irods :collection (:destination msg) prep/format-collection-doc))

(defn- reindex-data-object-handler
  [irods msg]
  (let [path         (:entity msg)
        mapping-type (prep/get-mapping-type :data-object)
        id           (prep/format-id path)]
    (if (es-doc/present? prep/index mapping-type id)
      (es-doc/update-with-script prep/index
                                 mapping-type
                                 id
                                 "ctx._source.dateModified = dateModified;
                                  ctx._source.fileSize = fileSize;"
                                 {:dateModified (prep/get-date-modified irods path)
                                  :fileSize     (:size msg)})
      (index-entry :data-object
                   (prep/format-data-object-doc irods path
                     :file-size (:size msg)
                     :file-type (:type msg))))))


(defn- reindex-data-object-metadata-handler
  [irods msg]
  (reindex-metadata irods :data-object (:entity msg) prep/format-data-object-doc))


(defn- reinidex-obj-dest-metadata-handler
  [irods msg]
  (reindex-metadata irods :data-object (:destination msg) prep/format-data-object-doc))


(defn- reindex-multiobject-metadata-handler
  [irods msg]
  (let [coll-path   (file/dirname (:pattern msg))
        obj-pattern (util/sql-glob->regex (file/basename (:pattern msg)))]
    (doseq [obj (r-lazy/list-files-in irods coll-path)]
      (when (re-matches obj-pattern (.getNodeLabelDisplayValue obj))
        (reindex-metadata irods :data-object obj prep/format-data-object-doc)))))


(defn- rename-collection-handler
  [irods msg]
  (let [old-path (:entity msg)
        new-path (:new-path msg)]
    (rename-entry irods :collection old-path (prep/format-collection-doc irods new-path))
    (es-doc/delete-by-query-across-all-types prep/index (es-query/wildcard :id (str old-path "/*")))
    (crawl-collection irods
                      new-path
                      #(index-entry :collection (prep/format-collection-doc %1 %2))
                      #(index-entry :data-object (prep/format-data-object-doc %1 %2)))))


(defn- rename-data-object-handler
  [irods msg]
  (rename-entry irods :data-object (:entity msg) (prep/format-data-object-doc irods (:new-path msg))))


(defn- rm-collection-handler
  [irods msg]
  (remove-entry :collection (:entity msg))
  (update-parent-modify-time irods (:entity msg)))


(defn- rm-data-object-handler
  [irods msg]
  (remove-entry :data-object (:entity msg))
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
        mapping-type (prep/get-mapping-type :data-object)
        id           (prep/format-id path)]
    (if (es-doc/present? prep/index mapping-type id)
      (es-doc/update-with-script prep/index
                                 mapping-type
                                 id
                                 "ctx._source.dateModified = dateModified;
                                  ctx._source.fileSize = fileSize;
                                  ctx._source.fileType = fileType;"
                                 {:dateModified (prep/get-date-modified irods path)
                                  :fileSize     (prep/get-data-object-size irods path)
                                  :fileType     (prep/get-data-object-type irods path)})
      (index-entry :data-object (prep/format-data-object-doc irods path)))))


(defn- resolve-consumer
  [routing-key]
  (case routing-key
    "collection.acl.mod"           update-collection-acl-handler
    "collection.add"               index-collection-handler
    "collection.metadata.add"      reindex-collection-metadata-handler
    "collection.metadata.adda"     reindex-collection-metadata-handler
    "collection.metadata.cp"       reinidex-coll-dest-metadata-handler
    "collection.metadata.mod"      reindex-collection-metadata-handler
    "collection.metadata.rm"       reindex-collection-metadata-handler
    "collection.metadata.rmw"      reindex-collection-metadata-handler
    "collection.metadata.set"      reindex-collection-metadata-handler
    "collection.mv"                rename-collection-handler
    "collection.rm"                rm-collection-handler
    "data-object.acl.mod"          update-data-object-acl-handler
    "data-object.add"              index-data-object-handler
    "data-object.cp"               index-data-object-handler
    "data-object.metadata.add"     reindex-data-object-metadata-handler
    "data-object.metadata.adda"    reindex-data-object-metadata-handler
    "data-object.metadata.addw"    reindex-multiobject-metadata-handler
    "data-object.metadata.cp"      reinidex-obj-dest-metadata-handler
    "data-object.metadata.mod"     reindex-data-object-metadata-handler
    "data-object.metadata.rm"      reindex-data-object-metadata-handler
    "data-object.metadata.rmw"     reindex-data-object-metadata-handler
    "data-object.metadata.set"     reindex-data-object-metadata-handler
    "data-object.mod"              reindex-data-object-handler
    "data-object.mv"               rename-data-object-handler
    "data-object.rm"               rm-data-object-handler
    "data-object.sys-metadata.mod" update-data-object-sys-meta-handler
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

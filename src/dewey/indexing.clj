(ns dewey.indexing
  "This is the logic that formats the elasticsearch index entry or entry update."
  (:require [clojurewerkz.elastisch.query :as es-query]
            [clojurewerkz.elastisch.rest.document :as es-doc]
            [dewey.doc-prep :as prep])
  (:import [org.irods.jargon.core.query CollectionAndDataObjectListingEntry]))


(def ^{:private true :const true} index "data")

(def ^{:private true :const true} collection-type "folder")
(def ^{:private true :const true} data-object-type "file")


(defmulti ^{:private true} format-id type)

(defmethod format-id String
  [path]
  path)

(defmethod format-id CollectionAndDataObjectListingEntry
  [entry]
  (.getFormattedAbsolutePath entry))


(defn- mapping-type-of
  [entity-type]
  (case entity-type
    :collection  collection-type
    :data-object data-object-type))


(defn entity-indexed?
  [entity-type entity]
  (es-doc/present? index (mapping-type-of entity-type) (format-id entity)))


(defn- index-entry
  [mapping-type entry]
  (es-doc/create index mapping-type entry :id (:id entry)))


(defn index-collection
  [irods collection]
  (index-entry collection-type
               (prep/format-folder (format-id collection)
                                   (.acl irods :collection collection)
                                   (.creator irods :collection collection)
                                   (.date-created irods collection)
                                   (.date-modified irods collection)
                                   (.metadata irods collection))))


(defn index-data-object
  [irods obj & {:keys [creator file-size file-type]}]
  (index-entry data-object-type
               (prep/format-file (format-id obj)
                                 (.acl irods :data-object obj)
                                 (or creator (.creator irods :data-object obj))
                                 (.date-created irods obj)
                                 (.date-modified irods obj)
                                 (.metadata irods obj)
                                 (or file-size (.data-object-size irods obj))
                                 (or file-type (.data-object-type irods obj)))))


(defn remove-entity
  [entity-type entity]
  (when (entity-indexed? entity-type entity)
    (es-doc/delete index (mapping-type-of entity-type) (format-id entity))))


(defn remove-entities-like
  [path-glob]
  (es-doc/delete-by-query-across-all-types index (es-query/wildcard :id path-glob)))


(defn update-collection-modify-time
  [irods entity]
  (es-doc/update-with-script index
                             collection-type
                             (format-id entity)
                             "ctx._source.dateModified = dateModified;"
                             {:dateModified (prep/format-time (.date-modified irods entity))}))


(defn update-metadata
  [irods entity-type entity]
  (es-doc/update-with-script index
                             (mapping-type-of entity-type)
                             (format-id entity)
                             "ctx._source.metadata = metadata"
                             {:metadata (prep/format-metadata (.metadata irods entity))}))


(defn update-acl
  [irods entity-type entity]
  (es-doc/update-with-script index
                             (mapping-type-of entity-type)
                             (format-id entity)
                             "ctx._source.userPermissions = permissions"
                             {:permissions (prep/format-acl (.acl irods entity-type entity))}))


(defn- update-data-obj-with-script
  [obj script vals]
  (es-doc/update-with-script index data-object-type (format-id obj) script vals))


(defn update-data-object
  ([irods obj file-size]
    (update-data-obj-with-script obj
                                 "ctx._source.dateModified = dateModified;
                                  ctx._source.fileSize = fileSize;"
                                 {:dateModified (prep/format-time (.date-modified irods obj))
                                  :fileSize     file-size}))
  ([irods obj file-size file-type]
    (update-data-obj-with-script obj
                                 "ctx._source.dateModified = dateModified;
                                  ctx._source.fileSize = fileSize;
                                  ctx._source.fileType = fileType;"
                                 {:dateModified (prep/format-time (.date-modified irods obj))
                                  :fileSize     file-size
                                  :fileType     file-type})))
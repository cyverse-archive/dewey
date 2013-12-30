(ns dewey.repo
  (:require [clj-jargon.init :as r-init]
            [clj-jargon.item-info :as r-info]
            [clj-jargon.lazy-listings :as r-lazy]
            [clj-jargon.metadata :as r-meta])
  (:import [org.irods.jargon.core.query CollectionAndDataObjectListingEntry]))


(defn- mk-user
  [name zone]
  {:name name :zone zone})


;; Create a hierarchy of iRODS entities. Collections and data objects are both entities.
(derive ::collection  ::entity)
(derive ::data-object ::entity)


(defmulti ^{:private true} get-acl #(vector %2 (type %3)))

(defmethod get-acl [:collection String]
  [ctx _ path]
  (.listPermissionsForCollection (:collectionAO ctx) path))

(defmethod get-acl [:data-object String]
  [ctx _ path]
  (.listPermissionsForDataObject (:dataObjectAO ctx) path))

(defmethod get-acl [:entity CollectionAndDataObjectListingEntry]
  [ctx _ entry]
  (.getUserFilePermission entry))


(defmulti ^{:private true} get-creator #(vector %2 (type %3)))

(defmethod get-creator [:collection String]
  [ctx _ path]
  (let [coll (r-info/collection ctx path)]
    (mk-user (.getCollectionOwnerName coll) (.getCollectionOwnerZone coll))))

(defmethod get-creator [:data-object String]
  [ctx _ path]
  (let [obj (r-info/data-object ctx path)]
    (mk-user (.getDataOwnerName obj) (.getDataOwnerZone obj))))

(defmethod get-creator [:entity CollectionAndDataObjectListingEntry]
  [ctx _ entry]
  (mk-user (.getOwnerName entry) (.getOwnerZone entry)))


(defmulti ^{:private true} get-data-object-size #(type %2))

(defmethod get-data-object-size String
  [ctx path]
  (r-info/file-size ctx path))

(defmethod get-data-object-size CollectionAndDataObjectListingEntry
  [ctx obj]
  (.getDataSize obj))


(defmulti ^{:private true} get-data-object-type #(type %2))

(defmethod get-data-object-type String
  [ctx path]
  (.getDataTypeName (r-info/data-object ctx path)))

(defmethod get-data-object-type CollectionAndDataObjectListingEntry
  [ctx obj]
  (.getDataTypeName (r-info/data-object ctx (.getFormattedAbsolutePath obj))))


(defmulti ^{:private true} get-date-created #(type %2))

(defmethod get-date-created String
  [ctx path]
  (r-info/created-date ctx path))

(defmethod get-date-created CollectionAndDataObjectListingEntry
  [ctx entry]
  (.getCreatedAt entry))


(defmulti ^{:private true} get-date-modified #(type %2))

(defmethod get-date-modified String
  [ctx path]
  (r-info/lastmod-date ctx path))

(defmethod get-date-modified CollectionAndDataObjectListingEntry
  [ctx entry]
  (.getModifiedAt entry))


(defmulti ^{:private true} get-metadata #(type %2))

(defmethod get-metadata String
  [ctx path]
  (r-meta/get-metadata ctx path))

(defmethod get-metadata CollectionAndDataObjectListingEntry
  [ctx entity]
  (r-meta/get-metadata ctx (.getFormattedAbsolutedPath entity)))


(defprotocol DataStore
  (acl [_ entity-type entity])
  (creator [_ entity-type entity])
  (data-object-size [_ obj])
  (data-object-type [_ obj])
  (date-created [_ entity])
  (date-modified [_ entity])
  (exists? [_ entity])
  (data-objects-in [_ path])
  (collections-in [_ path])
  (metadata [_ entity]))


(defrecord ^{:private true} IRODS [ctx]
  DataStore

  (acl [_ entity-type entity] (get-acl ctx entity-type entity))
  (creator [_ entity-type entity] (get-creator ctx entity-type entity))
  (data-object-size [_ obj] (get-data-object-size ctx obj))
  (data-object-type [_ obj] (get-data-object-type ctx obj))
  (date-created [_ entity] (get-date-created ctx entity))
  (date-modified [_ entity] (get-date-modified ctx entity))
  (exists? [_ path] (r-info/exists? ctx path))
  (data-objects-in [_ path] (r-lazy/list-files-in ctx path))
  (collections-in [_ path] (r-lazy/list-subdirs-in ctx path))
  (metadata [_ entity] (get-metadata ctx entity)))


(defn do-with-irods
  [irods-cfg perform]
  (r-init/with-jargon irods-cfg [ctx]
    (perform (->IRODS ctx))))
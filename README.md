dewey
=====

An AMQP message based iRODS indexer for elasticsearch.

dewey listens to the `irods` AMQP topic exchange for iRODS change messages. The iRODS change
messages are described in the document
https://docs.google.com/document/d/126uSOX8VWfFyRub1Ibqiknf4QbQuDZLOCktoBLpgqsE/edit. For each
change message, dewey updates the elasticsearch `data` index with the corresponding change. The web
page https://github.com/iPlantCollaborativeOpenSource/proboscis/tree/master/config/mappings
describes `data` schema.

Usage
-----

dewey is a service that is distributed as an RPM. To install and start the service on any RedHat-
compatible OS, run these commands:

```
$ sudo yum install dewey
$ sudo /sbin/service dewey start
```

License
-------

See the file LICENSE.txt.

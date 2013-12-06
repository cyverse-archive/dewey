(ns dewey.indexing-test
  (:use midje.sweet
        dewey.indexing)
  (:import [java.util Date]))

(facts "about `format-time`"
  (fact "works for a java.util.Date object"
    (format-time (Date. 1386180216000)) => "2013-12-04T18:03:36.000")
  (fact "works for a string containing a posix time in milliseconds"
    (format-time "1386180216000") => "2013-12-04T18:03:36.000"))



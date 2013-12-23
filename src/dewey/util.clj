(ns dewey.util
  "dewey utility functions")


(defn- special-regex?
  [c]
  (#{\\ \. \( \) \| \+ \^ \$ \[ \? \* \{} c))


(defn- translate-escaped
  [c]
  (if (= \\ c) "\\\\" c))


(defn sql-glob->regex
  "Takes an SQL glob-format string and returns a regex."
  [glob]
  (loop [stream glob
         re     ""]
    (let [[c] stream]
      (cond
        (nil? c)           (re-pattern re)
        (= c \\)           (recur (nnext stream) (str re (translate-escaped (fnext stream))))
        (= c \%)           (recur (next stream) (str re ".*"))
        (= c \_)           (recur (next stream) (str re \.))
        (special-regex? c) (recur (next stream) (str re \\ c))
        :else              (recur (next stream) (str re c))))))

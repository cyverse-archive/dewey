(ns dewey.util)


(defn- special-regex?
  [c]
  (#{\\ \. \( \) \| \+ \^ \$ \[ \? \* \{} c))

(defn- translate-escaped
  [c]
  (if (= \\ c) "\\\\" c))


(defn sql-glob->regex
  "Takes a glob-format string and returns a regex.

   This code was adapted from the glob->regex function in the clj-glob project,
   https://github.com/jkk/clj-glob/blob/master/src/org/satta/glob.clj on 2013-12-06."
  [glob]
  (loop [stream glob
         re     ""]
    (let [[c _] stream]
      (cond
        (nil? c)           (re-pattern re)
        (= c \\)           (recur (nnext stream) (str re (translate-escaped (fnext stream))))
        (= c \%)           (recur (next stream) (str re ".*"))
        (= c \_)           (recur (next stream) (str re \.))
        (special-regex? c) (recur (next stream) (str re \\ c))
        :else              (recur (next stream) (str re c))))))

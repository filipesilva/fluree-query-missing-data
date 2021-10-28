(ns repro
  (:require [fluree.db.api :as fdb]))


(def ledger "events/log")
(def conn (fdb/connect "http://localhost:8090"))
(def schema-tx [{:_id              :_collection
                 :_collection/name :event
                 :_collection/doc  "Athens semantic events."}
                {:_id               :_predicate
                 :_predicate/name   :event/id
                 :_predicate/doc    "A globally unique event id."
                 :_predicate/unique true
                 :_predicate/type   :string}
                {:_id             :_predicate
                 :_predicate/name :event/data
                 :_predicate/doc  "Event data serialized as an EDN string."
                 :_predicate/type :string}])


(defn new-event [x]
  {:_id        :event
   :event/id   (str "uuid-" x)
   :event/data "{}"})


(def order-by-query
  {:select {"?event" ["*"]}
   :where  [["?event" "event/id", "?id"]]
   ;; Subject (?event here) is a monotonically incrementing bigint,
   ;; so ordering by that gives us insertion order.
   :opts   {:orderBy ["ASC", "?event"]}})

(defn tx-size [size]
  (println size "bytes event data")
  [{:_id        :event
    :event/id   (str "size-" size)
    :event/data (repeat size "a")}])


(defn repro
  []

  ;; Create ledger
  @(fdb/new-ledger conn ledger)
  (fdb/wait-for-ledger-ready conn ledger)
  @(fdb/transact conn ledger schema-tx)

  ;; Add an event.
  (let [block (:block @(fdb/transact conn ledger [(new-event 1)]))
        db    (fdb/db conn ledger {:syncTo block})]

    ;; The query contains the transaction because syncTo was used.
    (println @(fdb/query db {:select ["*"]
                             :from "event"})))

  ;; Delete ledger and close conn.
  @(fdb/delete-ledger conn ledger)
  (fdb/close conn))

(defn -main
  []
  (repro))


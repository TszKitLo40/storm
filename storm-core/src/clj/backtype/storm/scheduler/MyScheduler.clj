;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.scheduler.MyScheduler
  (:use [backtype.storm util config log])
  (:require [backtype.storm.scheduler.EvenScheduler :as EvenScheduler])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment
            EvenScheduler ExecutorDetails])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

(defn- bad-slots [existing-slots num-executors num-workers]
  (if (= 0 num-workers)
    '()
    (let [distribution (atom (integer-divided num-executors num-workers))
          keepers (atom {})]
      (doseq [[node+port executor-list] existing-slots :let [executor-count (count executor-list)]]
        (when (pos? (get @distribution executor-count 0))
          (swap! keepers assoc node+port executor-list)
          (swap! distribution update-in [executor-count] dec)
          ))
      (->> @keepers
        keys
        (apply dissoc existing-slots)
        keys
        (map (fn [[node port]]
               (WorkerSlot. node port)))))))

(defn- slots-can-reassign [^Cluster cluster slots]
  (->> slots
    (filter
      (fn [[node port]]
        (if-not (.isBlackListed cluster node)
          (if-let [supervisor (.getSupervisorById cluster node)]
            (.contains (.getAllPorts supervisor) (int port))
            ))))))

(defn -prepare [this conf]
  )

;This function switch two executors from two different slots in purpose
(defn switch-executor [^String topology-id ^Cluster cluster]
  (let [assignment (.getAssignmentById cluster topology-id)
        ;_ (log-message "assignment:" (.getAssignments cluster))
        executor->slot (hashmap-to-persistent (.getExecutorToSlot assignment))
        ;_ (log-message "topology-id:" topology-id)
        ;_ (log-message "executor->slot:" executor->slot)
        slots (set (.getSlots assignment))
        ;_ (log-message "slots:" slots)
        first-slot (first slots)
        ;_ (log-message "first-slot:" first-slot)
        second-slot (second slots)
        ;_ (log-message "second-slot:" second-slot)
        first-slot-first-executor (if first-slot (first (keys (filter #(= (second %) first-slot) executor->slot))))
        ;_ (log-message "first-slot-first-executor:" first-slot-first-executor)
        second-slot-first-executor (if second-slot (first (keys (filter #(= (second %) second-slot) executor->slot))))
        ;_ (log-message "second-slot-first-executor:" second-slot-first-executor)
        ]
    (if (and first-slot-first-executor second-slot-first-executor)
      (do (.freeSlot cluster first-slot)
          (.freeSlot cluster second-slot)
          (.assign cluster first-slot topology-id (-> (keys (filter #(= (second %) first-slot) executor->slot))
                                        (set)
                                        (disj first-slot-first-executor)
                                        (conj second-slot-first-executor)))
          (.assign cluster second-slot topology-id (-> (keys (filter #(= (second %) second-slot) executor->slot))
                                         (set)
                                         (disj second-slot-first-executor)
                                         (conj first-slot-first-executor)))
          (log-message "[" (.getStartTask first-slot-first-executor) "," (.getEndTask first-slot-first-executor) "] and"
            "[" (.getStartTask second-slot-first-executor) "," (.getEndTask second-slot-first-executor) "] are interchanged!"))
      (log-message "the current topology does not have enough workers to make switch!")
      )))

(defn my-default-schedule [^Topologies topologies ^Cluster cluster]
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology (.getTopologies topologies)             ;@Li: the original value for the last argument is needs-scheduling-topologies. I just use topologies, becuase I want every topology can be interchanged
            :let [topology-id (.getId topology)
                  available-slots (->> (.getAvailableSlots cluster)
                                    (map #(vector (.getNodeId %) (.getPort %))))
                  all-executors (->> topology
                                  .getExecutors
                                  (map #(vector (.getStartTask %) (.getEndTask %)))
                                  set)
                  alive-assigned (EvenScheduler/get-alive-assigned-node+port->executors cluster topology-id)
                  alive-executors (->> alive-assigned vals (apply concat) set)
                  can-reassign-slots (slots-can-reassign cluster (keys alive-assigned))
                  total-slots-to-use (min (.getNumWorkers topology)
                                       (+ (count can-reassign-slots) (count available-slots)))
                  bad-slots (if (or (> total-slots-to-use (count alive-assigned))
                                  (not= alive-executors all-executors))
                              (bad-slots alive-assigned (count all-executors) total-slots-to-use)
                              [])]]
      (.freeSlots cluster bad-slots)
      (EvenScheduler/schedule-topologies-evenly (Topologies. {topology-id topology}) cluster)
      (switch-executor topology-id cluster))))

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (log-message "my-default-schedule will be called!")
  (my-default-schedule topologies cluster))

(ns indexer.core
	(use [clojure.set :only [union intersection]]))

(def *index* (ref {}))
(def *data* (ref {}))
(def *context*)
(def any nil)

(defmacro update-in-using
	[ data keys ele op & args ]
		`(if-let [s# (get-in ~data ~keys)] 
			(update-in ~data ~keys ~op ~@args)
			(-> ~data (assoc-in ~keys ~ele)
				(update-in ~keys ~op ~@args))))

(defn dissoc-in-using 
	[ data keys op & keys-to-remove ]
		(if (seq keys)
			(reduce #(assoc-in %1 keys 
				(op (get-in %1 keys) %2)) data keys-to-remove)  
			(apply op data keys-to-remove)))

;(disj #{ 1 2 } 2 1)
;
;(dissoc-in-using { 1 { 2 #{ 3 4 } } }  [ 1 2 ] disj 4 3)

(defn insert[ record ]
	(dosync 
		(let[ id (or (get record :_id) (count (get @*data* *context*))) 
				new-record (assoc record :_id id) ]
			(doseq[ [key val] new-record ]
				(alter *data* assoc-in [*context* id ] new-record)
				(alter *index* #(update-in-using %1  [*context* key val] #{} conj id))))))

(defn delete[ id ]
	(dosync 
		(let[ record (get-in @*data* [*context* id]) ]
			(doseq[ [k v] record ]
				(alter *index* dissoc-in-using [*context* k v] disj id)
				(alter *index* (fn[ data] (if (empty? (get-in data [*context* k v]))
					(dissoc-in-using data [*context* k] dissoc v)
						data)))

					)
			(alter *data* dissoc-in [*context* ] id) )))

;(from :perm
;	(insert { :ala "hej" :kot "w" })
;	(insert { :ala "bej" :kot "w" }))
;
;(from :perm
;	(select (where { :ala "hej"})))
;
;(from :perm
;	(delete 0))
;
;(println @*data*)
;(println @*index*)
;

(defmacro from[ name & body ]
	`(binding[ *context* ~name ]
		~@body))

(defn select
	([ ids ]
		(let [ctx *context*]
			(map #(get-in @*data* [ctx %1]) ids)))
	([ keys ids ]
		(seq (clojure.set/project 
			(select ids) keys))))

(defn where[ query ]
	(let[ r (filter (fn[x] (if (nil? x) (list) x)) (map (fn[ [key val] ]
			(clojure.set/union (get-in @*index* [*context* key val]) 
				(or (get-in @*index* [*context* key any]) #{}) )) query)) ]
		(if (seq r)
			(apply clojure.set/intersection r) (list))))


;;; solution to the problem ;;;
;
;We have set of triples which looks like: 
;Set of roles; set of operations; set of states 
;
;Triples are defined in scope of some entity with states, wildcard is 
;defined with _ 
;
;I need to answer on some operation invocation if given user 
;with his role is able to execute that operation for particular entity 
;which is in one of its states.
;Addtionally what are operations avaiable for given role and given state of entity.
;
;Example: 
;Roles: admin, operator, auditor 
;Entity: data form with states dirty, applied, rejected, executed 
;Operations on data form: reject, acept, list, enter 
;Triples of permissions: 
;admin, operator; reject, accept; applied 
;auditor, operator; list; _ 
;operator; enter; dirty 
;

(defn permission[ roles ops states ]
	(letfn [ (or-any [ a ] (or a #{ any })) ]
		(from :permission
			(doseq[ r (or-any roles) o (or-any ops) s (or-any states) ]
				(insert { :role r :op o :state s })))))

(permission #{:admin :operator } #{:reject :accept} #{:applied} )
(permission #{:auditor :operator } #{:list} any )
(permission #{:operator } #{:enter} #{:dirty} )

(defn can-access[ role op state ]
	(not (empty? (from :permission
		(where { :role role :op op :state state })))))

(defn get-operations[ role state ]
	(map :op (from :permission
		(select [ :op ]
			(where { :role role :state state })))))

; Some tests

(get-operations  :operator :applied)
(can-access :auditor :list :applied)
(can-access :admin :enter :dirty)





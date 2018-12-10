/**
 * Project: levelkv
 * File: levelkv.js
 * Author: JCloudYu
 * Create Date: Aug. 31, 2018 
 */
(()=>{
	"use strict";
	
	const path = require( 'path' );
	const {RAStorage} = require( 'rastorage' );
	const {ThrottledQueue, PromiseWaitAll, LevelKVError, Hash:{djb2a}} = require( './lib' );
	const {Serialize, Deserialize} 	= require('beson');
	
	
	
	const _REL_MAP = new WeakMap();
	const DB_STATUS = {
		OK: 0,
		CLOSING: 1,
		CLOSED: 2
	};
	const INDEX_BLOCK_POS = {
		DB_STATE: 1,
		ROOT_INDEX: 2
	};
	
	const DB_OP = {
		FETCH_INDEX: 'FETCH_INDEX',
		FETCH:	'FETCH',
		PUT:	'PUT',
		DEL:	'DEL',
		CLOSE:	'CLOSE'
	};
	const DB_OP_LOCK = {
		NONE:	0,
		READ:	1,
		WRITE:	2,
		ALL:	3
	};
	
	const INDEX_OP = {
		GET: 'GET',
		ADD: 'ADD',
		DEL: 'DEL'
	};
	const INDEX_LOCK = {
		NONE:	0,
		READ:	1,
		WRITE:	2
	};



	class DBCursor {
		constructor(db, segments) {
			_REL_MAP.set(this, {
				db, segments
			});
		}
		async toArray() {
			const results = [];
			for await ( let record of this ) {
				results.push(record);
			}
			
			return results;
		}
		get size() 		{ const {segments} = _REL_MAP.get(this); return segments.length; }
		get length() 	{ const {segments} = _REL_MAP.get(this); return segments.length; }
		next() {
			const {db, segments} = _REL_MAP.get(this);
			const {_op_throttle} = _REL_MAP.get(db);
			if ( segments.length > 0 ) {
				return { value: _op_throttle.push({op:DB_OP.FETCH, id: segments.shift()}) };
			}
			else {
				return {done:true};
			}
		}
		[Symbol.iterator](){ return this; }
	}
	class DBMutableCursor extends DBCursor {
		get segments() { const {segments} = _REL_MAP.get(this); return segments; }
		next() {
			const {db, segments} = _REL_MAP.get(this);
			const {_op_throttle} = _REL_MAP.get(db);
			if ( segments.length > 0 ) {
				let {_id:dataId, _in:internal=false, _v} = segments.shift();
				if ( !internal ) {
					return {value:Promise.resolve(_v)};
				}

				return { value: _op_throttle.push({op:DB_OP.FETCH, id:dataId}) };
			}
			else {
				return {done:true};
			}
		}
	}
	class LevelKV {
		constructor() {
			_REL_MAP.set(this, {
				_dirty:false,
				
				_db_status:DB_STATUS.CLOSED,
				_storage_path:null,
				_index_path:null,
				_hIndex:null,		// Handle
				_hData:null,		// Handle
				_state:null,
				_root_index:null,
				
				_op_throttle: ThrottledQueue.CreateQueueWithConsumer(___THROTTLE_TIMEOUT.bind(null, this)),
				_index_op_throttle: ThrottledQueue.CreateQueueWithConsumer(___INDEX_THROTTLE_TIMEOUT.bind(null, this)),
				
				_close_lock:setInterval(()=>{}, 86400)
			});
		}

		/**
		 * Close the database.
		 *
		 * @returns Promise
		**/
		close() {
			const PRIVATE = _REL_MAP.get(this);
			switch( PRIVATE._db_status ) {
				case DB_STATUS.OK:
				{
					PRIVATE._db_status = DB_STATUS.CLOSING;
				
					const p = PRIVATE._op_throttle.push({op:DB_OP.CLOSE});
					return p.promise;
				}
				
				case DB_STATUS.CLOSING:
					return Promise.reject(new LevelKVError( LevelKVError.DB_DUPLICATED_CLOSE_ERROR ));
				
				case DB_STATUS.CLOSED:
				default:
					return Promise.resolve();
			}
		}

		/**
		 * Get data from the database.
		 *
		 * @param {string|string[]} keys - A specific key or an array of keys to retrieve, if not given it will retrieve all data from the database.
		 * @param {Object} options
		 * @param {Boolean} [options.mutable_cursor=false]
		 * @returns {Promise<DBCursor>} - Database cursor of the retrieved data.
		 */
		get(keys=[], options={mutable_cursor:false}) {
			const {_op_throttle, _db_status} = _REL_MAP.get(this);
			if ( _db_status !== DB_STATUS.OK ) {
				throw new LevelKVError(LevelKVError.DB_CLOSING_ERROR);
			}
			if ( !Array.isArray(keys) ) { keys = [keys]; }
			
			
			
			const promises = [];
			const registered = new Map();
			const reserved_keys = [];
			for(let key of keys) {
				let prev = registered.get(key);
				if ( prev ) { continue; }

				const p = _op_throttle.push({op:DB_OP.FETCH_INDEX, key:key.toString()});
				registered.set(key, p);
				promises.push(p);
				reserved_keys.push(key);
			}

			if ( promises.length <= 0 ) {
				return Promise.resolve(new DBCursor(this, []));
			}


			return Promise.all(promises).then((matches)=>{
				let newMatches = [];
				for( let i=0; i < matches.length; i++ ) {
					let dataId = matches[i];
					if ( dataId === null ) {continue;}



					if (!options.mutable_cursor) {
						newMatches.push(dataId);
					}
					else {
						newMatches.push({
							_in:true, key:reserved_keys.shift(), _id:dataId
						});
					}
				}


				if (!options.mutable_cursor) {
					return new DBCursor(this, newMatches);
				}
				else {
					return new DBMutableCursor(this, newMatches);
				}
			});
		}

		/**
		 * Add data to the database.
		 *
		 * @param {string|string[]} keys - A specific key or an array of keys to add.
		 * @param {*} val - The value to add.
		 * @returns Promise
		**/
		put(keys=[], val) {
			const {_op_throttle, _db_status} = _REL_MAP.get(this);
			if ( _db_status !== DB_STATUS.OK ) {
				throw new LevelKVError(LevelKVError.DB_CLOSING_ERROR);
			}
			if ( !Array.isArray(keys) ) { keys = [keys]; }
		
		
			
			const serialized_val = Serialize(val);
			const promises = [];
			const registered = new Map();
			for(let key of keys) {
				let prev = registered.get(key);
				if ( prev ) {
					promises.push(prev);
					continue;
				}
				
				
				const p = _op_throttle.push({op:DB_OP.PUT, key:key.toString(), data:serialized_val});
				registered.set(key, p);
				promises.push(p);
			}
			
			
			
			if ( promises.length <= 0 ) {
				return Promise.resolve();
			}
			
			return PromiseWaitAll(promises);
		}

		/**
		 * Delete data from the database.
		 *
		 * @param {string|string[]} keys -  A specific key or an array of keys to delete.
		 * @returns Promise
		**/
		del(keys=[]) {
			const {_op_throttle, _db_status} = _REL_MAP.get(this);
			if ( _db_status !== DB_STATUS.OK ) {
				throw new LevelKVError(LevelKVError.DB_CLOSING_ERROR);
			}
			if ( !Array.isArray(keys) ) { keys = [keys]; }
		
		
		
			const promises = [];
			const registered = new Map();
			for(let key of keys) {
				let prev = registered.get(key);
				if ( prev ) {
					promises.push(prev);
					continue;
				}
				
				const p = _op_throttle.push({op:DB_OP.DEL, key:key.toString()});
				registered.set(key, p);
				promises.push(p);
			}
			
			if ( promises.length <= 0 ) {
				return Promise.resolve();
			}
			
			return PromiseWaitAll(promises);
		}

		/**
		 * Initialize the database.
		 *
		 * @async
		 * @param {string} dir - Directory path of the database. Make sure you have created or it will fail if the directory does not exist.
		 * @param {object} options - Database creating options. Defaults to {auto_create:true}, which means create a new database automatically if not exist.
		 * @returns {Promise<LevelKV>} - Promise object represents the database itself.
		**/
		static async initFromPath(dir, options={auto_create:true}) {
			const DB_PATH = path.resolve(dir);
			const DB = new LevelKV();
			const PRIVATE = _REL_MAP.get(DB);
			
			
			
			
			let INDEX_STORAGE, DATA_STORAGE;

			// region [ Prepare DB Index ]
			PRIVATE._index_path = `${DB_PATH}/index`;
			try {
				INDEX_STORAGE = await RAStorage.InitAtPath( PRIVATE._index_path );
				INDEX_STORAGE._serializer = Serialize;
				INDEX_STORAGE._deserializer = Deserialize;
				
				PRIVATE._hIndex	= INDEX_STORAGE;
			}
			catch(e) {
				const error = new LevelKVError(LevelKVError.DB_INDEX_INIT);
				error.details.push(e);
				throw error;
			}
			// endregion

			// region [ Prepare DB Storage ]
			PRIVATE._storage_path = `${DB_PATH}/storage`;
			try {
				DATA_STORAGE = await RAStorage.InitAtPath( PRIVATE._storage_path );
				//DATA_STORAGE._serializer = Serialize;
				DATA_STORAGE._deserializer = Deserialize;
				
				PRIVATE._hData	= DATA_STORAGE;
			}
			catch(e) {
				const error = new LevelKVError(LevelKVError.DB_STORAGE_INIT);
				error.details.push(e);
				throw error;
			}
			// endregion
			
			// region [ Load & Construct DB Index ]
			try {
				let db_state = await INDEX_STORAGE.get( INDEX_BLOCK_POS.DB_STATE );
				if ( !db_state ) {
					db_state = {create:Date.now()};
					await INDEX_STORAGE.put(db_state);
				}
				
				let root_indices = await INDEX_STORAGE.get( INDEX_BLOCK_POS.ROOT_INDEX );
				if ( !root_indices ) {
					root_indices = [];
					await INDEX_STORAGE.put(root_indices);
				}
				
				const db_root_index = new Map();
				for( let i=0; i<root_indices.length; i++ ) {
					let [hash, id] = root_indices[i];
					db_root_index.set(hash, id);
				}
				
				
				
				PRIVATE._state = db_state;
				PRIVATE._root_index = db_root_index;
			}
			catch(e) {
				const error = new LevelKVError(LevelKVError.DB_INDEX_INIT);
				error.details.push(e);
				throw error;
			}
			// endregion



			
			
			PRIVATE._db_status = DB_STATUS.OK;
			return DB;
		}
	}
	module.exports = { LevelKV, DBMutableCursor };
	
	
	
	
	
	
	async function ___THROTTLE_TIMEOUT(inst, queue) {
		if (queue.length <= 0) return;
		
		const push_back = [], promises=[], op_res_queue = [];
		const locker = new Map();
		while(queue.length > 0) {
			const op_req = queue.shift();
			const {info, ctrl} = op_req;
			switch(info.op) {

				case DB_OP.FETCH_INDEX:
				{
					const lock = locker.get(info.key)|0;
					if ( lock > DB_OP_LOCK.READ ) {
						push_back.push(op_req);
					}
					else {
						locker.set(info.key, DB_OP_LOCK.READ);
						promises.push(___DB_OP_FETCH_INDEX(inst, info));
						op_res_queue.push(ctrl);
					}
					break;
				}

				case DB_OP.FETCH:
				{
					const lock = locker.get(info.key)|0;
					if ( lock > DB_OP_LOCK.READ ) {
						push_back.push(op_req);
					}
					else {
						locker.set(info.key, DB_OP_LOCK.READ);
						promises.push(___DB_OP_FETCH(inst, info));
						op_res_queue.push(ctrl);
					}

					break;
				}
				
				case DB_OP.PUT:
				{
					const lock = locker.get(info.key)|0;
					if ( lock > DB_OP_LOCK.NONE ) {
						if ( lock === DB_OP_LOCK.READ ) {
							locker.set(info.key, DB_OP_LOCK.ALL);
						}
						
						push_back.push(op_req);
					}
					else {
						locker.set(info.key, DB_OP_LOCK.WRITE);
						promises.push(___DB_OP_PUT(inst, info));
						op_res_queue.push(ctrl);
					}
					break;
				}
				
				case DB_OP.DEL:
				{
					const lock = locker.get(info.key)|0;
					if ( lock > DB_OP_LOCK.NONE ) {
						if ( lock === DB_OP_LOCK.READ ) {
							locker.set(info.key, DB_OP_LOCK.ALL);
						}
						
						push_back.push(op_req);
					}
					else {
						locker.set(info.key, DB_OP_LOCK.WRITE);
						promises.push(___DB_OP_DEL(inst, info));
						op_res_queue.push(ctrl);
					}
				
					break;
				}
				
				case DB_OP.CLOSE:
				{
					if ( queue.length > 0 || op_res_queue.length > 0 ) {
						push_back.push(op_req);
					}
					else {
						promises.push(___DB_OP_CLOSE(inst, info));
						op_res_queue.push(ctrl);
					}
					break;
				}
			}
		}
		
		queue.splice(0, 0, ...push_back);
		
		
		
		let results = await PromiseWaitAll(promises);
		for(let {resolved, seq, result} of results) {
			const {resolve, reject} = op_res_queue[seq];
			(resolved?resolve:reject)(result)
		}
	}
	async function ___DB_OP_FETCH(inst, op) {
		const {_hData} = _REL_MAP.get(inst);
		const {id} = op;
		return await _hData.get(id);
	}
	async function ___DB_OP_FETCH_INDEX(inst, op) {
		const {key} = op;
		return await ___GET_INDEX(inst, key);
	}
	async function ___DB_OP_PUT(inst, op) {
		const {_hData} = _REL_MAP.get(inst);
		const {key, data} = op;
		
		let index = await ___GET_INDEX(inst, key);
		if ( index !== null ) {
			await _hData.set(index, data);
		}
		else {
			let storageId = await _hData.put(data);
			await ___ADD_INDEX(inst, key, storageId);
		}
	}
	async function ___DB_OP_DEL(inst, op) {
		const PRIVATE = _REL_MAP.get(inst);
		const {_hData} = PRIVATE;
		const {key} = op;
		
		let index = await ___GET_INDEX(inst, key);
		if ( index !== null ) {
			await _hData.del(index);
		}
		
		await ___DEL_INDEX(inst, key);
		
	}
	async function ___DB_OP_CLOSE(inst) {
		const PRIVATE = _REL_MAP.get(inst);
		
		await PromiseWaitAll([
			PRIVATE._hData.close(),
			PRIVATE._hIndex.close()
		]);
		
		clearInterval(PRIVATE._close_lock);
		PRIVATE._close_lock = null;
		PRIVATE._hData = PRIVATE._hIndex = PRIVATE._storage_path =
		PRIVATE._index_path = PRIVATE._storage_path = PRIVATE._root_index = null;
		PRIVATE._db_status = DB_STATUS.CLOSED;
	}
	
	
	
	
	
	
	
	function ___GET_INDEX(inst, key) {
		const {_index_op_throttle} = _REL_MAP.get(inst);
		return _index_op_throttle.push({op:INDEX_OP.GET, key});
	}
	function ___ADD_INDEX(inst, key, relId) {
		const {_index_op_throttle} = _REL_MAP.get(inst);
		return _index_op_throttle.push({op:INDEX_OP.ADD, key, id:relId});
	}
	function ___DEL_INDEX(inst, key) {
		const {_index_op_throttle} = _REL_MAP.get(inst);
		return _index_op_throttle.push({op:INDEX_OP.DEL, key});
	}
	
	async function ___INDEX_THROTTLE_TIMEOUT(inst, queue) {
		if (queue.length <= 0) return;
		
		
		
		const push_back = [], promises=[], op_res_queue = [];
		let lock = INDEX_LOCK.NONE;
		while(queue.length > 0) {
			const op_req = queue.shift();
			const {info, ctrl} = op_req;
			switch(info.op) {
				case INDEX_OP.GET:
					if ( lock > INDEX_LOCK.READ ) {
						push_back.push(op_req);
					}
					else {
						lock = INDEX_LOCK.READ;
						promises.push(___INDEX_OP_GET(inst, info));
						op_res_queue.push(ctrl);
					}
					break;
					
				case INDEX_OP.ADD:
					if ( lock !== INDEX_LOCK.NONE ) {
						push_back.push(op_req);
					}
					else {
						lock = INDEX_LOCK.WRITE;
						promises.push(___INDEX_OP_ADD(inst, info));
						op_res_queue.push(ctrl);
					}
					break;
					
				case INDEX_OP.DEL:
					if ( lock !== INDEX_LOCK.NONE ) {
						push_back.push(op_req);
					}
					else {
						lock = INDEX_LOCK.WRITE;
						promises.push(___INDEX_OP_DEL(inst, info));
						op_res_queue.push(ctrl);
					}
					break;
			}
		}
		
		queue.splice(0, 0, ...push_back);
		
		
		// NOTE: Wait for all index operations
		let results = await PromiseWaitAll(promises);
		
		
		// NOTE: If the indices are dirty, then clean it!
		if ( _REL_MAP.get(inst)._dirty ) {
			await ___INDEX_DIRTY_CLEAN(inst);
		}
		
		// NOTE: Resolve everything...
		for(let {resolved, seq, result} of results) {
			const {resolve, reject} = op_res_queue[seq];
			(resolved?resolve:reject)(result)
		}
	}
	async function ___INDEX_OP_GET(inst, opInfo) {
		const {key} = opInfo;
		const {_root_index, _hIndex} = _REL_MAP.get(inst);
		const hash = djb2a(key);
		const indexId = _root_index.get(hash);
		if ( !indexId ) return null;
		
		const indexList = await _hIndex.get(indexId);
		if ( Object(indexList) !== indexList ) {
			const error = new LevelKVError(LevelKVError.DB_INDEX_STRUCTURE);
			error.details.push({key, hash, indexId});
			throw error;
		}
		
		if ( indexList.hasOwnProperty(key) ) {
			return indexList[key];
		}
		
		return null;
	}
	async function ___INDEX_OP_ADD(inst, opInfo) {
		const {key, id:storageId} = opInfo;
		const PRIVATE = _REL_MAP.get(inst);
		const {_root_index, _hIndex, _dirty} = PRIVATE;
		const hash = djb2a(key);
		
		let indexId = _root_index.get(hash);
		if ( !indexId ) {
			indexId = await _hIndex.put({});
			_root_index.set(hash, indexId);
			PRIVATE._dirty = _dirty || true;
		}
		
		const indexList = await _hIndex.get(indexId);
		if ( Object(indexList) !== indexList ) {
			const error = new LevelKVError(LevelKVError.DB_INDEX_STRUCTURE);
			error.details.push({key, hash, indexId});
			throw error;
		}
		
		indexList[key] = storageId;
		await _hIndex.set(indexId, indexList);
	}
	async function ___INDEX_OP_DEL(inst, opInfo) {
		const {key} = opInfo;
		const PRIVATE = _REL_MAP.get(inst);
		const {_root_index, _hIndex, _dirty} = PRIVATE;
		const hash = djb2a(key);
		
		let indexId = _root_index.get(hash);
		if ( !indexId ) return;
		
		
		
		const indexList = await _hIndex.get(indexId);
		if ( Object(indexList) !== indexList ) {
			const error = new LevelKVError(LevelKVError.DB_INDEX_STRUCTURE);
			error.details.push({key, hash, indexId});
			throw error;
		}
		
		if ( indexList.hasOwnProperty(key) ) {
			delete indexList[key];
		}
		
		let keep = false;
		for(let k in indexList) {
			if ( indexList.hasOwnProperty(k) ) {
				keep = keep || true;
				break;
			}
		}
		
		
		
		
		if ( keep ) {
			await _hIndex.put(indexId, indexList);
		}
		else {
			await _hIndex.del(indexId);
			_root_index.delete(hash);
			PRIVATE._dirty = _dirty || true;
		}
	}
	async function ___INDEX_DIRTY_CLEAN(inst) {
		const PRIVATE = _REL_MAP.get(inst);
		const {_root_index, _hIndex} = PRIVATE;
		
		let list = [];
		_root_index.forEach((id, hash)=>{
			list.push([hash, id]);
		});
		await _hIndex.set( INDEX_BLOCK_POS.ROOT_INDEX, list );
		PRIVATE._dirty = false;
	}
})();

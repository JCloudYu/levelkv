/**
 * Project: levelkv
 * File: levelkv.js
 * Author: JCloudYu
 * Create Date: Aug. 31, 2018 
 */
(()=>{
	"use strict";
	
	const fs	= require( 'fs' );
	const path	= require( 'path' );
	
	
	
	
	const _LevelKV  = new WeakMap();
	const _DBCursor = new WeakMap();
	
	class DBCursor {
		constructor(db, segments) {
			const PROPS = {
				db: _LevelKV.get(db),
				segments
			};
			_DBCursor.set(this, PROPS);
		}
		async toArray() {
			const results = [];
			for await ( let record of this ) {
				results.push(record);
			}
			
			return results;
		}
		next() {
			const { db:{storage_fd}, segments } = _DBCursor.get(this);
			if ( segments.length > 0 ) {
				let {from, length} = segments.shift();
				return {value:new Promise((resolve, reject)=>{
					fs.read(storage_fd, Buffer.alloc(length), 0, length, from, (err, numBytes, buff)=>{
						if ( err ) {
							return reject({_system:true, error:err});
						}
						
						if ( numBytes !== length ) {
							return reject({_system:false, error:new Error("Not enough data!")});
						}
						
						resolve(JSON.parse(buff.toString('utf8')));
					});
				})};
			}
			else {
				return {done:true};
			}
		}
		[Symbol.iterator](){ return this; }
	}
	class LevelKV {
		constructor() {
			const PROPS = {};
			_LevelKV.set(this, PROPS);
			
			PROPS.valid = false;
		}
		
		async close() {}
		
		async get(keys=[]) {
			if ( !Array.isArray(keys) ) { keys = [keys]; }
			const {index} = _LevelKV.get(this);
			const matches = [];
			for( let key of keys ) {
				if ( index[key] ) { matches.push(key) }
			}
			
			return new DBCursor(this, matches);
		}
		
		async put(keys=[], val) {
			if ( !Array.isArray(keys) ) { keys = [keys]; }
			const {storage_fd, index} = _LevelKV.get(this);
			let promises = [];
			for( let key of keys ) {
				if ( index[key] ) {
				
				}
				
				let data = Buffer.from(JSON.stringify(val), 'utf8');
				fs.write(storage_fd, data, 0, data.length, state.storage.size, (err, numBytes)=>{
				
				});
			}
			
			
			// TODO: Update index
		}
		
		async del(keys=[]) {
			if ( !Array.isArray(keys) ) { keys = [keys]; }
			const {index, state} = _LevelKV.get(this);
			for( let key of keys ) {
				if ( index[key] ) {
					state.storage.fragments.push(index[key]);
					delete index[key];
				}
			}
			
			// TODO: Update index
		}
		
		static async initFromPath(dir, options={auto_create:true}) {
			const DB_PATH = path.resolve(dir);
			const DB = new LevelKV();
			const PROPS	= _LevelKV.get(DB);
			
			
			
			// region [ Read DB States ]
			PROPS.state_path = `${DB_PATH}/state.json`;
			try {
				PROPS.state = JSON.parse(fs.readFileSync(PROPS.state_path));
			}
			catch(e) {
				if ( !options.auto_create ) {
					throw new Error(`Cannot read database state! (${PROPS.state_path})`);
				}
				else {
					PROPS.state = ___GEN_DEFAULT_STATE();
					try {
						fs.writeFileSync(JSON.stringify(PROPS.state));
					}
					catch(e) {
						throw new Error(`Cannot write database state! (${PROPS.state_path})`);
					}
				}
			}
			// endregion

			// region [ Read DB Index ]
			PROPS.index_path = `${DB_PATH}/index.jlst`;
			PROPS.index_segd_path = `${DB_PATH}/index.segd`;
			try {
				PROPS.index_segd_fd = fs.openSync( PROPS.index_segd_path, "a+" );
				PROPS.index_fd = fs.openSync( PROPS.index_path, "a+" );
				PROPS.index = ___READ_INDEX( PROPS.index_segd_fd, PROPS.index_fd );
			}
			catch(e) {
				/*
				PROPS.index = {};
				
				try {
					___WRITE_INDEX(PROPS.index_path, PROPS.index);
				}
				catch(e) {
					throw new Error(`Cannot write database main index! (${PROPS.index_path})`);
				}
				*/
			}
			// endregion
			
			// region [ Prepare DB Storage ]
			PROPS.storage_path = `${DB_PATH}/storage.jlst`;
			try {
				PROPS.storage_fd = fs.openSync( PROPS.storage_path, "a+" );
			}
			catch(e) {
				throw new Error( `Cannot access database storage! (${PROPS.storage_path})` );
			}
			// endregion
			
			
			
			PROPS.valid = true;
			return DB;
		}
	}
	
	module.exports = LevelKV;
	
	
	
	function ___READ_INDEX(segd_fd, index_fd) {
		const SEGMENT_DESCRIPTOR_LENGTH = 9;
		const segd_size = fs.fstatSync(segd_fd).size;
		let rLen, buff = Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH), segd_pos = 0, prev = null;

		const result = {};
		while(segd_pos < segd_size) {
			rLen = fs.readSync(segd_fd, buff, 0, SEGMENT_DESCRIPTOR_LENGTH, segd_pos);
			if ( rLen !== 9 ) {
				throw "Insufficient data in index segmentation descriptor!";
			}

			if ( !prev ) {
				prev = Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH);
			}
			else
			if ( prev[SEGMENT_DESCRIPTOR_LENGTH - 1] ) {
				let pos 		= prev.readDoubleLE(0);
				let length 		= buff.readDoubleLE(0) - pos;

				let raw_index 	= Buffer.alloc(length);
				rLen 			= fs.readSync(index_fd, raw_index, 0, length, pos);
				if ( rLen !== length ) {
					throw "Insufficient data in index!";
				}


				let indexStr = raw_index.toString();
				let { 0:key, 1:position, 2:len } = JSON.parse( indexStr.slice(0, indexStr.length - 1) );

				result[key] = {pos:position, length:len};
			}



			let tmp = prev;
			prev = buff;
			buff = tmp;

			segd_pos += SEGMENT_DESCRIPTOR_LENGTH;
		}


		return result;
	}
	async function ___WRITE_INDEX(index_path, index) {
	
	}
	function ___GEN_DEFAULT_STATE() {
		return {
			version:1, total_records:0,
			index:{ segments:0, size:0, frags:[] },
			storage:{ size:0, frags:[] }
		};
	}

	
	
	
	/*
	const PROP_MAP = new WeakMap();
	class LevelKV {
		constructor(){
			PROP_MAP[this] = {};
		}
		async open(dir, options={type:'json'}) {
			const PROPS = PROP_MAP.get(this);
			PROPS.db = new ((options.type === 'json') ? DEFAULT_BSON_DB : DEFAULT_JSON_DB)();
			return PROPS.db.open(dir, options);
		}
		async close() {
			const PROPS = PROP_MAP.get(this);
			return PROPS.db.close();
		}
		async get(query=null) {
			const PROPS = PROP_MAP.get(this);
			return PROPS.db.get(query);
		}
		async put(query=null, content={}) {
			const PROPS = PROP_MAP.get(this);
			return PROPS.db.put(query, content);
		}
		async del(query=null) {
			const PROPS = PROP_MAP.get(this);
			return PROPS.db.del(query);
		}
	}
	module.exports=LevelKV;
	*/
	
	
	
	
	/*
		const levelkv = require('levelkv');
		let db = await levelkv();
		await db.open()
		await db.close()
		await db.put()
		await db.del()
		
		let iterator = await db.get()
		await iterator.next()
		await iterator.seek()
		await iterator.end()
		
		
		db.batch()
		db.approximateSize()
		db.compactRange()
		db.getProperty()
		db.iterator()
	 */
})();

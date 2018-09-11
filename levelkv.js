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

	const SEGMENT_DESCRIPTOR_LENGTH = 9;
	const DATA_IS_AVAILABLE 		= 0x01;
	const DATA_IS_UNAVAILABLE 		= 0x00;

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
		
		async close() {
			const props = _LevelKV.get(this);

			fs.closeSync(props.index_segd_fd);
			fs.closeSync(props.index_fd);
			fs.closeSync(props.storage_fd);

			props.valid = false;
		}
		
		async get(keys=[]) {
			const {index, valid} = _LevelKV.get(this);
			if( !valid ) throw new Error( 'Database is not available!' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }
			if( !keys.length ) keys = Object.keys(index);

			const matches = [];
			for( let key of keys ) {
				if ( index[key] ) { matches.push(index[key]); }
			}
			return new DBCursor(this, matches);
		}
		
		async put(keys=[], val) {
			const {storage_fd, index_fd, index_segd_fd, index_segd, index, state, state_path, valid} = await _LevelKV.get(this);
			if( !valid ) throw new Error( 'Database is not available!' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }

			// INFO: Update the value
			for( let key of keys ) {
				if ( index[key] ) {
					const prev_index 	= index[key];
					const prev_segd 	= index_segd[key];
					state.index.frags.push({from: prev_segd.from, length: prev_segd.length});
					state.storage.frags.push({from: prev_index.from, length: prev_index.length});

					// INFO: Update segd
					const segd = Buffer.alloc(1);
					segd.writeUInt8(DATA_IS_UNAVAILABLE, 0);
					fs.writeSync( index_segd_fd, segd, 0, 1, prev_segd.segd_pos + SEGMENT_DESCRIPTOR_LENGTH - 1 );
				}



				const data_raw 	= Buffer.from(JSON.stringify(val) + '\n', 'utf8');
				const new_index = [key, state.storage.size, data_raw.length];
				const index_raw = Buffer.from(JSON.stringify(new_index) + '\n', 'utf8');


				// INFO: Write storage
				fs.appendFileSync(storage_fd, data_raw);
				state.storage.size += data_raw.length;

				// INFO: Write index
				fs.appendFileSync(index_fd, index_raw);
				state.index.size += index_raw.length;

				// INFO: Write index segment descriptor
				const segd_size = fs.fstatSync(index_segd_fd).size;
				const segd = Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH);
				segd.writeDoubleLE(state.index.size, 0);
				segd.writeUInt8(DATA_IS_AVAILABLE, SEGMENT_DESCRIPTOR_LENGTH - 1);
				fs.writeSync(index_segd_fd, segd, 0, SEGMENT_DESCRIPTOR_LENGTH, segd_size);



				// INFO: Update index
				index[key] = {from: new_index[1], length: new_index[2]};
				index_segd[key] = {from: state.index.size, length: index_raw.length, segd_pos: segd_size - SEGMENT_DESCRIPTOR_LENGTH};
			}



			// INFO: Update state
			state.total_records = Object.keys(index).length;
			fs.writeFileSync(state_path, JSON.stringify(state));
		}
		
		async del(keys=[]) {
			const {index_segd_fd, index_segd, index, state, state_path, valid} = _LevelKV.get(this);
			if( !valid ) throw new Error( 'Database is not available !' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }

			for( let key of keys ) {
				if ( index[key] ) {
					state.storage.frags.push(index[key]);
					state.index.frags.push(index_segd[key]);
					delete index[key];

					// INFO: Update segd
					const prev_segd = index_segd[key];
					const segd = Buffer.alloc(1);
					segd.writeUInt8(DATA_IS_UNAVAILABLE, 0);
					fs.writeSync( index_segd_fd, segd, 0, 1, prev_segd.segd_pos + SEGMENT_DESCRIPTOR_LENGTH - 1 );
				}
			}


			// INFO: Update state
			state.total_records = Object.keys(index).length;
			fs.writeFileSync(state_path, JSON.stringify(state));
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
						fs.writeFileSync(PROPS.state_path, JSON.stringify(PROPS.state));
					}
					catch(e) {
						throw new Error(`Cannot write database state! (${PROPS.state_path})`);
					}
				}
			}
			// endregion

			// region [ Read DB Index ]
			PROPS.index_path 		= `${DB_PATH}/index.jlst`;
			PROPS.index_segd_path 	= `${DB_PATH}/index.segd`;
			PROPS.index_segd 		= {};
			try {
				PROPS.index_fd 		= fs.openSync( PROPS.index_path, "a+" );
				PROPS.index_segd_fd = fs.openSync( PROPS.index_segd_path, "r+" );

				const { index, index_segd } =  ___READ_INDEX( PROPS.index_segd_fd, PROPS.index_fd );
				PROPS.index 		= index;
				PROPS.index_segd 	= index_segd;
			}
			catch(e) {
				PROPS.index = {};
				PROPS.index_segd = {};

				try {
					___WRITE_IDNEX_SEGD(PROPS.index_segd_path).then((index_segd_fd)=>{
						PROPS.index_segd_fd = index_segd_fd;
					});
				}
				catch(e) {
					throw new Error(`Cannot write database main index! (${PROPS.index_path})`);
				}
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
		const segd_size = fs.fstatSync(segd_fd).size;
		let rLen, buff 	= Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH), segd_pos = 0, prev = null;

		const r_index = {};
		const r_index_segd = {};

		// INFO: Add the first index position.
		if( !segd_size ){
			const segd = Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH);
			segd.writeDoubleLE(0, 0);
			segd.writeUInt8(DATA_IS_AVAILABLE, SEGMENT_DESCRIPTOR_LENGTH - 1);
			fs.writeSync(segd_fd, segd, 0, SEGMENT_DESCRIPTOR_LENGTH, 0);
		}



		while(segd_pos < segd_size) {
			rLen = fs.readSync(segd_fd, buff, 0, SEGMENT_DESCRIPTOR_LENGTH, segd_pos);
			if ( rLen !== SEGMENT_DESCRIPTOR_LENGTH ) {
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


				let index_str = raw_index.toString();
				let { 0:key, 1:position, 2:len } = JSON.parse( index_str.slice(0, index_str.length - 1) );

				r_index[key] 		= {from:position, 	length:len};
				r_index_segd[key] 	= {from:pos, 		length:length, segd_pos: segd_pos - SEGMENT_DESCRIPTOR_LENGTH};
			}



			let tmp = prev;
			prev = buff;
			buff = tmp;

			segd_pos += SEGMENT_DESCRIPTOR_LENGTH;
		}


		return {index: r_index, index_segd: r_index_segd};
	}
	async function ___WRITE_IDNEX_SEGD(index_segd_path){
		let segd = Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH);
		segd.writeDoubleLE(0, 0);
		segd.writeUInt8(DATA_IS_AVAILABLE, SEGMENT_DESCRIPTOR_LENGTH - 1);
		fs.appendFileSync(index_segd_path, segd);
		return fs.openSync( index_segd_path, "r+" );
	}
	function ___GEN_DEFAULT_STATE() {
		return {
			version:1, total_records:0,
			index:{ size:0, frags:[] },
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

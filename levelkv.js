/**
 * Project: levelkv
 * File: levelkv.js
 * Author: JCloudYu
 * Create Date: Aug. 31, 2018 
 */
(()=>{
	"use strict";
	
	const path			= require( 'path' );
	const {RAStorage} 	= require( 'rastorage' );
	const {Binary, Serialize, Deserialize} 	= require('beson');


	
	const _LevelKV  	= new WeakMap();
	const _DBCursor 	= new WeakMap();
	const _RAStorage 	= new WeakMap();

	const DEFAULT_PROCESSOR = {
		serializer: (input)=>{ return Serialize(input); },
		deserializer: (input)=>{ return Deserialize(input); }
	};

	const INDEX_BLOCK = 1;



	class DBCursor {
		constructor(db, segments) {
			const PROPS = {
				db: db,
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
		get size() 		{ const {segments} = _DBCursor.get(this); return segments.length; }
		get length() 	{ const {segments} = _DBCursor.get(this); return segments.length; }
		next() {
			const { db, segments } = _DBCursor.get(this);
			const {RAS_DATA} = _RAStorage.get(db);
			if ( segments.length > 0 ) {
				let {key, dataId, in_memory, value} = segments.shift();
				if( in_memory === true )
				{
					return { value: new Promise((resolve, reject)=>{
						resolve(value);
					}) };
				}

				return {value:new Promise(async(resolve, reject)=>{
					const result = await RAS_DATA.get(dataId);
					resolve( result );
				})};
			}
			else {
				return {done:true};
			}
		}
		[Symbol.iterator](){ return this; }
	}
	class DBMutableCursor extends DBCursor {
		constructor(dbCursor) {
			if( !(dbCursor instanceof DBCursor) ) throw new Error(`The parameter should be a DBCursor!`);

			const {db, segments} = _DBCursor.get(dbCursor);
			super( db, Deserialize( Serialize(segments) ) );
			_DBCursor.get(dbCursor).segments = [];
		}
		get segments() { const {segments} = _DBCursor.get(this); return segments; }
	}
	class LevelKV {
		constructor() {
			const PROPS = {};
			_LevelKV.set(this, PROPS);

			PROPS.valid = false;
		}

		/**
		 * Close the database.
		 *
		 * @async
		 */
		async close() {
			_LevelKV.get(this).valid = false;
			const {RAS_INDEX, RAS_DATA} = _RAStorage.get(this);

			try {
				await RAS_INDEX.close();
				await RAS_DATA.close();
			}
			catch(e)
			{
				throw new Error( `An error occurs when closing the database! (${e})` );
			}
		}

		/**
		 * Get data from the database.
		 *
		 * @param {string|string[]} keys - A specific key or an array of keys to retrieve, if not given it will retrieve all data from the database.
		 * @returns {DBCursor} - Database cursor of the retrieved data.
		 */
		get(keys=[]) {
			const {index, valid} = _LevelKV.get(this);
			if( !valid ) throw new Error( 'Database is not available!' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }
			if( !keys.length ) keys = Object.keys(index);

			const matches = [];
			for( let key of keys ) {
				// INFO: Cast typical data type to string
				if( key instanceof Binary ) key = key.toString();
				if( key instanceof ArrayBuffer ) key = Binary.from(key).toString();

				if ( index[key] ) matches.push(index[key]);
			}
			return new DBCursor(this, matches);
		}

		/**
		 * Add data to the database.
		 *
		 * @async
		 * @param {string|string[]} keys - A specific key or an array of keys to add.
		 * @param {*} val - The value to add.
		 */
		async put(keys=[], val) {
			const {index, valid} = _LevelKV.get(this);
			const {RAS_INDEX, RAS_DATA} = _RAStorage.get(this);
			if( !valid ) throw new Error( 'Database is not available!' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }

			try {
				for( let key of keys ) {
					// INFO: Cast typical data type to string
					if( key instanceof Binary ) key = key.toString();
					if( key instanceof ArrayBuffer ) key = Binary.from(key).toString();

					// INFO: Mark the duplicate key
					const prev_index = index[key];
					if ( prev_index ) {
						await RAS_DATA.set( prev_index.dataId, val );
					}
					else
					{
						const dataId 	= await RAS_DATA.put(val);
						index[key] 		= {key, dataId};
					}
				}
			}
			catch(e)
			{
				throw new Error( `Cannot put data! (${e})` );
			}



			await RAS_INDEX.set( INDEX_BLOCK, index );
		}

		/**
		 * Delete data from the database.
		 *
		 * @async
		 * @param {string|string[]} keys -  A specific key or an array of keys to delete.
		 */
		async del(keys=[]) {
			const {index, valid} = _LevelKV.get(this);
			const {RAS_INDEX, RAS_DATA} = _RAStorage.get(this);
			if( !valid ) throw new Error( 'Database is not available !' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }

			try {
				for( let key of keys ) {
					// INFO: Cast typical data type to string
					if( key instanceof Binary ) key = key.toString();
					if( key instanceof ArrayBuffer ) key = Binary.from(key).toString();

					const prev_index 	= index[key];
					if ( prev_index ) {
						delete index[key];
						await RAS_DATA.del(prev_index.dataId);
					}
				}
			}
			catch(e)
			{
				throw new Error( `Cannot delete data! (${e})` );
			}



			await RAS_INDEX.set( INDEX_BLOCK, index );
		}

		/**
		 * Initialize the database.
		 *
		 * @async
		 * @param {string} dir - Directory path of the database. Make sure you have created or it will fail if the directory does not exist.
		 * @param {object} options - Database creating options. Defaults to {auto_create:true}, which means create a new database automatically if not exist.
		 * @returns {Promise<LevelKV>} - Promise object represents the database itself.
		 */
		static async initFromPath(dir, options={auto_create:true}) {
			const DB_PATH 	= path.resolve(dir);
			const DB 		= new LevelKV();
			const PROPS		= _LevelKV.get(DB);
			let RAS_INDEX, RAS_DATA;



			// region [ Prepare DB Index ]
			PROPS.index_path = `${DB_PATH}/index`;
			try {
				RAS_INDEX 				= await RAStorage.InitAtPath( PROPS.index_path );
				RAS_INDEX._serializer 	= DEFAULT_PROCESSOR.serializer;
				RAS_INDEX._deserializer = DEFAULT_PROCESSOR.deserializer;
			}
			catch(e) {
				throw new Error( `Cannot access database main index! (${e})` );
			}
			// endregion

			// region [ Prepare DB Storage ]
			PROPS.storage_path = `${DB_PATH}/storage`;
			try {
				RAS_DATA 				= await RAStorage.InitAtPath( PROPS.storage_path );
				RAS_DATA._serializer 	= DEFAULT_PROCESSOR.serializer;
				RAS_DATA._deserializer 	= DEFAULT_PROCESSOR.deserializer;
			}
			catch(e) {
				throw new Error( `Cannot access database storage! (${e})` );
			}
			// endregion



			// region [ Read DB Index ]
			try {
				let index = await RAS_INDEX.get( INDEX_BLOCK );
				PROPS.index = index;

				if( !index ) {
					PROPS.index = {};
					await RAS_INDEX.put( PROPS.index );
				}
			}
			catch(e) {
				throw new Error(`Cannot read database main index! (${e})`);
			}
			// endregion



			_RAStorage.set(DB, { RAS_INDEX, RAS_DATA });
			PROPS.valid = true;
			return DB;
		}
	}
	
	module.exports = { LevelKV, DBMutableCursor };



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

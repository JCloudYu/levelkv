/**
 * Project: levelkv
 * File: levelkv.js
 * Author: JCloudYu
 * Create Date: Aug. 31, 2018 
 */
(()=>{
	"use strict";
	
	const fs		= require( 'fs' );
	const path		= require( 'path' );
	const promisefy = require( './lib/promisefy' );
	const {RAStorage} = require( 'rastorage' );
	const {Binary, Serialize, Deserialize} 	= require('beson');


	
	const _LevelKV  	= new WeakMap();
	const _DBCursor 	= new WeakMap();
	const _RAStorage 	= new WeakMap();

	const SEGD_TIMEOUT 	= ___UNIQUE_TIMEOUT();

	const DEFAULT_PROCESSOR = {
		serializer: (input)=>{ return Serialize(input); },
		deserializer: (input)=>{ return Deserialize(input); }
	};


	const SEGMENT_DESCRIPTOR_LENGTH = 4;



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
			const props = _LevelKV.get(this);
			const {RAS_INDEX, RAS_DATA} = _RAStorage.get(this);
			props.valid = false;

			try {
				await promisefy( fs.close, fs, props.index_segd_fd );
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
			const {index_segd_fd, index_segd, index, valid} = _LevelKV.get(this);
			const {RAS_INDEX, RAS_DATA} = _RAStorage.get(this);
			if( !valid ) throw new Error( 'Database is not available!' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }

			try {
				for( let key of keys ) {
					// INFO: Cast typical data type to string
					if( key instanceof Binary ) key = key.toString();
					if( key instanceof ArrayBuffer ) key = Binary.from(key).toString();

					const prev_index 	= index[key];

					// INFO: Mark the duplicate key
					if ( prev_index ) {
						await RAS_DATA.set( prev_index.dataId, val );
					}
					else
					{
						const dataId 	= await RAS_DATA.put(val);
						const new_index = [key, dataId];
						const indexId 	= await RAS_INDEX.put(new_index);

						index[key] 		= {key, dataId};
						index_segd[key] = {indexId};
					}
				}
			}
			catch(e)
			{
				throw new Error( `Cannot put data! (${e})` );
			}



			await ___UPDATE_SEGD( index_segd_fd, index_segd, Object.keys(index).length );
		}

		/**
		 * Delete data from the database.
		 *
		 * @async
		 * @param {string|string[]} keys -  A specific key or an array of keys to delete.
		 */
		async del(keys=[]) {
			const {index_segd_fd, index, index_segd, valid} = _LevelKV.get(this);
			const {RAS_INDEX, RAS_DATA} = _RAStorage.get(this);
			if( !valid ) throw new Error( 'Database is not available !' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }

			try {
				for( let key of keys ) {
					// INFO: Cast typical data type to string
					if( key instanceof Binary ) key = key.toString();
					if( key instanceof ArrayBuffer ) key = Binary.from(key).toString();

					const prev_index 	= index[key];
					const prev_segd 	= index_segd[key];
					if ( prev_index ) {
						delete index[key];
						delete index_segd[key];
						await RAS_DATA.del(prev_index.dataId);
						await RAS_INDEX.del(prev_segd.indexId);
					}
				}
			}
			catch(e)
			{
				throw new Error( `Cannot delete data! (${e})` );
			}



			await ___UPDATE_SEGD( index_segd_fd, index_segd, Object.keys(index).length );
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
			
			await ___CREATE_DIR(DB_PATH);



			// region [ Prepare DB Initialization ]
			PROPS.index_path 		= `${DB_PATH}/index`;
			PROPS.storage_path 		= `${DB_PATH}/storage`;
			let RAS_INDEX, RAS_DATA;
			try {
				RAS_INDEX 	= await RAStorage.InitAtPath( PROPS.index_path );
				RAS_DATA 	= await RAStorage.InitAtPath( PROPS.storage_path );

				RAS_INDEX._serializer 	= DEFAULT_PROCESSOR.serializer;
				RAS_INDEX._deserializer = DEFAULT_PROCESSOR.deserializer;

				RAS_DATA._serializer 	= DEFAULT_PROCESSOR.serializer;
				RAS_DATA._deserializer 	= DEFAULT_PROCESSOR.deserializer;

				_RAStorage.set(DB, { RAS_INDEX, RAS_DATA });
			}
			catch(e) {
				throw new Error( `Cannot initialize index and storage database! (${e})` );
			}
			// endregion


			// region [ Read DB Index ]
			PROPS.index_segd_path 	= `${DB_PATH}/index.segd`;
			try {
				[PROPS.index_segd_fd] = await promisefy( fs.open, fs, PROPS.index_segd_path, "r+" );
				[PROPS.index, PROPS.index_segd] =  await ___READ_INDEX( PROPS.index_segd_fd, RAS_INDEX );
			}
			catch(e) {
				PROPS.index 		= {};
				PROPS.index_segd 	= {};

				try {
					[PROPS.index_segd_fd] = await ___OPEN_NEW_FILE(PROPS.index_segd_path);
				}
				catch(e) {
					throw new Error(`Cannot read database main index! (${e})`);
				}
			}
			// endregion



			PROPS.valid = true;
			return DB;
		}
	}
	
	module.exports = { LevelKV, DBMutableCursor };
	


	async function ___UPDATE_SEGD(segd_fd, index_segd, total_records) {
		await promisefy( fs.ftruncate, fs, segd_fd, total_records * SEGMENT_DESCRIPTOR_LENGTH );



		let segd_pos = 0;
		const buff = Buffer.alloc( SEGMENT_DESCRIPTOR_LENGTH );
		for( const segd in index_segd ) {
			if( !index_segd.hasOwnProperty( segd ) ) continue;

			buff.writeUInt32LE( index_segd[segd].indexId, 0 );
			await promisefy( fs.write, fs, segd_fd, buff, 0, buff.length, segd_pos );
			segd_pos += SEGMENT_DESCRIPTOR_LENGTH;
		}
	}

	function ___UNIQUE_TIMEOUT() {
		let hTimeout = null;
		return (...args)=>{
			if( hTimeout !== null ) {
				try { clearTimeout( hTimeout ); } catch(e) {}
			}
			return (hTimeout = setTimeout( ...args ));
		}
	}

	async function ___READ_INDEX(segd_fd, ras_index) {
		const [stats] = await promisefy( fs.fstat, fs, segd_fd );
		const segd_size = stats.size;

		let rLen, buff 	= Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH), segd_pos = 0;
		const index = {};
		const index_segd = {};

		while(segd_pos < segd_size) {
			[rLen] = await promisefy( fs.read, fs, segd_fd, buff, 0, SEGMENT_DESCRIPTOR_LENGTH, segd_pos );
			if ( rLen !== SEGMENT_DESCRIPTOR_LENGTH ) {
				throw "Insufficient data in index segmentation descriptor!";
			}

			const indexId 		= buff.readUInt32LE( 0 );
			const [key, dataId] = await ras_index.get( indexId );


			index[key] 		= {key, dataId};
			index_segd[key] = {indexId};

			segd_pos += SEGMENT_DESCRIPTOR_LENGTH;
		}


		return [index, index_segd];
	}

	async function ___OPEN_NEW_FILE(path) {
		const [fd] = await promisefy( fs.open, fs, path, "a+" );
		await promisefy( fs.close, fs, fd );
		return await promisefy( fs.open, fs, path, "r+" );
	}
	async function ___CREATE_DIR(dir) {
		try {
			await promisefy( fs.mkdir, fs, dir, { recursive: true } );
		}
		catch(e) {
			throw new Error( `Cannot create the levelkv directory! (${e})` );
		}
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

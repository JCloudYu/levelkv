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
	
	
	
	const _LevelKV  = new WeakMap();
	const _DBCursor = new WeakMap();

	const SEGMENT_DESCRIPTOR_LENGTH = 9;
	const DATA_IS_AVAILABLE 		= 0x01;
	const DATA_IS_UNAVAILABLE 		= 0x00;

	let _clearId = null;

	class DBCursor {
		constructor(db, segments) {
			const PROPS = {
				db: (this.constructor.name === 'DBCursor') ? _LevelKV.get(db): db,
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
			const { db:{storage_fd}, segments } = _DBCursor.get(this);
			if ( segments.length > 0 ) {
				let {key, from, length, in_memory, value} = segments.shift();
				if( in_memory === true )
				{
					return { value: new Promise((resolve, reject)=>{
						resolve(value);
					}) };
				}
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
	class DBMutableCursor extends DBCursor {
		constructor(dbCursor) {
			if( !(dbCursor instanceof DBCursor) ) throw new Error(`The parameter should be a DBCursor!`);

			const {db, segments} = JSON.parse( JSON.stringify(_DBCursor.get(dbCursor)) );
			super( db, segments );
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
			props.valid = false;
			___CLEAR_REORGANIZE_FILES();

			try {
				await promisefy( fs.close, fs, props.index_segd_fd );
				await promisefy( fs.close, fs, props.index_fd );
				await promisefy( fs.close, fs, props.storage_fd );
			}
			catch(e)
			{
				throw new Error( `An error occurs when closing the database! (${e})` );
			}
		}

		/**
		 * Get data from the database.
		 *
		 * @async
		 * @param {string|string[]} keys - A specific key or an array of keys to retrieve, if not given it will retrieve all data from the database.
		 * @returns {Promise<DBCursor>} - Promise object represents the database cursor of the retrieved data.
		 */
		async get(keys=[]) {
			const {index, valid} = _LevelKV.get(this);
			if( !valid ) throw new Error( 'Database is not available!' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }
			if( !keys.length ) keys = Object.keys(index);

			const matches = [];
			for( let key of keys ) {
				if ( index[key] ) {
					const data = index[key];
					data.key = key;
					matches.push(index[key]);
				}
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
			const {storage_fd, index_fd, index_segd_fd, index_segd, index, state, state_path, valid} = _LevelKV.get(this);
			if( !valid ) throw new Error( 'Database is not available!' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }

			try {
				for( let key of keys ) {
					const prev_index 	= index[key];
					const prev_segd 	= index_segd[key];

					const data_raw 	= Buffer.from(JSON.stringify(val) + '\n', 'utf8');
					const new_index = [key, state.storage.size, data_raw.length];
					const index_raw = Buffer.from(JSON.stringify(new_index) + '\n', 'utf8');

					const segd_size = state.segd.size;
					const segd 		= Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH);

					const storage_pos 	= state.storage.size;
					const index_pos 	= state.index.size;


					state.storage.size += data_raw.length;
					state.index.size += index_raw.length;

					state.segd.size += SEGMENT_DESCRIPTOR_LENGTH;
					segd.writeDoubleLE(state.index.size, 0);
					segd.writeUInt8(DATA_IS_AVAILABLE, SEGMENT_DESCRIPTOR_LENGTH - 1);


					// INFO: Update index
					index[key] = {from: new_index[1], length: new_index[2]};
					index_segd[key] = {from: state.index.size, length: index_raw.length, segd_pos: segd_size - SEGMENT_DESCRIPTOR_LENGTH};


					// INFO: Mark the duplicate key
					if ( prev_index ) {
						state.index.frags.push({from: prev_segd.from, length: prev_segd.length});
						state.storage.frags.push({from: prev_index.from, length: prev_index.length});

						// INFO: Update segd
						const segd = Buffer.alloc(1);
						segd.writeUInt8(DATA_IS_UNAVAILABLE, 0);

						await promisefy( fs.write, fs, index_segd_fd, segd, 0, 1, prev_segd.segd_pos + SEGMENT_DESCRIPTOR_LENGTH - 1 );
					}


					// INFO: Write storage, index, and index segment descriptor
					await promisefy( fs.write, fs, storage_fd, data_raw, 0, data_raw.length, storage_pos );
					await promisefy( fs.write, fs, index_fd, index_raw, 0, index_raw.length, index_pos );
					await promisefy( fs.write, fs, index_segd_fd, segd, 0, SEGMENT_DESCRIPTOR_LENGTH, segd_size );
				}



				// INFO: Update state
				state.total_records = Object.keys(index).length;
				await promisefy( fs.writeFile, fs, state_path, JSON.stringify(state) );
			}
			catch(e)
			{
				throw new Error( `Cannot put data! (${e})` );
			}
		}

		/**
		 * Delete data from the database.
		 *
		 * @async
		 * @param {string|string[]} keys -  A specific key or an array of keys to delete.
		 */
		async del(keys=[]) {
			const {index_segd_fd, index_segd, index, state, state_path, valid} = _LevelKV.get(this);
			if( !valid ) throw new Error( 'Database is not available !' );


			if ( !Array.isArray(keys) ) { keys = [keys]; }

			try {
				for( let key of keys ) {
					const prev_index 	= index[key];
					const prev_segd 	= index_segd[key];
					if ( prev_index ) {
						state.storage.frags.push({from: prev_index.from, length: prev_index.length});
						state.index.frags.push({from: prev_index.from, length: prev_index.length});
						delete index[key];

						// INFO: Update segd
						const segd = Buffer.alloc(1);
						segd.writeUInt8(DATA_IS_UNAVAILABLE, 0);
						await promisefy( fs.write, fs, index_segd_fd, segd, 0, 1, prev_segd.segd_pos + SEGMENT_DESCRIPTOR_LENGTH - 1 );
					}
				}


				// INFO: Update state
				state.total_records = Object.keys(index).length;
				await promisefy( fs.writeFile, fs, state_path, JSON.stringify(state) );
			}
			catch(e)
			{
				throw new Error( `Cannot delete data! (${e})` );
			}
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
			const DB_PATH = path.resolve(dir);
			const DB = new LevelKV();
			const PROPS	= _LevelKV.get(DB);
			
			
			
			// region [ Read DB States ]
			PROPS.state_path = `${DB_PATH}/state.json`;
			try {
				const [content] = await promisefy( fs.readFile, fs, PROPS.state_path );
				PROPS.state = JSON.parse( content );
			}
			catch(e) {
				if ( !options.auto_create ) {
					throw new Error(`Cannot read database state! (${PROPS.state_path})`);
				}
				else {
					PROPS.state = ___GEN_DEFAULT_STATE();
					try {
						await promisefy( fs.writeFile, fs, PROPS.state_path, JSON.stringify(PROPS.state) );
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
				[PROPS.index_fd] 		= await promisefy( fs.open, fs, PROPS.index_path, "r+" );
				[PROPS.index_segd_fd] 	= await promisefy( fs.open, fs, PROPS.index_segd_path, "r+" );

				const { index, index_segd } =  await ___READ_INDEX( PROPS.index_segd_fd, PROPS.index_fd );
				PROPS.index 		= index;
				PROPS.index_segd 	= index_segd;
			}
			catch(e) {
				PROPS.index = {};
				PROPS.index_segd = {};

				try {
					[PROPS.index_fd] 		= await ___OPEN_NEW_FILE( PROPS.index_path );
					[PROPS.index_segd_fd] 	= await ___WRITE_IDNEX_SEGD(PROPS.index_segd_path);

					const [stats] = await promisefy( fs.fstat, fs, PROPS.index_segd_fd );
					PROPS.state.segd.size = stats.size;

					await promisefy( fs.writeFile, fs, PROPS.state_path, JSON.stringify( PROPS.state ) );
				}
				catch(e) {
					throw new Error(`Cannot write database main index! (${PROPS.index_path})`);
				}
			}
			// endregion

			// region [ Prepare DB Storage ]
			PROPS.storage_path = `${DB_PATH}/storage.jlst`;
			try {
				[PROPS.storage_fd] = await promisefy( fs.open, PROPS.storage_path, "r+" );
			}
			catch(e) {
				try {
					[PROPS.storage_fd] = await ___OPEN_NEW_FILE( PROPS.storage_path );
				}
				catch(e) {
					throw new Error( `Cannot access database storage! (${PROPS.storage_path})` );
				}
			}
			// endregion
			
			

			PROPS.valid = true;
			// INFO: Start to reorganize files
			await ___REORGANIZE_FILES(PROPS);

			return DB;
		}
	}
	
	module.exports = { LevelKV, DBMutableCursor };
	
	
	
	async function ___READ_INDEX(segd_fd, index_fd) {
		const [stats] = await promisefy( fs.fstat, fs, segd_fd );
		const segd_size = stats.size;
		let rLen, buff 	= Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH), segd_pos = 0, prev = null;

		const r_index = {};
		const r_index_segd = {};


		while(segd_pos < segd_size) {
			[rLen] = await promisefy( fs.read, fs, segd_fd, buff, 0, SEGMENT_DESCRIPTOR_LENGTH, segd_pos );
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
				[rLen] = await promisefy( fs.read, fs, index_fd, raw_index, 0, length, pos );
				if ( rLen !== length ) {
					throw "Insufficient data in index!";
				}


				let index_str = raw_index.toString();
				let [key, position, len] = JSON.parse( index_str.slice(0, index_str.length - 1) );

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

	async function ___UPDATE_INDEX(segd_fd, index_fd) {
		const [stats] = await promisefy( fs.fstat, fs, segd_fd );
		const segd_size = stats.size;

		let rLen, buff 	= Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH), segd_pos = 0, prev = null;

		const r_index 		= {};
		const r_index_segd 	= {};
		const remains 		= { segd: [], index:[] };
		const removes 		= { segd: [], index: [], storage: [] };
		const shift 		= { segd: 0, index: 0, storage: 0 };


		while(segd_pos < segd_size) {
			[rLen] = await promisefy( fs.read, fs, segd_fd, buff, 0, SEGMENT_DESCRIPTOR_LENGTH, segd_pos );

			if ( rLen !== SEGMENT_DESCRIPTOR_LENGTH ) {
				throw "Insufficient data in index segmentation descriptor!";
			}

			if ( !prev ) {
				prev = Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH);
			}
			else
			{
				let pos 		= prev.readDoubleLE(0);
				let length 		= buff.readDoubleLE(0) - pos;
				let raw_index 	= Buffer.alloc(length);
				[rLen] = await promisefy( fs.read, fs, index_fd, raw_index, 0, length, pos );
				if ( rLen !== length ) {
					throw "Insufficient data in index!";
				}


				let index_str = raw_index.toString();
				let [key, position, len] = JSON.parse( index_str.slice(0, index_str.length - 1) );
				let new_prev, new_buff;

				if ( prev[SEGMENT_DESCRIPTOR_LENGTH - 1] ) {
					const new_index = Buffer.from( JSON.stringify( [key, position + shift.storage, len] ) + '\n', 'utf8' );
					const new_index_length = new_index.length;

					r_index[key] 		= {from: position + shift.storage, 	length:len};
					r_index_segd[key] 	= {from: pos + shift.index, 		length:new_index.length, segd_pos: segd_pos - SEGMENT_DESCRIPTOR_LENGTH + shift.segd};

					new_prev = ___GEN_SEGD( pos + shift.index, DATA_IS_AVAILABLE );
					new_buff = ___GEN_SEGD( pos + shift.index + new_index.length, DATA_IS_AVAILABLE );

					remains.segd.push( new_prev );
					remains.index.push( new_index );
					shift.index -= length - new_index_length;

				}
				else {
					removes.storage.push({
						from: 	position,
						length: len
					});
					shift.segd 		-= SEGMENT_DESCRIPTOR_LENGTH;
					shift.index 	-= length;
					shift.storage 	-= len;
				}

				if( ( new_buff && (segd_pos + SEGMENT_DESCRIPTOR_LENGTH === segd_size )) ){
					remains.segd.push(new_buff);
				}
			}



			let tmp = prev;
			prev = buff;
			buff = tmp;

			segd_pos += SEGMENT_DESCRIPTOR_LENGTH;
		}

		if( segd_pos === SEGMENT_DESCRIPTOR_LENGTH || (-shift.segd === segd_size - SEGMENT_DESCRIPTOR_LENGTH) ) {
			remains.segd.unshift( ___GEN_SEGD(0, DATA_IS_AVAILABLE) );
		}



		await promisefy( fs.ftruncate, fs, segd_fd );
		await promisefy( fs.ftruncate, fs, index_fd );
		await promisefy( fs.writeFile, fs, segd_fd, Buffer.concat( remains.segd ) );
		await promisefy( fs.writeFile, fs, index_fd, Buffer.concat( remains.index ) );

		return {index: r_index, index_segd: r_index_segd, removes};
	}
	async function ___OPEN_NEW_FILE(path) {
		const [fd] = await promisefy( fs.open, fs, path, "a+" );
		await promisefy( fs.close, fs, fd );
		return await promisefy( fs.open, fs, path, "r+" );
	}
	async function ___WRITE_IDNEX_SEGD(index_segd_path) {
		let segd = Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH);
		segd.writeDoubleLE(0, 0);
		segd.writeUInt8(DATA_IS_AVAILABLE, SEGMENT_DESCRIPTOR_LENGTH - 1);
		await promisefy( fs.appendFile, fs, index_segd_path, segd );
		return await promisefy( fs.open, fs, index_segd_path, "r+" );
	}
	function ___GEN_DEFAULT_STATE() {
		return {
			version:1, total_records:0,
			segd: { size:0 },
			index:{ size:0, frags:[] },
			storage:{ size:0, frags:[] }
		};
	}
	function ___GEN_SEGD(position, status){
		const segd = Buffer.alloc(SEGMENT_DESCRIPTOR_LENGTH);
		segd.writeDoubleLE(position, 0);
		segd.writeUInt8(status, SEGMENT_DESCRIPTOR_LENGTH - 1);
		return segd;
	}

	async function ___REMOVE_CONTENT(fd, frags){
		const [content] = await promisefy( fs.readFile, fs, fd );
		let frag = frags.shift();
		let start = 0;
		let data = [];
		while( frag ){
			const {from, length} = frag;
			data.push( content.slice(start, from) );

			start = from + length;
			frag = frags.shift();
		}
		data.push( content.slice( start ) );

		let result = Buffer.concat(data);
		await promisefy( fs.ftruncate, fs, fd );
		await promisefy( fs.writeFile, fs, fd, result );
	}
	async function ___REORGANIZE_FILES(props){
		const {state, state_path, index_segd_fd, index_fd, storage_fd} = props;
		const {index, index_segd, removes} =  await ___UPDATE_INDEX( index_segd_fd, index_fd );
		await ___REMOVE_CONTENT(storage_fd, removes.storage);

		props.index = index;
		props.index_segd = index_segd;

		state.total_records = Object.keys(index).length;
		let [stats] = await promisefy( fs.fstat, fs, index_segd_fd );
		state.segd.size = stats.size;

		[stats] = await promisefy( fs.fstat, fs, index_fd );
		state.index.size = stats.size;

		[stats] = await promisefy( fs.fstat, fs, storage_fd );
		state.storage.size = stats.size;

		await promisefy( fs.writeFile, fs, state_path, JSON.stringify(state) );



		_clearId = setTimeout(
			___REORGANIZE_FILES.bind(null, props), 30000
		);
	}
	function ___CLEAR_REORGANIZE_FILES(){
		clearTimeout( _clearId );
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

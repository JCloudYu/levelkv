/**
 * Project: levelkv
 * File: levelkv.js
 * Author: JCloudYu
 * Create Date: Aug. 31, 2018 
 */
(()=>{
	"use strict";
	
	const DEFAULT_JSON_DB = require( './levelkv-json' );
	const DEFAULT_BSON_DB = require( './levelkv-bson' );
	
	
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

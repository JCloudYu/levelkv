/**
 * Project: beson
 * File: index.js
 * Author: JCloudYu
 * Create Date: Sep. 03, 2018
 */
(()=>{
	"use strict";
	
	const util	 = require( 'util' );
	const crypto = require( 'crypto' );
	
	// See http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param for the definition of these parameters;
	const FNV_PRIME_HIGH = 0x0100, FNV_PRIME_LOW = 0x0193;			// 16777619 0x01000193
	const OFFSET_BASIS	= Buffer.from([0xC5, 0x9D, 0x1C, 0x81]);	// 2166136261 [0x81, 0x1C, 0x9D, 0xC5]
	const FNV1A32 = ___FNV1A32.bind(null, Buffer.alloc(4));
	const FNV1A24 = ___FNV1A24.bind(null, Buffer.alloc(4));
	
	const HOST_NAME		= require('os').hostname;
	const MACHINE_ID	= FNV1A24(HOST_NAME);
	const HEX_FMT_CHECK	= /^[0-9a-fA-F]{24}$/;
	
	
	
	
	
	
	
	
	let SEQ_NUMBER = (crypto.randomBytes(4).readUInt32BE(0) & 0xFFFFFF);
	const _PROPS = new WeakMap();
	class ObjectId {
		constructor(id=null) {
			const PROPS = {};
			_PROPS.set(this, PROPS);
			
			
			
			// region [ Default Constructor ]
			if ( id === null || typeof id === 'number' ) {
				PROPS._rawId	= ___GEN_OBJECT_ID(id);
				PROPS._hexId	= PROPS._rawId.toString( 'hex' );
				PROPS._b64urlId = PROPS._rawId.toString( 'base64' ).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
				return;
			}
			// endregion
			
			// region [ Copy Constructor ]
			if ( id instanceof ObjectId ) {
				const OTHER = _PROPS.get(id);
				PROPS._rawId	= Buffer.from(OTHER._rawId);
				PROPS._hexId	= OTHER._hexId;
				PROPS._b64urlId = OTHER._b64urlId;
				return;
			}
			// endregion
			
			// region [ Cast other type or error... ]
			const RAW_ID = ___CAST_OBJECT_ID(id);
			if ( RAW_ID === null ) {
				throw new TypeError(
					'Argument passed in must be either a buffer, a number, a date object, a binary string of 12 bytes or a hex string with 24 characters.'
				);
			}
			
			PROPS._rawId	= RAW_ID;
			PROPS._hexId	= PROPS._rawId.toString( 'hex' );
			PROPS._b64urlId = PROPS._rawId.toString( 'base64' ).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
			// endregion
		}
		toString(type='hex') {
			const PROPS = _PROPS.get(this);
			switch ( type ) {
				case "raw":
					return this.buffer;
					
				case "base64":
					return PROPS._rawId.toString('base64');
					
				case "base64url":
					return PROPS._b64urlId;
					
				case "hex":
				default:
					return PROPS._hexId;
			}
		}
		toJSON(type='hex') {
			return this.toString(type);
		}
		equals(other) {
			const type = typeof other;
			if ( other === null || type === "number" || other instanceof Date ) {
				return false;
			}
		
			const PROPS = _PROPS.get(this);
			if ( other instanceof ObjectId) {
				const OTHER = _PROPS.get(other);
				return (OTHER._rawId.compare(PROPS._rawId) === 0);
			}
			
			const OTHER = ___CAST_OBJECT_ID(other);
			if ( other === null ) { return false; }
			return (OTHER.compare(PROPS._rawId) === 0);
		}
		get time() {
			const PROPS = _PROPS.get(this);
			return _PROPS._rawId.readUInt32BE(0);
		}
		set time(time) {
			if ( time instanceof Date) {
				time = (time.getTime()/1000)|0;
			}
			
			if ( typeof time !== "number" ) {
				throw new TypeError(
					"time property accepts only a number or date object"
				);
			}
			
			const PROPS = _PROPS.get(this);
			PROPS._rawId.writeUInt32BE(time, 0);
		}
		get buffer() {
			const PROPS = _PROPS.get(this);
			return Buffer.from(PROPS._rawId);
		}
		[util.inspect.custom]() {
			return `ObjectID( ${this.toString('hex')} )`;
		}
	}


	module.exports = ObjectId;
	Object.assign(ObjectId, {
		fnv1a24: FNV1A24,
		fnv1a32: FNV1A32,
		GenObjectId:(id=null)=>{
			return new ObjectId(id);
		}
	});
	
	
	
	
	
	
	function ___FNV1A32(HASH_RESULT, input, encoding='utf8'){
		let octets = input;
		if ( !Buffer.isBuffer(input) ) {
			octets = Buffer.from(input, encoding);
		}
		
		OFFSET_BASIS.copy(HASH_RESULT);
		for( let i = 0; i < octets.length; i += 1 ) {
			HASH_RESULT[0] = (HASH_RESULT[0] ^ octets[i]) >>> 0;
			
			
			let hash_low	= HASH_RESULT.readUInt16LE(0);
			let hash_high	= HASH_RESULT.readUInt16LE(2);
			let new_low		= hash_low * FNV_PRIME_LOW;
			let new_high	= hash_low * FNV_PRIME_HIGH + hash_high * FNV_PRIME_LOW +  + (new_low>>>16);
			
			HASH_RESULT.writeUInt16LE((new_low  & 0xFFFF)>>>0, 0);
			HASH_RESULT.writeUInt16LE((new_high & 0xFFFF)>>>0, 2);
		}
		return HASH_RESULT.readUInt32LE(0);
	}
	
	function ___FNV1A24(HASH_RESULT, input, encoding){
		const _32bit = ___FNV1A32(HASH_RESULT, input, encoding);
		const base = _32bit & 0xFFFFFF;
		const top = (_32bit >>> 24) & 0xFF;
		return (base ^ top) & 0xFFFFFF;
	}
	function ___GEN_OBJECT_ID(init=null) {
		const time	= init || ((Date.now()/1000)|0);
		const pid	= process.pid % 0xFFFF;
		const inc	= (SEQ_NUMBER=(SEQ_NUMBER+1) % 0xffffff);



		const buffer = Buffer.alloc(12);
		// region [ Fill in the id information ]
		// Fill in the info in reversed order to prevent byte-wise assignments
		buffer.writeUInt32BE(inc, 8);			// [9-11] seq
		buffer.writeUInt16BE(pid, 7);			// [7-8] pid
		buffer.writeUInt32BE(MACHINE_ID, 3);	// [4-6] machine id
		buffer.writeUInt32BE(time, 0);			// [0-3] epoch time
		// endregion
		
		
		
		return buffer;
	}
	function ___CAST_OBJECT_ID(candidate) {
		if ( Buffer.isBuffer(candidate) && candidate.length === 12 ) {
			return Buffer.from(candidate.slice(0, 12));
		}
		
		const type = typeof candidate;
		if ( type === "string" ) {
			if ( candidate.length === 12 ) {
				return Buffer.from(candidate, 'binary');
			}
			else
			if ( candidate.length === 24 && HEX_FMT_CHECK.test(id) ) {
				return Buffer.from(candidate, 'hex');
			}
			
			return null;
		}
		
		if ( candidate instanceof Date ) { candidate = candidate.getTime(); }
		if ( candidate === null || type === "number" ) {
			return ___GEN_OBJECT_ID(candidate);
		}
	}
})();

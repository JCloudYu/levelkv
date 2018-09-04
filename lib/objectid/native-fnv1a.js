/**
 * Project: beson
 * File: fnv1a.js
 * Author: JCloudYu
 * Create Date: Sep. 03, 2018
 */
(()=>{
	"use strict";
	
	// See http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param for the definition of these parameters;
	const FNV_PRIME_HIGH = 0x0100, FNV_PRIME_LOW = 0x0193;	// 16777619 0x01000193
	const OFFSET_BASIS	= Buffer.from([0xC5, 0x9D, 0x1C, 0x81]);	// 2166136261 [0x81, 0x1C, 0x9D, 0xC5]
	const HASH_RESULT = Buffer.alloc(4);
	
	/**
	 * Implementation of the FNV-1a hash for a 32-bit hash value
	 * Algorithm can be found here: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-1a
	 * @ignore
	 */
	function fnv1a32(input, encoding='utf8'){
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
	
	/**
	 * Implements FNV-1a to generate 32-bit hash, then uses xor-folding
	 * to convert to a 24-bit hash. See here for more info:
	 * http://www.isthe.com/chongo/tech/comp/fnv/#xor-fold
	 * @ignore
	 */
	function fnv1a24(input, encoding){
		const _32bit = fnv1a32(input, encoding);
		const base = _32bit & 0xFFFFFF;
		const top = (_32bit >>> 24) & 0xFF;
		return (base ^ top) & 0xFFFFFF;
	}
	
	module.exports = {fnv1a24, fnv1a32};
})();

/**
 *	Author: JCloudYu
 *	Create: 2018/12/05
**/
(()=>{
	"use strict";
	
	const SEED = 5381;
	module.exports = {
		/**
		 * Generate a djb2 hash for given input array buffer
		 *
		 * @param {String} input
		 * @return {Number}
		**/
		djb2(input) {
			let val	 = Buffer.from(input);
			let hash = new Uint32Array(1);
			hash[0] = SEED;
			for( let i=0; i<val.length; i++ ) {
				hash[0] = ((hash[0] << 5) + hash[0]) + val[i];
			}
			
			return hash[0];
		},
		
		/**
		 * Generate a djb2a hash for given input array buffer
		 *
		 * @param {String} input
		 * @return {Number}
		**/
		djb2a(input) {
			let val	 = Buffer.from(input);
			let hash = new Uint32Array(1);
			hash[0] = SEED;
			for( let i=0; i<val.length; i++ ) {
				hash[0] = ((hash[0] << 5) + hash[0]) ^ val[i];
			}
			
			return hash[0];
		}
	}
})();

/**
 *	Author: JCloudYu
 *	Create: 2018/12/06
**/
(()=>{
	"use strict";
	
	const ERROR_MAP = {
		UNKNOWN_ERROR: "UNKNOWN_ERROR",
		DB_STORAGE_INIT: "DB_STORAGE_INIT",
		DB_INDEX_INIT: "DB_INDEX_INIT",
		DB_STORAGE_ACCESS: "DB_STORAGE_ACCESS",
		DB_INDEX_ACCESS: "DB_INDEX_ACCESS",
		DB_DUPLICATED_CLOSE_ERROR: "DB_DUPLICATED_CLOSE_REQUEST",
		DB_CLOSING_ERROR: "DB_CLOSING_ERROR",
		DB_CLOSED_ERROR: "DB_CLOSED_ERROR",
		DB_INDEX_STRUCTURE: "DB_INDEX_STRUCTURE"
	};
	const MSG_MAP = {
		[ERROR_MAP.UNKNOWN_ERROR]: "Unknown error has occurred!",
		[ERROR_MAP.DB_STORAGE_INIT]: "Cannot initialize database storage system!",
		[ERROR_MAP.DB_INDEX_INIT]: "Cannot initialize database index system!",
		[ERROR_MAP.DB_STORAGE_ACCESS]: "Cannot access database storage system!",
		[ERROR_MAP.DB_INDEX_ACCESS]: "Cannot access database index system!",
		[ERROR_MAP.DB_CLOSING_ERROR]: "Database is closing now!",
		[ERROR_MAP.DB_DUPLICATED_CLOSE_ERROR]: "Database close request has been invoked already!",
		[ERROR_MAP.DB_CLOSED_ERROR]: "Database has been closed!",
		[ERROR_MAP.DB_INDEX_STRUCTURE]: "Index contains invalid data!"
	};
	
	
	
	class LevelKVError extends Error {
		constructor(code) {
			super(MSG_MAP[code]||MSG_MAP[ERROR_MAP.UNKNOWN_ERROR]);
			Object.defineProperties(this, {
				code:{value:code, enumerable:true},
				details:{value:[], enumerable:true}
			});
		}
	}
	
	Object.assign(LevelKVError, ERROR_MAP);
	
	module.exports = {LevelKVError};
})();

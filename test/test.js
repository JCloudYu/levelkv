/**
 * Project: levelkv
 * File: test.js
 * Author: JCloudYu
 * Create Date: Aug. 31, 2018 
 */
(async()=>{
	"use strict";
	
	const fs = require( 'fs' );
	const crypto = require( 'crypto' );
	const levelkv = require( '../levelkv' );
	
	const ITER_LOOPS  = 1000;
	const MAX_RECORDS = 10000;
	const DB_PATH = `${__dirname}/test_db`;
	
	
	fs.mkdirSync(DB_PATH, 0o777);
	let DB = await levelkv.initFromPath(DB_PATH);
	
	console.log( "Preparing testing data..." );
	{
		console.time();
		for ( let i=0; i<MAX_RECORDS; i++ ) {
			let data = { a:1, b:2, c:"3", d:null, e:undefined, f:false, g:crypto.randomBytes(1024 + Math.floor(Math.random()*6)) };
			DB.put( `data_${i}`, data );
		}
		console.timeEnd();
	}
	
	
	
//	console.log( "Start key querying tests" );
//	let testData = { "a":1, "b":"2", "c":crypto.randomBytes(10) };
	
	
	
	
	
	/*
		{
		for ( let i=0; i<MAX_RECORDS; i++ ) {
			let choice = Math.floor(Math.random()*3);
			let data;
			switch(choice) {
				// Object
				case 0:
					data = { a:1, b:2, c:"3", d:null, e:undefined, f:false, g:crypto.randomBytes(1024 + Math.floor(Math.random()*6)) };
					break;
				
				// Array
				case 1:
					data = [ 1, 2, "3", null, undefined, false, crypto.randomBytes(1024 + Math.floor(Math.random()*6)) ];
					break;
				
				// Primitives
				case 2:
				{
					let type = Math.floor(Math.random()*6);
					switch(type) {
						case 0:
							data = Math.random();
							break;
						case 1:
							data = Math.floor(Math.random()*999);
							break;
						case 2:
							data = "ABDSFDSAFDSA";
							break;
						case 3:
							data = null;
							break;
						case 4:
							data = undefined;
							break;
						case 5:
							data = false;
							break;
					}
				}
			}
			
			DB.put( `data_${i}`, data );
		}
	}
	
	 */
})();

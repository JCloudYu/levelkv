(()=>{
	"use strict";

	module.exports=(func, thisArg=null, ...args)=>{
		return new Promise((resolve, reject)=>{
			func.call(thisArg, ...args, (err, ...results)=>{
				if ( err ) {
					return reject(err);
				}

				resolve(results);
			});
		});
	};
})();
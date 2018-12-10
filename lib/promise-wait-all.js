/**
 *	Author: JCloudYu
 *	Create: 2018/12/06
**/
(()=>{
	"use strict";
	
	module.exports.PromiseWaitAll=(promise_queue=[])=>{
		if ( !Array.isArray(promise_queue) ){
			promise_queue = [promise_queue];
		}
		
		if( promise_queue.length === 0 ){
			return Promise.resolve([]);
		}
		
		return new Promise((resolve, reject) =>{
			let result_queue=[], ready_count=0, resolved = true;
			for(let idx=0; idx<promise_queue.length; idx++) {
				let item = {resolved:true, seq:idx, result:null};
				
				result_queue.push(item);
				Promise.resolve(promise_queue[idx]).then(
					(result)=>{
						resolved = (item.resolved = true) && resolved;
						item.result = result;
					},
					(error)=>{
						resolved = (item.resolved = false) && resolved;
						item.result = error;
					}
				).then(()=>{
					ready_count++;
					
					if ( promise_queue.length === ready_count ) {
						(resolved?resolve:reject)(result_queue);
					}
				});
			}
		});
	};
})();

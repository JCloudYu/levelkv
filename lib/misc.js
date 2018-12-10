/**
 *	Author: JCloudYu
 *	Create: 2018/12/05
**/
(()=>{
	"use strict";
	
	const _ThrottledQueue = new WeakMap();
	class ThrottledQueue {
		constructor() {
			_ThrottledQueue.set(this, {
				_timeout: ThrottleTimeout(),
				_queue: []
			});
			
			this.consumer = null;
		}
		
		/** @type {Number} */
		get length() {
			return _ThrottledQueue.get(this)._queue.length;
		}
		
		/**
		 * Create
		 * @param {*} info
		 * @return Promise<*>;
		**/
		push(info=null) {
			const {_queue, _timeout} = _ThrottledQueue.get(this);
			const promise = PassivePromise();
			const item = {info, ctrl:promise};
			_queue.push(item);
			
			_timeout(___CONSUME_QUEUE, 0, this);
			return promise.promise;
		}
		
		/**
		 * Create a ThrottledQueue object with default consumer api
		 *
		 * @param {function(*[]):Promise<Boolean>} consumer
		 * @returns {ThrottledQueue}
		**/
		static CreateQueueWithConsumer(consumer) {
			const queue = new ThrottledQueue();
			queue.consumer = consumer;
			return queue;
		}
	}
	
	module.exports = {
		ThrottleTimeout,
		ThrottledQueue,
		PassivePromise
	};
	
	
	function ThrottleTimeout() {
		let _scheduled	= null;
		let _executing	= false;
		let _hTimeout	= null;
		const timeout_cb = (cb, delay=0, ...args)=>{
			_scheduled = {cb, delay, args};
			
			if ( _executing ) return;
			
			
			if ( _hTimeout ) {
				clearTimeout(_hTimeout);
				_hTimeout = null;
			}
			__DO_TIMEOUT();
		};
		timeout_cb.clear=()=>{
			_scheduled = null;
			if ( _hTimeout ) {
				clearTimeout(_hTimeout);
				_hTimeout = null;
			}
		};
		return timeout_cb;
		
		
		
		function __DO_TIMEOUT() {
			if ( !_scheduled ) return;
		
			let {cb, delay, args} = _scheduled;
			_hTimeout = setTimeout(()=>{
				_executing = true;
				
				Promise.resolve(cb(...args))
				.then(
					()=>{
						_executing = false;
						_hTimeout = null;
						
						__DO_TIMEOUT();
					},
					(e)=>{
						_executing	= false;
						_hTimeout	= null;
						_scheduled	= null;
						
						throw e;
					}
				);
			}, delay);
			_scheduled = null;
		}
	}
	function PassivePromise() {
		let resolve, reject;
		const promise = new Promise((_resolve, _reject)=>{resolve=_resolve; reject=_reject;});
		return {promise, resolve, reject}
	}
	
	
	
	
	
	
	function ___CONSUME_QUEUE(inst) {
		if ( typeof inst.consumer !== "function" ) {
			return;
		}
		
		const {_queue, _timeout} = _ThrottledQueue.get(inst);
		Promise.resolve(inst.consumer(_queue))
		.then((should_continue=true)=>{
			if ( should_continue === false ) return;
			if ( _queue.length <= 0 ) return;
		
			_timeout(___CONSUME_QUEUE, 0 , inst);
		})
		.catch((e)=>{ throw e; });
	}
})();

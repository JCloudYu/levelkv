(()=>{
	"use strict";

	const {Serialize, Deserialize} = require( 'beson' );
	const {Socket} = require( 'net' );
	const {NetSocket, NET_SOCKET_EVENT, LEVELKV_EVENT} = require( './net-socket' );




	const DATA_STATUS = {

		RETRIEVED: 3
	};

	const PORT 		= 12345;
	const HOST 		= 'localhost';
	const _REL_MAP 	= new WeakMap();

	class Client {
		constructor() {
			const socket = new Socket();

			socket
			.on( NET_SOCKET_EVENT.CONNECT, 	___HANDLE_CONNECT.bind(this) )
			.on( NET_SOCKET_EVENT.CLOSE, 	___HANDLE_CLOSE.bind(this) )
			.on( NET_SOCKET_EVENT.ERROR, 	___HANDLE_ERROR.bind(this) )
			.on( NET_SOCKET_EVENT.DATA, 	___HANDLE_DATA.bind(this) );

			this._socket 		= socket;
			this._serializer 	= (input)=>{ return Serialize(input); };
			this._deserializer 	= (input)=>{ return Deserialize(input); };

			return this;
		}
		get(key) {
			this.send( {op: LEVELKV_EVENT.GET_DATA, key } );
			// return cursor
		}
		put(key, value) {
			this.send( {op: LEVELKV_EVENT.PUT_DATA, key, value} );
		}
		del(key) {
			this.send( {op: LEVELKV_EVENT.DEL_DATA, key} );
		}
		/*
		next() {
			const {cursor} = _REL_MAP.get(this);
			if( !cursor ) return undefined;

			this.send( {op: LEVELKV_EVENT.GET_NEXT, segments: cursor.next()} );
		}
		*/
		send(data) {
			console.log( 'data:', data );
			return this._socket.write( Buffer.from( this._serializer( data ) ));
		}
		connect(...args) {
			return this._socket.connect(...args);
		}
		end() {
			return this._socket.end();
		}
	}


	module.exports = {Client};

	try {
		const _client = new Client();
		_client.connect(PORT, HOST);
	}
	catch(e) {
		throw e;
	}


	function ___HANDLE_CONNECT() {
		const _this = this;
		console.log( `Client (${_this.id}) has connected!` );
		this.send({op: LEVELKV_EVENT.PUT_DATA, key: 'test', value: 'test-data'});
		this.send({op: LEVELKV_EVENT.GET_DATA, key: 'test'});
	}
	function ___HANDLE_CLOSE() {
		const _this = this;
		console.log( `Client (${_this.id}) is disconnected!` );
	}
	function ___HANDLE_ERROR(error) {
		console.log( `Error occurs! ${error}` );
	}
	async function ___HANDLE_DATA(data) {
		console.log( `Receive data:`, this._deserializer( data ) );
		const {op, key, value, segments} = this._deserializer(data);
		switch (op) {
			case LEVELKV_EVENT.GET_DATA:
			{
				// TODO: return cursor segments
				_REL_MAP.set(this, {
					cursor: new Cursor(segments),
					value: []
				} );
				break;
			}
			case LEVELKV_EVENT.GET_NEXT:
			{
				// TODO: get next value
				_REL_MAP.get(this).value.push(value);
				break;
			}
			default:
			{
				console.log( `Invalid operation type! (${op})` );
				break;
			}
		}
	}

	class Cursor {
		constructor(segments) {

		}
		next() {

		}
	}
})();

(()=>{
	const crypto 			= require( 'crypto' );
	const {Socket} = require( 'net' );
	const {Serialize, Deserialize} = require( 'beson' );


	const NET_SOCKET_EVENT = {
		CONNECT: 	'connect',
		CLOSE: 		'close',
		ERROR: 		'error',
		DATA: 		'data'
	};

	const LEVELKV_EVENT = {
		GET_DATA: 		1,
		GET_NEXT: 		2,
		PUT_DATA: 		3,
		DEL_DATA: 		4
	};



	class NetSocket {
		constructor(socket) {
			socket = socket || new Socket();

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
		on( eventName, callback ) {
			if( typeof callback !== 'function') throw 'Event callback accepts type function only!';

			switch (eventName) {
				case NET_SOCKET_EVENT.CONNECT:
					this._socket.on( NET_SOCKET_EVENT.CONNECT, callback.bind(this) );
					break;
				case NET_SOCKET_EVENT.CLOSE:
					this._socket.on( NET_SOCKET_EVENT.CLOSE, callback.bind(this) );
					break;
				case NET_SOCKET_EVENT.ERROR:
					this._socket.on( NET_SOCKET_EVENT.ERROR, callback.bind(this) );
					break;
				case NET_SOCKET_EVENT.DATA:
					this._socket.on( NET_SOCKET_EVENT.DATA, callback.bind(this) );
					break;
			}
			return this;
		}
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
		get id() {
			return this._id;
		}
	}


	module.exports = {NetSocket, NET_SOCKET_EVENT, LEVELKV_EVENT};



	function ___HANDLE_CONNECT() {
		const _this = this;
		console.log( `Client (${_this.id}) has connected!` );
	}
	function ___HANDLE_CLOSE() {
		const _this = this;
		console.log( `Client (${_this.id}) is disconnected!` );
	}
	function ___HANDLE_ERROR(error) {
		console.log( `Error occurs! ${error}` );
	}
	function ___HANDLE_DATA(data) {
		console.log( `Receive data:`, this._deserializer( data ) );
	}
})();
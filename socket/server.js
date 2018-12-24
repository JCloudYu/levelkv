(async()=>{
	const net = require( 'net' );
	const crypto 			= require( 'crypto' );
	const {NetSocket, NET_SOCKET_EVENT, LEVELKV_EVENT} = require( './net-socket' );
	const {LevelKV} 		= require( '../levelkv' );
	const {Serialize, Deserialize} = require( 'beson' );

	const PORT 		= 12345;
	const HOST 		= 'localhost';
	const DB_DIR 	= `${__dirname}/../ignored.test/levelkv-db`;






	const _database 	= await LevelKV.initFromPath( `${DB_DIR}` );




	const NET_SERVER_EVENT = {
		CONNECTION: 'connection',
		CLOSE: 		'close',
		ERROR: 		'error'
	};
	class Server {
		constructor() {
			this._server 	= net.createServer()
			.on( NET_SERVER_EVENT.CONNECTION, 	___HANDLE_SERVER_CONNECT.bind(this) )
			.on( NET_SERVER_EVENT.CLOSE, 		___HANDLE_SERVER_CLOSE.bind(this) )
			.on( NET_SERVER_EVENT.ERROR, 		___HANDLE_SERVER_ERROR.bind(this) );

			this._serializer 	= (input)=>{ return Serialize(input); };
			this._deserializer 	= (input)=>{ return Deserialize(input); };
		}
		listen(...args) {
			this._server.listen(...args);
		}
		send(data) {
			console.log( 'send data:', data );
			return this._socket.write( Buffer.from( this._serializer( data ) ));
		}
	}

	const _server = new Server();
	_server.listen(PORT, HOST);


	function ___HANDLE_SERVER_CONNECT(socket) {
		console.log( `[Server]: connect!` );

		socket
		.on( NET_SOCKET_EVENT.CONNECT, 	___HANDLE_SOCKET_CONNECT.bind(this) )
		.on( NET_SOCKET_EVENT.CLOSE, 	___HANDLE_SOCKET_CLOSE.bind(this))
		.on( NET_SOCKET_EVENT.ERROR, 	___HANDLE_SOCKET_ERROR.bind(this) )
		.on( NET_SOCKET_EVENT.DATA, 	___HANDLE_SOCKET_DATA.bind(this) );

		this._socket = socket;
	}
	function ___HANDLE_SERVER_CLOSE(q) {
		console.log( `[Server]: close` );
	}
	function ___HANDLE_SERVER_ERROR(error) {
		console.log( `[Server]: Error occurs! (${error})` );
	}

	function ___HANDLE_SOCKET_CONNECT() {
		console.log(`[Socket]: connect!`);
	}
	function ___HANDLE_SOCKET_CLOSE() {
		console.log(`[Socket]: close!`);
	}
	function ___HANDLE_SOCKET_ERROR(error) {
		console.log(`[Socket]: error! (${error})`);
	}
	async function ___HANDLE_SOCKET_DATA(data) {
		console.log(`[Socket]: data!`);

		const {op, key, value, segments} = this._deserializer(data);
		switch (op) {
			case LEVELKV_EVENT.GET_DATA:
			{
				const cursor = await _database.get( key, {mutable_cursor: true} );
				this.send( {op: LEVELKV_EVENT.GET_DATA, segments: cursor.segments} );
				break;
			}
			case LEVELKV_EVENT.GET_NEXT:
			{
				const cursor = new DBMutableCursor(_database, segments);
				this.send( {op: LEVELKV_EVENT.GET_NEXT, value: await cursor.next().value} );
				break;

			}
			case LEVELKV_EVENT.PUT_DATA:
			{
				_database.put( key, value );
				break;
			}
			case LEVELKV_EVENT.DEL_DATA:
			{
				_database.del( key );
				break;
			}
			default:
			{
				console.log( `Invalid operation type! (${op})` );
				break;
			}
		}
	}
})();
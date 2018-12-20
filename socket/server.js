(async()=>{
	const crypto 			= require( 'crypto' );
	const {LevelKV} 		= require( '../levelkv' );
	const {NetEvtServer} 	= require( 'netevt' );
	const {Serialize, Deserialize} = require( 'beson' );

	const PORT 		= 1234;
	const HOST 		= 'localhost';
	const DB_DIR 	= `${__dirname}/ignored.test/levelkv-db`;

	const LEVELKV_EVENT = {
		CONNECTED: 			'connected',
		DISCONNECTED: 		'disconnected',
		GET_DATA_REQ: 		'3',
		GET_DATA_ACK: 		'4',
		GET_LENGTH_REQ: 	'5',
		GET_LENGTH_ACK: 	'6',
		GET_NEXT_REQ: 		'7',
		GET_NEXT_ACK: 		'8',
		GET_TO_ARRAY_REQ: 	'9',
		GET_TO_ARRAY_ACK: 	'10',
		PUT_DATA_REQ: 		'11',
		DEL_DATA_REQ: 		'12'
	};



	const _cursors 		= {};
	const _database 	= await LevelKV.initFromPath( `${DB_DIR}` );
	const _serverInst 	= new NetEvtServer();
	_serverInst._serializer 	= (input)=>{ return Serialize(input); };
	_serverInst._deserializer 	= (input)=>{ return Deserialize(input); };



	_serverInst
	.on( LEVELKV_EVENT.CONNECTED, (e)=>{
		const {sender: client} = e;
		client.id = `${Date.now()}_${crypto.randomBytes(5).toString('hex')}`;
		console.log( `Client (${client.id}) has connected!` );
	})
	.on( LEVELKV_EVENT.DISCONNECTED, (e)=>{
		const {sender: client} = e;
		console.log( `Client (${client.id}) has disconnected!` );
	})
	.on( LEVELKV_EVENT.GET_DATA_REQ, async(e, data)=>{
		const {sender: client} = e;
		const {key} = data;

		console.log( `Get key (${key}) from database for client (${client.id})` );
		const cursor 	= await _database.get( key );
		const token 	= crypto.randomBytes(5).toString('hex');
		_cursors[token] = cursor;

		client.triggerEvent( LEVELKV_EVENT.GET_DATA_ACK, {token} );
	})
	.on( LEVELKV_EVENT.GET_LENGTH_REQ, (e, data)=>{
		const {sender: client} = e;
		const {token} 	= data;
		const cursor 	= _cursors[token];
		if( !cursor ) return;

		client.triggerEvent( LEVELKV_EVENT.GET_LENGTH_ACK, {token, length: cursor.length} );
	} )
	.on( LEVELKV_EVENT.GET_TO_ARRAY_REQ, async(e, data)=>{
		const {sender: client} = e;
		const {token} = data;
		const cursor = _cursors[token];
		if( !cursor ) return;

		client.triggerEvent( LEVELKV_EVENT.GET_TO_ARRAY_ACK, {token, value: await cursor.toArray()} );
		delete _cursors[token];
	} )
	.on( LEVELKV_EVENT.GET_NEXT_REQ, async(e, data)=>{
		const {sender: client} = e;
		const {token} = data;
		const cursor = _cursors[token];

		if( !cursor ) return;

		if( cursor.length > 0 ) {
			client.triggerEvent( LEVELKV_EVENT.GET_NEXT_ACK, {token, value: await cursor.next().value} );
			return;
		}
		delete _cursors[token];
	} )
	.on( LEVELKV_EVENT.PUT_DATA_REQ, (e, data)=>{
		const {sender: client} = e;
		const {key, value} = data;

		console.log( `Add key (${key}) from database for client (${client.id})` );
		_database.put( key, value );
	})
	.on( LEVELKV_EVENT.DEL_DATA_REQ, (e, data)=>{
		const {sender: client} = e;
		const {key} = data;

		console.log( `Delete key (${key}) from database for client (${client.id})` );
		_database.del( key);
	})
	.listen( PORT, HOST );
})();
(()=>{
	const {LevelKV} 		= require( '../levelkv' );
	const {NetEvtServer} 	= require( 'netevt' );
	const {Serialize, Deserialize} = require( 'beson' );

	const PORT = 1234;
	const HOST = 'localhost';
	const BASE_DIR 		= `${__dirname}/ignored.test/levelkv-dbs`;
	const _DATABASES 	= {};
	const _CLIENTS 		= {};
	const LEVELKV_EVENT = {
		CONNECTED: 			'connected',
		DISCONNECTED: 		'disconnected',
		OPEN_DATABASE_REQ: 	'OPEN_DATABASE_REQ',
		OPEN_DATABASE_ACK: 	'OPEN_DATABASE_ACK',
		CLOSE_DATABASE_REQ: 'CLOSE_DATABASE_REQ',
		CLOSE_DATABASE_ACK: 'CLOSE_DATABASE_ACK',
		GET_DATA_REQ: 		'GET_DATA_REQ',
		GET_DATA_ACK: 		'GET_DATA_ACK',
		PUT_DATA_REQ: 		'PUT_DATA_REQ',
		PUT_DATA_ACK: 		'PUT_DATA_ACK',
		DEL_DATA_REQ: 		'DEL_DATA_REQ',
		DEL_DATA_ACK: 		'DEL_DATA_ACK'
	};
	const STATUS = {
		SUCCESS: 	0,
		FAIL: 		-1
	};

	const ServerInst = new NetEvtServer();
	ServerInst._serializer 		= (input)=>{ return Serialize(input); };
	ServerInst._deserializer 	= (input)=>{ return Deserialize(input); };



	ServerInst
	.on( LEVELKV_EVENT.CONNECTED, (e)=>{
		const {sender: client} = e;
		client.id = Date.now();
		_CLIENTS[client.id] = {};
		console.log( `Client ${client.id} has connected!` );
	})
	.on( LEVELKV_EVENT.DISCONNECTED, (e)=>{
		const {sender: client} = e;
		delete _CLIENTS[client.id];
		console.log( `Client ${client.id} has disconnected!` );
	})
	.on( LEVELKV_EVENT.OPEN_DATABASE_REQ, async(e, data)=>{
		const {sender: client} = e;
		const {db_name} = data;

		console.log( `Open database (${db_name}) for client ${client.id}` );
		if( !_DATABASES[db_name] ) {
			_DATABASES[db_name] = await LevelKV.initFromPath( `${BASE_DIR}/${db_name}` );
		}
		_CLIENTS[client.id][db_name] = _DATABASES[db_name];
		client.triggerEvent( LEVELKV_EVENT.OPEN_DATABASE_ACK, { status: STATUS.SUCCESS, message: '' } );
	})
	.on( LEVELKV_EVENT.CLOSE_DATABASE_REQ, (e, data)=>{
		const {sender: client} = e;
		const {db_name} = data;

		console.log( `Close database (${db_name}) for client ${client.id}` );
		delete _CLIENTS[client.id][db_name];
		client.triggerEvent( LEVELKV_EVENT.CLOSE_DATABASE_ACK, { status: STATUS.SUCCESS, message: '' } );
	})
	.on( LEVELKV_EVENT.GET_DATA_REQ, async(e, data)=>{
		const {sender: client} = e;
		const {db_name, key} = data;

		console.log( `Get key ${key} from database (${db_name}) for client ${client.id}` );
		if( !_CLIENTS[client.id][db_name] ) {
			const message = `Cannot get data for client (${client.id})! Database (${db_name}) is not open yet!`;
			client.triggerEvent( LEVELKV_EVENT.GET_DATA_ACK, { status: STATUS.FAIL, message } );
			console.log( message );
			return;
		}

		const result = await _CLIENTS[client.id][db_name].get( key );
		client.triggerEvent( LEVELKV_EVENT.GET_DATA_ACK, { status: STATUS.SUCCESS, message: '', data: await result.toArray() } );
	})
	.on( LEVELKV_EVENT.PUT_DATA_REQ, (e, data)=>{
		const {sender: client} = e;
		const {db_name, key, value} = data;

		console.log( `Add key ${key} from database (${db_name}) for client ${client.id}` );
		if( !_CLIENTS[client.id][db_name] ) {
			const message = `Cannot add data for client (${client.id})! Database (${db_name}) is not open yet!`;
			client.triggerEvent( LEVELKV_EVENT.PUT_DATA_ACK, { status: STATUS.FAIL, message } );
			console.log( message );
			return;
		}

		_CLIENTS[client.id][db_name].put( key, value );
		client.triggerEvent( LEVELKV_EVENT.PUT_DATA_ACK, { status: STATUS.SUCCESS, message: '' } );
	})
	.on( LEVELKV_EVENT.DEL_DATA_REQ, (e, data)=>{
		const {sender: client} = e;
		const {db_name, key} = data;

		console.log( `Delete key ${key} from database (${db_name}) for client ${client.id}` );
		if( !_CLIENTS[client.id][db_name] ) {
			const message = `Cannot delete data for client (${client.id})! Database (${db_name}) is not open yet!`;
			client.triggerEvent( LEVELKV_EVENT.DEL_DATA_ACK, { status: STATUS.FAIL, message } );
			console.log( message );
			return;
		}

		_CLIENTS[client.id][db_name].del( key);
		client.triggerEvent( LEVELKV_EVENT.DEL_DATA_ACK, { status: STATUS.SUCCESS, message: '' } );
	})
	.listen( PORT, HOST );
})();
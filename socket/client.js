(()=>{
	"use strict";

	const beson = require('beson');
	const {NetEvtClient} = require( 'netevt' );
	const clientInst = new NetEvtClient();
	let _server;



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



	clientInst._serializer 		= (input)=>{return beson.Serialize(input);};
	clientInst._deserializer 	= (input)=>{return beson.Deserialize(input);};

	const tokens = {};


	clientInst
	.on( LEVELKV_EVENT.CONNECTED, (e)=>{
		const {sender: server} = e;
		_server = server;
		console.log( "Connected to server!" );

		console.log( `Database is ready, start to do operations...` );
		server.triggerEvent( LEVELKV_EVENT.GET_DATA_REQ, {
			key: 'key-test'
		} );

		server.triggerEvent( LEVELKV_EVENT.PUT_DATA_REQ, {
			key: 'key-test',
			value: { val: 'new value' }
		} );

		server.triggerEvent( LEVELKV_EVENT.GET_DATA_REQ, {
			key: 'key-test'
		} );

		server.triggerEvent( LEVELKV_EVENT.DEL_DATA_REQ, {
			key: 'key-test'
		} );

		server.triggerEvent( LEVELKV_EVENT.GET_DATA_REQ, {
			key: 'key-test'
		} );

		server.triggerEvent( LEVELKV_EVENT.PUT_DATA_REQ, {
			key: 'key-test',
			value: 'new value2'
		} );

		server.triggerEvent( LEVELKV_EVENT.GET_DATA_REQ, {
			key: 'key-test'
		} );
	})
	.on( LEVELKV_EVENT.DISCONNECTED, (e)=>{
		console.log( `Disconnected from server!` );
	})
	.on( LEVELKV_EVENT.GET_DATA_ACK, (e, data)=>{
		const {sender: server} = e;

		console.log( `Receiving data from server!` );
		const {token} = data;
		console.log(data);
		tokens[token] = {token, length: 0, value: []};

		//server.triggerEvent( LEVELKV_EVENT.GET_TO_ARRAY_REQ, {token} );
		server.triggerEvent( LEVELKV_EVENT.GET_LENGTH_REQ, {token} );
	})
	.on( LEVELKV_EVENT.GET_LENGTH_ACK, (e, data)=> {
		const {sender: server} = e;
		const {token, length} = data;
		console.log( `GET_LENGTH_ACK: Receiving ${token} length (${length}) from server!` );
		tokens[token].length = length;

		for( let i=0; i<length; i++ ) {
			server.triggerEvent( LEVELKV_EVENT.GET_NEXT_REQ, {token} );
		}
	} )
	.on( LEVELKV_EVENT.GET_TO_ARRAY_ACK, (e, data)=> {
		const {token, value} = data;
		console.log( `GET_TO_ARRAY_ACK: Receiving ${token} from server!` );
		tokens[token].value = value;
		console.log( `\tGET_TO_ARRAY_ACK: `, tokens[token] );
	} )
	.on( LEVELKV_EVENT.GET_NEXT_ACK, (e, data)=> {
		const {token, value} = data;
		console.log( `GET_NEXT_ACK: Receiving ${token} from server!` );
		tokens[token].value.push( value );
		console.log( `\tGET_NEXT_ACK: `, tokens[token] );
	} )
	.connect( 1234, 'localhost' );
})();

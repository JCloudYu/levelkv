# LevelKV #
A simple native Node.js implementation of the well-known key-value based LevelDB library.



## Installation ##
> npm  i levelkv



## Examples ##
```javascript
// Open a database
const { LevelKV, DBMutableCursor } = require('levelkv');
const db = await LevelKV.initFromPath('your_directory_path');


let key = 'key';
let value = { a:1, b:2, c:3 };

// Add data
await db.put( `${key}`, value );
await db.put( `${key}2`, value );

// Get data
let result = await db.get( key );
console.log( result.length );
console.log( await result.toArray() );


// Delete data
await db.del( key );

// Get all data
result = await db.get();
for await( let value of result )
{
    console.log( value );
}



// Use Mutable Cursor
const dbCursor          = await db.get();
const dbMutableCursor 	= new DBMutableCursor(dbCursor);
const segments          = dbMutableCursor.segments;
for( let segment of segments )
{
    segment.in_memory = true;
    segment.value = 'newValue';
}
console.log( await dbMutableCursor.toArray() );



// Close the database
await db.close();
```



## Notice ##
All the operations in LevelKV are asynchronous, remember to add `await` keyword in front of the function call if you need.



## Operations ##
### Open A Database ###
```javascript
/**
 * Initialize the database.
 *
 * @async
 * @param {string} dir - Directory path of the database. Make sure you have created or it will fail if the directory does not exist.
 * @param {object} options - Database creating options. Defaults to {auto_create:true}, which means create a new database automatically if not exist.
 * @returns {Promise<LevelKV>} - Promise object represents the database itself.
 */
.initFromPath(dir, options={auto_create:true})
```

### Close A Database ###
```javascript
/**
 * Close the database.
 *
 * @async
 */
.close()
```

### Reads And Writes  ###
```javascript
/**
 * Get data from the database.
 *
 * @async
 * @param {string|string[]} keys - A specific key or an array of keys to retrieve, if not given it will retrieve all data from the database.
 * @returns {Promise<DBCursor>} - Promise object represents the database cursor of the retrieved data.
 */
.get(keys=[])
```
```javascript
/**
 * Add data to the database.
 *
 * @async
 * @param {string|string[]} keys - A specific key or an array of keys to add.
 * @param {*} val - The value to add.
 */
.put(keys=[], val)
```
```javascript
/**
 * Delete data from the database.
 *
 * @async
 * @param {string|string[]} keys -  A specific key or an array of keys to delete.
 */
.del(keys=[])
```



---
## For Maintainer ##
### Install Project ###
* Clone Project:
    > git clone \<project-url\>
* Install Dependency Packages:
    > npm install
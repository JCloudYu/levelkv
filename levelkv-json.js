(()=>{
    'use strict';

    const fs = require('fs');

    class LevelKV_DB_JSON {
        constructor(){
        }

        /*
        * @async
        * @param {string} dir
        * @param {object} options
        */
        async open(dir, options={}) {
        }

        /*
        * @async
        */
        async close() {
        }

        /*
        * @async
        * @param {string | [string, ...]} query
        * @return {cursor}
        */
        async get(query=[]) {
            const a = "123";
        }

        /*
        * @async
        * @param {string | [string, ...]} query
        * @param {object} content
        */
        async put(query=[], content={}) {
        }

        /*
        * @async
        * @param {string | [string, ...]} query
        */
        async del(query=[]) {
        }
    }

    module.exports = LevelKV_DB_JSON;
})();

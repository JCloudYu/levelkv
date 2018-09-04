(() =>{
    'use strict';

    const fs = require('fs');
    const path = require('path');
    const readline = require('readline');
    // const lockfile = require('lockfile');
    const tiiny = require('tiinytiny');

    const stateFileName = '.states.json';
    const indexExtName = 'jidx';
    const dataExtName = 'jdata';
    const nullIndex = '-1:-1';

    class LevelKV_DB_JSON {
        constructor() {
        }

        /*
        * @async
        * @param {string} dir
        * @param {object} options
        */
        async open(dir, options = {}) {
            dir = path.resolve(dir);

            const stateFilePath = `${dir}/${stateFileName}`;
            let statJson = await new Promise((resolve, reject) => {
                // check state file exist
                fs.stat(stateFilePath, (err) => {
                    (err) ? reject(err) : resolve();
                })
            })
            .then(() => {
                // read state file
                return new Promise((resolve, reject) => {
                    fs.readFile(stateFilePath, 'utf8', (err, data) => {
                        (err) ? reject(err) : resolve(JSON.parse(data));
                    });
                });
            })
            .catch((err) => {
                console.error(err.message);
                return null;
            });

            if (!statJson) {
                return;
            }

            this.statJson = statJson;
            this.dir = dir;
            this.indexFile = `${dir}/${statJson['indexFile']}.${indexExtName}`;
            this.dataFiles = statJson['dataFiles'].map(fileName => `${dir}/${fileName}.${dataExtName}`);
        }

        /*
        * @async
        */
        async close() {
        }

        /*
        * @async
        * @param {string | [string, ...]} keys
        * @return {cursor}
        */
        async get(keys = []) {
            keys = __verifyIsArray(keys);

            let promises = [];
            for (const key of keys) {
                const promise = Promise.resolve(this.__getIndex(key))
                .then(async (idx) => {
                    const [fileIdx, lineIdx] = idx.split(':');
                    const lineJson = JSON.parse(await this.__readLine(this.dataFiles[fileIdx], lineIdx));
                    return lineJson[key];
                });
                promises.push(promise);
            }

            let cursor = [];
            let results = await tiiny.PromiseWaitAll(promises);
            results.forEach((pRes)=>{
                let result = null;
                if (pRes.resolved) {
                    result = pRes.result;
                }
                cursor.push(result);
            });
            return cursor;
        }

        /*
        * @async
        * @param {string | [string, ...]} keys
        * @param {any} value
        */
        async put(keys = [], value) {
            keys = __verifyIsArray(keys);

            let promises = [];
            for (const key of keys) {
                const promise = Promise.resolve(this.__getIndex(key))
                .then(async (idx) => {
                    const [fileIdx, lineIdx] = idx.split(':');
                    const lineJson = JSON.parse(await this.__readLine(this.dataFiles[fileIdx], lineIdx));
                    return lineJson[key];
                });
                promises.push(promise);
            }

            let cursor = [];
            let results = await tiiny.PromiseWaitAll(promises);
            results.forEach((pRes)=>{
                let result = null;
                if (pRes.resolved) {
                    result = pRes.result;
                }
                cursor.push(result);
            });
            return cursor;
        }

        /*
        * @async
        * @param {string | [string, ...]} queries
        */
        async del(queries = []) {
        }
    
        async __getIndex(key) {
            if (key === null) {
                return nullIndex;
            }

            return new Promise((resolve, reject) => {
                fs.readFile(this.indexFile, 'utf8', (err, data) => {
                    (err) ? reject(err) : resolve(JSON.parse(data));
                });
            })
            .then((idxJson) => {
                return idxJson['key'] || nullIndex;
            })
            .catch((err) => {
                console.error(err);
                return nullIndex;
            });
        }

        async __addIndx(idxs = {}) {
            if (__isObject(idxs)) {
                return false;
            }

            return new Promise((resolve, reject) => {
                fs.readFile(this.indexFile, 'utf8', (err, data) => {
                    (err) ? reject(err) : resolve(JSON.parse(data));
                });
            })
            .then((idxJson) => {
                Object.entries(idxs).forEach(([key, val]) => {
                    idxJson[key] = val;
                });
                fs.writeFile(this.indexFile, 'utf8', (err) => {
                    if (err) throw err;
                    return true;
                });
            })
            .catch((err) => {
                console.error(err);
                return false;
            });
        }
    
        async __readLine(filePath, lineNo) {
            const rl = readline.createInterface({
                input: fs.createReadStream(filePath)
            });
    
            let anchor = 0;
            const data = await new Promise((resolve, reject) => {
                rl.on('line', (line) => {
                    ++anchor;
                    if (anchor === lineNo) {
                        rl.close();
                        resolve(line);
                    }
                });
            });
            return data;
        }

        async __writeLine(filePath, data) {
            const ws = fs.createWriteStream(filePath, { flags: 'a' });
            ws.write(JSON.stringify(data));
            ws.write('\n');
            ws.close();
        }
    }

    module.exports = LevelKV_DB_JSON;

    function __verifyIsArray(data) {
        return (Array.isArray(data) ? data : [data]);
    }

    function __isObject(data) {
        return (typeof data === 'object' && data !== null);
    }
})();

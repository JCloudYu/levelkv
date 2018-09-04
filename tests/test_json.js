(async () => {
    process.chdir(__dirname);

    const JSON  = require('../levelkv-json');
    const db = new JSON();
    await db.open('./test_json_db');
    // let r = await db.get(['bbb', 'ccc', 'aaa']);
    let r = await db.put('aaa', 123);
    console.log(r);
    // db.open('D:/company/project/levelkv/error/test_json_db');
})();

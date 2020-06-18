const sqlite3 = require('sqlite3');


// Private var to store cached db path.
var cachedDbPath = "";


module.exports = {


    // Optional db path caching:
    cacheDbPath(dbPath) {
        cachedDbPath = dbPath;
    },

    // Execution of raw queries:
    executeQuery(sql, dbPath) {

        console.log(cachedDbPath);
        var db = undefined;
        if (!dbPath) {
            var db = new sqlite3.Database(cachedDbPath);
        } else {
            var db = new sqlite3.Database(dbPath);
        }

        return new Promise((resolve, reject) => {
            console.log(`Executing SQL: ${sql}`);
            db.all(sql, (err, res) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(res);
                }
            });
        });
    },
    execute(sql, dbPath) {

        console.log(cachedDbPath);
        var db = undefined;
        if (!dbPath) {
            var db = new sqlite3.Database(cachedDbPath);
        } else {
            var db = new sqlite3.Database(dbPath);
        }

        return new Promise((resolve, reject) => {
            console.log(`Executing SQL: ${sql}`);
            db.run(sql, (err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve({});
                }
            });
        });
    },
    executeBatch(statements, dbPath) {

        console.log(cachedDbPath);
        var db = undefined;
        if (!dbPath) {
            var db = new sqlite3.Database(cachedDbPath);
        } else {
            var db = new sqlite3.Database(dbPath);
        }

        return new Promise((resolve, reject) => {
            db.serialize(function () {
                console.log(`Executing SQL: BEGIN`);
                db.exec("BEGIN");
                var sql = statements.join('; ');
                console.log(`Executing SQL: ${sql}`);
                db.exec(sql, (err) => {
                    if (err) {
                        console.log(`Executing SQL: ROLLBACK`);
                        db.exec("ROLLBACK");
                        reject(err);
                    } else {
                        console.log(`Executing SQL: COMMIT`);
                        db.exec("COMMIT");
                        resolve({});
                    }
                });
            });
        });
    },

    // Building of queries and subsequent execution:
    executeInsert(tableName, body, dbPath) {

        var insertSql = this.buildInsert(tableName, body);

        return this.execute(insertSql, dbPath);
    },
    executeDelete(tableName, where, dbPath) {
        var deleteSql = this.buildDelete(tableName, where);

        return this.execute(deleteSql, dbPath);
    },
    executeUpdate(tableName, set, where, dbPath) {

        var updateSql = this.buildUpdate(tableName, set, where);

        return this.execute(updateSql, dbPath);
    },
    executeSelect(tableName, where, dbPath) {

        if (where !== undefined) {

            var selectSql = this.buildSelect(tableName, where);

            return this.executeQuery(selectSql, dbPath);
        } else {
            return this.executeQuery(`select * from ${tableName}`, dbPath);
        }
    },

    // Building of queries:
    buildInsert(tableName, body) {
        var insertSql = `insert into ${tableName} (`;

        var values = [];
        for (var key in body) {
            insertSql += `${key},`;
            values.push(body[key]);
        }

        insertSql = insertSql.substr(0, insertSql.lastIndexOf(',')) + ') values (';

        for (idx in values) {
            var val = values[idx];
            if (!val) {
                insertSql += `null,`;
            } else {
                insertSql += `'${val}',`;
            }

        }

        insertSql = insertSql.substr(0, insertSql.lastIndexOf(',')) + ')';

        return insertSql;
    },
    buildDelete(tableName, where) {
        var deleteSql = `delete from ${tableName} where `;

        for (var key in where) {
            if (where[key] === null) {
                deleteSql += `${key} = null and `;
            } else {
                deleteSql += `${key} = '${where[key]}' and `;
            }
        }

        deleteSql = deleteSql.substr(0, deleteSql.lastIndexOf(' and '));

        return deleteSql;
    },
    buildUpdate(tableName, set, where) {
        var updateSql = `update ${tableName} set `;

        for (var key in set) {
            if (set[key] === null) {
                updateSql += `${key} = null, `;
            } else {
                updateSql += `${key} = '${set[key]}', `;
            }
        }

        updateSql = updateSql.substr(0, updateSql.lastIndexOf(', ')) + ' where ';

        for (var key in where) {
            if (where[key] === null) {
                updateSql += `${key} = null and `
            } else {
                updateSql += `${key} = '${where[key]}' and `
            }
        }

        updateSql = updateSql.substr(0, updateSql.lastIndexOf(' and '));

        return updateSql;
    },
    buildSelect(tableName, where) {
        var selectSql = `select * from ${tableName} where `;

        for (var key in where) {
            selectSql += `${key} = '${where[key]}' and `
        }

        selectSql = selectSql.substr(0, selectSql.lastIndexOf(' and '));

        return selectSql;
    },

    // Other utility functions:
    async recordExists(tableName, where, dbPath) {
        try {
            var res = await this.executeSelect(tableName, where, dbPath);
            return res.length > 0;
        } catch (e) {
            return false;
        }

    }
};
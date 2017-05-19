var seq = require('seq'),
    dal = require('../dal'),
    _ = require("under_score");
    lib_mysql = require("mysql");
    bufferedReader = require('buffered-reader');

//
// Not to be consufed with node mysql module, this is just a mysql driver for dal

function _mysql() {
};

module.exports = {
    create : function(connInfo, cb /* (err, db) */) {
        var driver = new _mysql();
        driver.create(connInfo, cb);
    }
};

_mysql.prototype = {

    // connInfo is :
    // "driver"    : "mysql",
    // "host"      : "localhost",
    // "port"      : 3306,
    // "user"      : "root",
    // "database"  : "fld1",
    // "password"  : ""
    create: function(connInfo, externcb /* function(err, db) */) {
        this.client = lib_mysql.createConnection(connInfo);
        this.connInfo = connInfo;
        this.name = connInfo.driver;
        
        var driver = this;
        var db = {	// returned object in async cb, wraps around this
            driver 	 : this,
            schema : {
                constraints : null,
                tables : {}
            }
        };
        
        // construct vo object for all tables in this database,
        // establish forieng key relationships between tables by reading from mysql meta tables
        seq()
            .seq(function() {
                driver.client.query("show tables", this);
                // callback(err, rows, fields ), rows is [], fields is {}
            })
            // this.stack is [ rows, fields ], flatten to [ row0, row1, .... fields ], then remove last elem fields
            .flatten(false).pop()
            // then for each elem in stack, execute in sequence
            // find basic table definition for each table via desc
            .seqEach(function(row, i) {
                var self = this;
                var tableName = row[_('Tables_in_%s').format(connInfo.database)];
                driver.client.query(_("desc `%s`").format(tableName), function(err, results, fields){
                    if (!err) {
                        var tableDef = db.schema.tables[tableName] = {
                                tableName   : tableName,
                                database    : connInfo.database,
                                cols        : {},
                                autoIncCol  : null,
                                primaryKey  : null,
                                uniqueKeys  : [],
                                foreignKeys : [],
                                refByKeys   : []

                            };
                        // parse table def!
                        _(results).each(function(field, i) {
                            // Field: 'id',
                            // Type: 'bigint(20)',
                            // Null: 'NO',
                            // Key: 'PRI',
                            // Default: null,
                            // Extra: 'auto_increment',
                            var type,typeLength=null,typeData=null;
                            var m = /^(\w+)\((.+)\)(.*)$/.exec(field.Type); // match against mysql's weird type string that contains (\d+)
                            if (m) {
                                type = m[1] + m[3];
                                if (type == 'enum')
                                    typeData = _(m[2].split(',')).map(function(e) { return _(e).trim("'\""); });
                                else
                                    typeLength = parseInt(m[2]);
                            }
                            else
                                type = field.Type;
                                
                            var catType = driver.categorizeType(type);
                            if (!catType) {
                                externcb(new Error(_("%s.%s: unrecognized type: %s").format(tableName, field.Field, type)));
                                return;
                            }
                            
                            var defaultVal ;
                            if (field.Default==null)
                                defaultVal =  require("../types")[catType];
                            else { // field.Default is always string as returned by mysql, convert to native type
                                switch (catType) {
                                case 'int'    : defaultVal = parseInt(field.Default); break;
                                case 'float'  : defaultVal = parseInt(field.Default); break;
                                case 'string' :
                                case 'enum'   :
                                case 'blob'   :
                                case 'time'   :
                                    defaultVal = field.Default;
                                    break;
                                case 'datetime' :
                                    defaultVal = driver.dateTimeToJs(field.Default);
                                    break;
                                }
                                
                            }
                            var col = tableDef.cols[field.Field] = {   // the column object
                                tableName  : tableName,
                                colName    : field.Field,
                                type       : type,
                                typeLength : typeLength,
                                typeData   : typeData,
                                defaultVal : defaultVal,
                                nullable   : field.Null!='NO',
                                autoInc    : (field.Extra == 'auto_increment'),
                                typeCat    : catType
                            };
                            if (col.autoInc) tableDef.autoIncCol = col;
                            
                            
                        });
                    }
                    // to signal end of this job, so next action can continue, pass down error if any
                    self(err);
                })
            })
            // then read constraints
            .seq( function() {
                var self = this;
                // SELECT DISTINCT a.TABLE_SCHEMA,a.TABLE_NAME,a.CONSTRAINT_NAME,a.CONSTRAINT_TYPE,b.COLUMN_NAME,b.ORDINAL_POSITION,b.POSITION_IN_UNIQUE_CONSTRAINT,b.REFERENCED_TABLE_SCHEMA,b.REFERENCED_TABLE_NAME,b.REFERENCED_COLUMN_NAME,c.UPDATE_RULE,c.DELETE_RULE FROM information_schema.TABLE_CONSTRAINTS a INNER JOIN information_schema.KEY_COLUMN_USAGE b ON a.TABLE_SCHEMA=b.TABLE_SCHEMA AND a.TABLE_NAME=b.TABLE_NAME AND a.CONSTRAINT_NAME=b.CONSTRAINT_NAME LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS c ON b.CONSTRAINT_NAME=b.CONSTRAINT_NAME AND c.TABLE_NAME=b.TABLE_NAME AND c.REFERENCED_TABLE_NAME=b.REFERENCED_TABLE_NAME AND c.CONSTRAINT_SCHEMA=b.CONSTRAINT_SCHEMA WHERE a.TABLE_SCHEMA='vdf' ORDER BY a.TABLE_NAME,a.CONSTRAINT_NAME,b.ORDINAL_POSITION,b.POSITION_IN_UNIQUE_CONSTRAINT;
                var sql =
                    "SELECT DISTINCT " +
                        "a.TABLE_SCHEMA,a.TABLE_NAME,a.CONSTRAINT_NAME,a.CONSTRAINT_TYPE, " +
                        "b.COLUMN_NAME,b.ORDINAL_POSITION,b.POSITION_IN_UNIQUE_CONSTRAINT,b.REFERENCED_TABLE_SCHEMA,b.REFERENCED_TABLE_NAME,b.REFERENCED_COLUMN_NAME," +
                        "c.UPDATE_RULE,c.DELETE_RULE " +
                    "FROM information_schema.TABLE_CONSTRAINTS a " +
                        "INNER JOIN information_schema.KEY_COLUMN_USAGE b " +
                            "ON a.TABLE_SCHEMA=b.TABLE_SCHEMA AND a.TABLE_NAME=b.TABLE_NAME AND a.CONSTRAINT_NAME=b.CONSTRAINT_NAME " +
                        "LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS c " +
                            "ON b.CONSTRAINT_NAME=b.CONSTRAINT_NAME AND c.TABLE_NAME=b.TABLE_NAME AND c.REFERENCED_TABLE_NAME=b.REFERENCED_TABLE_NAME AND c.CONSTRAINT_SCHEMA=b.CONSTRAINT_SCHEMA " +
                    "WHERE a.TABLE_SCHEMA=? " +
                    "ORDER BY a.TABLE_SCHEMA,a.TABLE_NAME,a.CONSTRAINT_NAME,b.ORDINAL_POSITION,b.POSITION_IN_UNIQUE_CONSTRAINT";
                driver.client.query(sql, [connInfo.database]
                    , function(err, rows, fields) {
                        if (!err) {
                            // {tableName: [rows]}
                            var constraintsMap = _(rows).reduce(function(result, row){
                                if (!(row.TABLE_NAME in result))
                                    result[row.TABLE_NAME] = [];
                                result[row.TABLE_NAME].push(row);
                                return result;
                            }, {});
                            
                            // {tableName: {constaint_name: [rows]}}
                            constraintsMap = _(constraintsMap).reduce(function(newMap, rows, tableName ){
                                newMap[tableName] = _(rows).reduce(function(result, row) {
                                    if (!(row.CONSTRAINT_NAME in result))
                                        result[row.CONSTRAINT_NAME] = [];
                                    result[row.CONSTRAINT_NAME].push({
                                        database  : row.TABLE_SCHEMA,
                                        tableName : row.TABLE_NAME,
                                        colName   : row.COLUMN_NAME,
                                        constraintName : row.CONSTRAINT_NAME,
                                        constraintType : row.CONSTRAINT_TYPE,
                                        iOrdinal       : row.ORDINAL_POSITION,  // col position in combo key, 1 based
                                        refDatabase    : row.REFERENCED_TABLE_SCHEMA,
                                        refTableName   : row.REFERENCED_TABLE_NAME,
                                        refColName     : row.REFERENCED_COLUMN_NAME,
                                        updateRule     : row.UPDATE_RULE,
                                        deleteRule     : row.DELETE_RULE
                                    });
                                    return result;
                                }, {});
                                return newMap;
                            }, {});
                            
                            db.schema.constraints = constraintsMap;
                            
                            function schemaInfoToCols(tableDef, schemanInfos) {
                                return _(schemanInfos).reduce(function(cols, schemaInfo) {
                                    cols.push(tableDef.cols[schemaInfo.colName]);
                                    return cols;
                                }, []);
                            };

                            _(db.schema.tables).each(function(tableDef, tableName) {
                            
                                if (tableName in constraintsMap) {
                                    _(constraintsMap[tableName]).each(function(rows, constraintName){
                                        switch (rows[0].constraintType) {
                                        case 'PRIMARY KEY':
                                            tableDef.primaryKey = schemaInfoToCols(tableDef, rows); // array of col object
                                            break;
                                        case 'UNIQUE':
                                            tableDef.uniqueKeys.push(schemaInfoToCols(tableDef, rows));
                                            break;
                                        case 'FOREIGN KEY':
                                            if (connInfo.database != rows[0].refDatabase) {
                                                externcb(new Error(_("%s.%s: foreign ref to different database (%s) not currently supported").format(connInfo.database, tableName, rows[0].refDatabase)));
                                                return;
                                            }
                                            
                                            // foreign key consists of pair of columns
                                            var fkey = _(rows).reduce(function(pair,  row) {
                                                pair[0].push(tableDef.cols[row.colName]);
                                                pair[1].push(db.schema.tables[row.refTableName].cols[row.refColName]);
                                                return pair;
                                            }, [[], []]);
                                            
                                            tableDef.foreignKeys.push(fkey);
                                            db.schema.tables[rows[0].refTableName].refByKeys.push(fkey);
                                            
                                            break;
                                        }
                                    });
                                }
                            });
                        }
                        self(err);
                    });
            })
            // end of chain, call external externcb
            .seq(function() {
                externcb(null, db);
            })
            // must place catch at the end as we want to abort on any error
            // because catch implicitely continues to next action in chain
            .catch(function(err) {
                externcb(err, db);
            })
            ;
    },
    
    // use ? as place holder for vals
    // i.e. "select * from table where col=?", [1]
    query : function(sql, vals, cb /*function(err, rows, fields) */) {
        return this.client.query(sql, vals, cb);
    },
    
    escape : function(val) {
        return this.client.escape(val);
    },
    escapeLike : function(val) {
        return _(this.client.escape(val)).trim("'");
    },
    
    // render native javascipt objects to values that's uderstood by database
    renderNativeTypes : function(vals) {
        return _(vals).map(function(val) {
            if (val instanceof Date)
                return _('%04d-%02d-%02d %02d:%02d:%02d').format(val.getFullYear(), val.getMonth()+1, val.getDate(), val.getHours(), val.getMinutes(), val.getSeconds());
            else return val;
        });
    },
    
    // convert mysql datetime str into native js Date type
    dateTimeToJs : function(strNativeVal) {
        if (strNativeVal == 'CURRENT_TIMESTAMP')
            return new Date();
        else
            return new Date(strNativeVal);
    },


    // condition is actually a vo object, construcetd via vo's 'static' .key() or .pk() methods
    // example: dal.read(dal.vo.<key>.<table>.key( { id: 1}) )
    // to read all rows in table, specify an empty object, e.g.
    //     dal.read(dal.vo.<key>.<table>.key( {} ))
    read : function(condition, externcb /* function(err, rows) */) {
        var tableName = condition._tableDef.tableName;
        var sql = _("select * from `%s` ").format(tableName);
        
        var cond = this._sqlCond(condition, condition._cols(), externcb);
        if (!cond) return;
        sql += cond.sql;
        var vals = cond.vals;
        
        this.query(sql, this.renderNativeTypes(vals), function(err, rows, fields) {
            externcb(err, rows);
        });
    },
    
    // cols: array of column names
    // return {sql: 'where `a`=?', vals: ['key'] }
    _sqlCond : function(vo, cols, externcb) {
        var sql = '';
        var vals = [];
        if (cols.length>0) {
            sql += "where ";
            sql += _(cols).reduce(function(s, c) {
                if (!(c in vo)) {
                    process.nextTick(function() {
                        externcb(new Error(_("%s.%s is expected but not found").format(vo._tableDef.tableName, c)));
                    });
                    return;
                }
                vals.push(vo[c]);
                s += _("`%s`=? and ").format(c);
                return s;
            }, '');
            sql = _(sql).cut(4);    // remove trailing and
        }
        return {sql: sql, vals: vals};
    },
    
    
    // private, return ['`col`,`col`', '?,?' ]
    _insertSqlFragments : function(cols) {
        return [
            _(cols).map(function(c) {return _('`%s`').format(c)} ).join(','),
            _('?,').chain().multiply(cols.length).cut(1).value()
        ];
    },
    // example:
    // dal.insert(new dal.vo.<key>.<table>({ id:1, name:'me'}), function(err, info) {} )
    insert : function(vo, externcb /* function(err, info) */) {
        var cols = vo._cols();
        var strs = this._insertSqlFragments(cols);
        var sql = _("insert into `%s` (%s) values (%s)").format(vo._tableDef.tableName, strs[0], strs[1]);

        var vals = _(cols).reduce(function(vals, c) { vals.push(vo[c]); return vals; }, []);
        this.query(sql, this.renderNativeTypes(vals), function(err, info){
            if (!err && info.insertId)
                vo[vo._tableDef.autoIncCol.colName] = info.insertId;
            externcb(err, info);
        });
    },
    insertMany : function(arrayOfVo, externcb /* function(err, info) */) {
        var vo = arrayOfVo[0];
        var cols = vo._cols();
        
        var strs = this._insertSqlFragments(cols);
        var sql = _("insert into `%s` (%s) values ").format(vo._tableDef.tableName, strs[0]);
        var vals = [];
        _(arrayOfVo).each(function(vo) {
            sql += _('(%s),').format(strs[1]);
            vals = vals.concat( _(cols).reduce(function(vals, c) { vals.push(vo[c]); return vals; }, []) );
        });
        sql = _(sql).cut(1);

        this.query(sql, this.renderNativeTypes(vals), function(err, info){
            externcb(err, info);
        });
    },
    
    // given a vo, write it to database using its primary key or unique key attribute as condition
    update : function(vo, externcb /*function(err, info) */ ) {
        var tableName = vo._tableDef.tableName;
        
        // auto-determine to use primary key or unique key as condition
        var condKeyNamesSet = this._condOfVo(vo, externcb);
        if (!condKeyNamesSet) return;
        
        var vals = [];
        var sql = _("update `%s` set ").format(tableName);
        _(vo._cols()).each(function(c) {   // construct what to update
            if (!(c in condKeyNamesSet)) {
                sql += _('`%s`=?,').format(c);
                vals.push(vo[c]);
            }
        });
        if (vals.length==0) {
            process.nextTick(function() {
                externcb(new Error(_("%s cant' be updated, because no updatable fields (non-primary or non-unique fields) are found").format(tableName)));
            });
            return;
        }
        sql = _(sql).cut(1) + " ";   // cut off trailing ,
        
        var cond = this._sqlCond(vo, _(condKeyNamesSet).keys(), externcb);
        if (!cond) return;
        sql += cond.sql;
        vals = vals.concat(cond.vals);
        
        this.query(sql, this.renderNativeTypes(vals), function(err, info) {
            externcb(err, info);
        });
    },
    
    // auto-determine to use primary key or unique key as condition
    // return { col1: val, col2: val}
    _condOfVo : function(vo, externcb) {
        var condKeys;
        if (vo._tableDef.primaryKey) condKeys = vo._tableDef.primaryKey;
        else if (vo._tableDef.uniqueKeys.length>0) condKeys = vo._tableDef.uniqueKeys[0];
        if (!condKeys) {
            process.nextTick(function() {
                externcb( new Error(_("%s: no valid condition can be determined").format(vo._tableDef.tableName)));
            });
            return;
        }
        
        return _(condKeys).reduce(function(set,k){ set[k.colName] = vo[k.colName]; return set;}, {});
    },
    
    // given a vo or primary key or unique key, delete from database
    // the vo must ref a table with primary or unique key
    delete : function(condition, externcb /*function(err, info) */) {
        var tableName =condition._tableDef.tableName;
        
        // auto-determine to use primary key or unique key as condition
        var condKeyNamesSet = this._condOfVo(condition, externcb);
        if (!condKeyNamesSet) return;

        var sql = _("delete from `%s` ").format(tableName);
        var cond;
        cond = this._sqlCond(condition, _(condKeyNamesSet).keys(), externcb);
        if (!cond) return;
        sql += cond.sql;

        this.query(sql, this.renderNativeTypes(cond.vals), function(err, info) {
            externcb(err, info);
        });
        
    },
    
    // for mysql this maps to sql: insert on duplicate key update
    // uniqueColNames:  optional, sometimes a table contains multiple unique constraints, use uniqueColnames to specify exactly which
    //                  constraint to use
    write : function(vo, uniqueColNames, externcb /* function(err, info) */) {
        if (_(uniqueColNames).isFunction()) {
            externcb = uniqueColNames;
            uniqueColNames = undefined;
        }
        
        var tableName =vo._tableDef.tableName;
        var condKeyNamesSet ;
        if (uniqueColNames) {
            // todo: ensure uniqueColNames refer to a valid unique constraint!!
            condKeyNamesSet = _(uniqueColNames).reduce(function(o,c) { o[c] = vo[c]; return o;}, {});
        } else {
            condKeyNamesSet = this._condOfVo(vo, externcb);
        }
        if (!condKeyNamesSet) return;
        
        var cols = vo._cols();
        var sql = _("insert into `%s` (%s) values (%s)").format(
            tableName,
            _(cols).map(function(c) {return _('`%s`').format(c)} ).join(','),
            _('?,').chain().multiply(cols.length).cut(1).value() );
        var vals = this.renderNativeTypes(_(cols).reduce(function(vals, c) {
            vals.push(vo[c]);
            return vals;
            }, []));
            
        var updateCols = _(cols).filter(function(c) { return !(c in condKeyNamesSet)} );
        sql += " on duplicate key update ";
        sql += _(updateCols).chain().reduce(function(sql, c) {
                    sql += _("`%s`=?,").format(c);
                    vals.push(vo[c]);
                    return sql;
                }, '').cut(1).value();  // strip tailing ,

        this.query(sql, this.renderNativeTypes(vals), function(err, info) {
            if (!err && info.insertId)
                vo[vo._tableDef.autoIncCol.colName] = info.insertId;
            externcb(err, info);
        })
    },
    
    readAny : function(voCondition, strCondition, externcb /*function(err, rows) */) {
        var tableName = voCondition._tableDef.tableName;
        var sql = _("select * from `%s` ").format(tableName);
        
        var cond = this._sqlCond(voCondition, voCondition._cols(), externcb);
        if (!cond) return;
        sql += cond.sql;
        var vals = cond.vals;
        
        if (cond.sql.length==0 && strCondition && strCondition.length>0)
            sql += "where 1=1";
        if (strCondition) {
            sql += " ";
            sql += strCondition;
        }
        
        this.query(sql, this.renderNativeTypes(vals), function(err, rows, fields) {
            externcb(err, rows);
        });
        
    },
    
    updateAny : function(vo, voCondition, strCondition, externcb /*function(err, rows) */) {
        var tableName = vo._tableDef.tableName;
        
        var vals = [];
        var sql = _("update `%s` set ").format(tableName);
        _(vo._cols()).each(function(c) {   // construct what to update
            sql += _('`%s`=?,').format(c);
            vals.push(vo[c]);
        });
        if (vals.length==0) {
            process.nextTick(function() {
                externcb(new Error(_("%s cant' be updated, because no updatable fields (non-primary or non-unique fields) are found").format(tableName)));
            });
            return;
        }
        sql = _(sql).cut(1) + " ";   // cut off trailing ,
        
        var cond = this._sqlCond(voCondition, voCondition._cols(), externcb);
        if (!cond) return;
        sql += cond.sql;
        if (cond.sql.length==0 && strCondition.length>0)
            sql += "where 1=1";
        sql += " ";
        sql += strCondition;
        vals = vals.concat(cond.vals);
        
        this.query(sql, this.renderNativeTypes(vals), function(err, info) {
            externcb(err, info);
        });
    },
    
    deleteAny : function(voCondition, strCondition, externcb /*function(err, rows) */) {
        var tableName =voCondition._tableDef.tableName;

        var sql = _("delete from `%s` ").format(tableName);
        var cond = this._sqlCond(voCondition, voCondition._cols(), externcb);
        if (!cond) return;
        sql += cond.sql;
        if (cond.sql.length==0 && strCondition.length>0)
            sql += "where 1=1";
        sql += " ";
        sql += strCondition;

        this.query(sql, this.renderNativeTypes(cond.vals), function(err, info) {
            externcb(err, info);
        });
    },
    
    dbSignature : function() {
        return _("%s:%s:%s").format(this.connInfo.host, this.connInfo.port, this.connInfo.database);
    },
    
    // Here is importSql logic
    // sqlFile is separated into two sections, main and patch (always after main),  separated by marker #== PatchBegin
    // furthure more, a db specific marker can be defined inside patch section, in format #== host:port:database
    // importSql will scan the file, save sqls in main section, and sqls in patch section after db marker that matches this mysql connection (if not
    // found, then patch sqls are empty)
    // if patch sqls are empty, then main sqls are executed, otherwise, patch sqls are executed
    // Implied workflow
    // * Define all reference create table statements and reference initial data (insert statements) inside main section
    // * If table/data changes between revisions, make change to sql inside main section, and also
    // * Add the sql statements to alter table or data in the patch section, and for database deployment target, make sure the database marker
    //   is before these statements
    // * Deploy (run importSql)
    // * After deploy, move the database marker to the end (so that you wont' get confused later), and submit into vercion control system.

    // sqlFile : local fs path of the sql file
    importSql : function(sqlFile, cb /* cb(err) */) {
    
        var driver = this;
        var dbSig = this.dbSignature();
        
        var sql='';
        var sqls = [];
        var patchSqls = [];
        
        var inPatchSection = false;
        var inMyPatchSection = false;
        
        var now = new Date();
        new bufferedReader.DataReader(sqlFile, 'utf8')
        .on("error", cb)
        .on("line", function(line) {
            line = _(line).trim();
            if (line.length==0) {}
            else if (/^\#/.test(line)) {    // comment
                if (/^#={2,}\s*Patch\s*Begin/i.test(line)) {
                    inPatchSection = true;
                }
                else {
                    // #== dbhost:3306:my_database
                    var match = /^#={2,}\s*(\S+)$/.exec(line);
                    if (match) {    //
                        var sig = match[1];
                        if (dbSig == sig && inPatchSection) {
                            inMyPatchSection = true;
                            patchSqls = []; // if same db sig encountered multiple times, then next one always reset previous one
                        }
                    }
                }
            }
            else {
                sql += line;
                if (/;$/.test(line)) {	// line ends in ; sql terminator, TODO: what if ; is in comment ?
                    if (sql) {
                        sql = _(sql).trim(';');
                        if (!inPatchSection) sqls.push(sql);
                        else if (inMyPatchSection) patchSqls.push(sql);
                    }
                    sql = '';
                }
            }
        })
        .on("end", function() {
            var runSqls = patchSqls.length>0?patchSqls:sqls;
            
            seq(runSqls)
            .seqEach(function(sql){
                driver.query(sql, this);
            })
            .seq(function() {
                cb();
            })
            .catch(function(err) {
                cb(err);
            })
        })
        .read();
    },

    // return one of [int, float, string, datetime, time, enum, blob]
    categorizeType : function(nativeType) {
        if (/int\b/.test(nativeType))  // decimal, numeric not handled
            return 'int';
        else if (/text\b/.test(nativeType) || nativeType == 'varchar' || nativeType == 'char' )
            return 'string';
        else if (/blob\b/.test(nativeType) || nativeType == 'binary' || nativeType == 'varbinary' )
            return 'blob';
        else if (nativeType == 'float' || nativeType == 'double' || nativeType == 'decimal') // real not handled
            return 'float';
        else if (nativeType == 'date' || nativeType == 'datetime' || nativeType == 'timestamp')
            // year, time unhandled
            return 'datetime';
        else if (nativeType == 'enum')  // set not handled
            return 'enum';
        else if (nativeType == 'time')
            return 'time';
        else
            return null;
    },
    
}

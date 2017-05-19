var seq = require('seq'),
    _ = require("under_score");

/*

dal stands for data access layer.  It provides object layer access to underlying relational data defined in RDBMS,
similar to ORM.  In contrast to ORM, dal does not provide utility to automatically create relational table based
on object definition.  In the opinion of the author, this is a mis-guided and unnecessary 'feature':  allowing developer
to define relational table via defining object is dangerous and usually result in bad database design.  And that
the author is too lazy to implement such feature (the real reason, :)

A simple example:

    dal.init({
        'default' : {
            driver: 'mysql',
            host: 'localhost',
            port: 3306,
            user: 'root',
            password: 'abcd1234'
        }
    }, function(err){
        var db = dal.db.default;    // the default database object
        // suppose database contains table 'user':
        // create table user(
        //   id int primary key auto_increment,
        //   name varchar(255) unique,
        //   group varchar(255),
        //   created datetime);

        // read a row from 'user' table with column id equals 2, change column name to 'smith'
        dal.readOne( db.user.key({id: 2}), function(err, userId2) {
            userId2.name = 'smith';
            dal.update(userId2);
            // or to delete user
            dal.delete(userId2);
        });

        // read all rows from 'user' where group is 'admin'
        dal.read( db.user.key({group: 'admin'}, function(err, users) {
        });

        // read all rows from 'user' where created date is older than 2011-01-01
        dal.readAny( db.user.key({}), "created < '2011-01-01'", function(err, info) {
        });

        // insert a new user 'jane'
        var janeUser = new db.user({name: 'jane', group: 'admin', created: new Date()});
        dal.insert(janeUser, function(err, info) {
            // janeUser.id is automatically set to newly created id because id is auto_increment
            // info is returned by underlying database driver, in this case it contains "insertId" member which
            // equals to janeUser.id
        });

        // create a user 'joe' with id 3, if id 3 already exists in table, override, otherwise insert
        // dal determines the 'existence' by using primary key or unique key defined on table
        dal.write( new db.user({id: 3, name: 'joe', group: 'admin, created: new Date()}), function(err, info) {
            // !err to check if write succeeded
            // info is returned by underlying database driver
        });
    });

Hopefully, the example above is self explanatory, it covers basic operations on a single table.

The key feature of dal is that it "understands" the relationship model of underlying database, during "init",
the relationships are read and stored in memory.  The methods dig/pull/pick/purge takes advantage of this understanding
to allow user perform powerful relationship aware queries.

Lets first define a set of relational tables (used in dal's test):


                          +------------+    +-------------+   +-------------+
                          |   user     |    |  company    |   |  address    |
                          |------------|    |-------------|   |-------------|
                          |   id       |    |  id         |   |  id         |
                          |   name     |    |  name       |   |  value      |
                          |   sex      |    |  created    |   |             |
                          +------------+    +-------------+   +-------------+

 */
module.exports = {
    /*
    after calling init, dal.db is filled with
    {
        <key> : {
            driver : the underlying database driver, see ./driver/ sub folder
            vo     : {}
            schema : {
                 constraints: {tableName : { constraint_name: [colDetails]}},
                 tables  : {
                     <table>  :  {
                            key:         // dal.<key>
                            tableName:   // table name
                            database:    // database name
                            cols:  {
                                <col> : {
                                     tableName  : ,
                                     colName    : ,
                                     type       : data type,
                                     typeLength : mysql col display length (int(11), varchar(20)),
                                     typeData   : null usually, for mysql enum type, array of possible values,
                                     'default'  : <default val>,
                                     nullable    : bool, if column can be null
                                     autoInc    : bool,
                                },
                             },
                             autoIncCol:  // col
                             primaryKey:  // [col, ...]
                             uniqueKeys:  // [ [col], ...]
                             foreignKeys: // [ [ [srcCol], [tgtCol] ], ...]
                             refByKeys:   // [ [ [srcCol], [tgtCol] ], ...]
                     }
                 }
            },
        }
    }
    */
    db  : {},
    
/*
    options: map of key to connInfo:
        key:       the key to map database to
        connInfo:  the database connection info object, 'driver' key is to identify what type of database.
        {
              driver : 'mysql',
              user   : root
              ...
        }
    externcb: function(err)
*/
    init: function(options, externcb) {
        var self = this;
        seq(_(options).keys())
            .seqEach(function(key, i){
                if (key == "driver" || key == "schema" || key=="vo")
                    throw new Error(_("key %s is reserved").format(key));
                self.addDatabase(key, options[key], this);
            })
            .seq(function() {
                externcb();
            })
            .catch(function(err){
                externcb(err);
            })
            ;
    },

    addDatabase: function(key, connInfo, externcb /* function(err, key, db) */) {
        var self = this;
        switch (connInfo.driver) {
        case 'mysql':
            require("./driver/_mysql").create(connInfo, function(err, db){
                if (!err) {
                    
                    self.db[key] = db; // add dal.db.<key>
                    var vos = self.db[key].vo  = {};
                    
                    _(db.schema.tables).each(function(table, name) {
                        var tableDef = db.schema.tables[table.tableName];
                        tableDef.key = key;

                        // .bind pre-specify the first constructor argument to be tableDef
                        var voType = require("./vo").bind(undefined, tableDef);
                        voType.tableDef = tableDef;
                        if (tableDef.primaryKey)
                            voType.pkNames = _(tableDef.primaryKey).map(function(e){ return e.colName});
                        else
                            voType.pkNames = [];
                        
                        /// supply static static helpers for each vo
                        voType.key = function(o) {   // key is just a short cut to create an object
                            return new voType(o, true);
                        }
                        voType.pk = function(o) {
                            if (_(voType.pkNames).compare(_(o).keys()) != 0)   // ensure valid primary key is supplied
                                throw new Error(_("%s primary key is %s, but %s are supplied").format(tableDef.tableName, voType.pkNames, _(o).keys() ) );
                            return new voType(o, true);
                        }
                        
                        // new dal.db.<key>.vo.<table>({key:val})  to construct a vo on the table
                        vos[table.tableName] = voType;
                    });
                    
                    // first database is mapped directly under dal.db
                    if (_(self.db).size()==1) {
                        _(self.db).extend(db);
                    }
                    
                }
                if (externcb) 
                    externcb(err, key, db);
            });
            break;
        default:
            throw new Error(_("Driver %s is not supported").format(connInfo.driver));
            break;
        }
    },
   
    // condition is actually a vo object, constructed via vo's 'static' .key() or .pk() methods
    // example: dal.read(dal.db.<key>.vo.<table>.key( { id: 1}) )
    // to read all rows in table, use readAny
    read : function(condition, externcb /* function(err, rows) */) {
        if (condition._cols().length == 0)
            externcb(new Error(_("readError on %s: no condition specified").format(condition._tableDef.tableName)));
        else {
            var self = this;
            var dbkey = condition._tableDef.key;
            var tableName = condition._tableDef.tableName;
            this.db[dbkey].driver.read(condition, function(err, rows){
                // convert rows to the actual vo object before return, set check = false, as we trust db is always correct
                externcb(err, err?null:_(rows).map(function(e) { return new self.db[dbkey].vo[tableName](e, false) }));
            });
        }
    },
    
    
    readOne : function(condition, externcb /* function(err, row) */ ) {
        if (condition._cols().length == 0)
            externcb(new Error(_("readOneError on table %s: no condition specified").format(condition._tableDef.tableName)));
        else {
            this.read(condition, function(err, rows) {
                if (!err) {
                    if (rows.length!=1)
                        externcb(new Error(_("readOneError on table %s: got %d instead of 1 row").format(condition._tableDef.tableName, rows.length)), rows);
                    else
                        externcb(err, rows[0]);
                }
                else
                    externcb(err, null);
            })
        }
    },
    
    // example:
    // dal.insert(new dal.db.<key>.vo.<table>({ id:1, name:'me'}), function(err, info) {} )
    // if vo contains an auto_increment column, it's auto filled upon callback in driver: before externcb is called)
    insert : function(vo, externcb /* function(err, info) */) {
        this.db[vo._tableDef.key].driver.insert(vo, externcb);
    },
    insertMany : function(arrayOfVo, externcb /* function(err, info) */) {
        if (arrayOfVo.length>0)
            this.db[arrayOfVo[0]._tableDef.key].driver.insertMany(arrayOfVo, externcb);
    },
    
    // given a vo, write it to database using its primary key or unique key attribute as condition
    update : function(vo, externcb /*function(err, info) */ ) {
        // using primary key or unique key as condition
        this.db[vo._tableDef.key].driver.update(vo, externcb);
    },
    
    // given a vo or primary key or unique key, delete from database
    // the vo must ref a table with primary or unique key
    delete : function(condition, externcb /*function(err, info) */ ) {
        if (condition._cols().length == 0)
            externcb(new Error("deleteError: no condition specified"));
        else    
            this.db[condition._tableDef.key].driver.delete(condition, externcb);
    },
    
    // write to table, insert if not exist, update otherwise, using primary or unique key to determine existence
    // for mysql this maps to sql: insert on duplicate key update
    // uniqueColNames: optional, specify which col(s) determine uniqueness
    write : function(vo, externcb /* function(err, info) */, uniqueColNames) {
        this.db[vo._tableDef.key].driver.write(vo, externcb, uniqueColNames);
    },
    
    // support customized sql condition directly specified in string
    readAny : function(voCondition, strCondition, externcb /*function(err, rows) */) {
       var self = this;
       var dbkey = voCondition._tableDef.key;
       var tableName = voCondition._tableDef.tableName;
       this.db[voCondition._tableDef.key].driver.readAny(voCondition, strCondition, function(err, rows){
            // convert rows to the actual vo object before return, set check = false, as we trust db is always correct
            externcb(err, err?null:_(rows).map(function(e) { return new self.db[dbkey].vo[tableName](e, false) }));
        });
    },
    
    // vo : what to update
    updateAny : function(vo, voCondition, strCondition, externcb /*function(err, rows) */) {
        this.db[voCondition._tableDef.key].driver.updateAny(vo, voCondition, strCondition, externcb);
    },
    
    deleteAny : function(voCondition, strCondition, externcb /*function(err, rows) */) {
        this.db[voCondition._tableDef.key].driver.deleteAny(voCondition, strCondition, externcb);
    },

    // dig: a special read that's aware of foreign key relationship, and dig into column that references other tables
    // vo:  the vo object to dig
    // level: how many ref levels to recursively dig, 0: none, 1: dig direct ref, 2: dig ref's ref, ... so on, default: infinite
    //
    // the digged object stored as
    // vo._dig = { colName : <vo> }
    //    * if ref key contains > 1 cols, the first colName is used
    dig : function(vo, externcb /*function(err)*/, level) {   // default no level limit
        if (level === undefined)
            level = -1;
        if (level==0) {
            externcb(null);
            return;
        }
        var dal = this;
        vo._dig = {};
        seq()
            .seq( function() {
                this(null, vo._tableDef.foreignKeys);                
            })
            .flatten(false) // expand vo._tableDef.foreignKeys so that seqEach can iterate
            .seqEach( function(fkey, i) {
                var seq = this;
                var srcCols = fkey[0];
                var tgtCols = fkey[1];
                
                var condition = srcCols.reduce(function(o, col, i) { 
                        o[tgtCols[i].colName] = vo[col.colName]; 
                        return o;
                    }, {});
                var voCondition = dal.db[vo._tableDef.key].vo[tgtCols[0].tableName].key(condition);
                
                dal.readOne(voCondition, function(err, otherVo) {
                    if (!err)
                        vo._dig[srcCols[0].colName] = otherVo;
                    seq(err, otherVo);
                });
            })
            .seq(function(){    // place all vos returned by seqEach above into a single array
                var allvos = _(this.args).chain().values().reduce(function(array, v) { return array.concat(v[0]); }, []).value();
                this(null, allvos);
            })
            .flatten(false)
            .seqEach( function(vo, i) {
                var seq = this;
                dal.dig(vo, function(err) {
                    seq(err);
                }, level-1);
            })
            .seq(function() {
                externcb(null);
            })
            .catch(function(err) {  
                externcb(err);
            })
            ;
    },

    // pull: a special read that's aware of foreign key relationship, and 'pull' all other vo that depend on this vo
    // level: how many ref levels to recursively pull, 0: none, 1: pull vo's direct dependents only, 2: pull dep's deps, ... so on, default is infinite
    // the pulled objects are stored as:
    // vo._pull = {othertable.col1.col2 : [ subvo, ]}
    pull : function(vo, externcb /*function(err)*/, level) {   // default no level limit
        if (level === undefined)
            level = -1;
        if (level==0) {
            externcb(null);
            return;
        }
        
        var dal = this;
        vo._pull = {};
        seq()
            .seq( function() {
                this(null, vo._tableDef.refByKeys);                
            })
            .flatten(false) // expand vo._tableDef.refByKeys so that seqEach can iterate
            .seqEach( function(fkey, i) {
                var seq = this;
                var srcCols = fkey[0];
                var tgtCols = fkey[1];
                
                var depName = srcCols[0].tableName + "." + _(srcCols).chain().reduce(function(s, c){ s+=c.colName; s+='.'; return s;}, '').cut(1).value();
                
                var condition = tgtCols.reduce(function(o, col, i) { 
                    o[srcCols[i].colName] = vo[col.colName]; 
                    return o;
                }, {});
                var voCondition = dal.db[vo._tableDef.key].vo[srcCols[0].tableName].key(condition);
                
                dal.read(voCondition, function(err, vos) {
                    if (!err)
                        vo._pull[depName] = vos;
                    seq(err, vos);
                });
            })
            .seq(function(){
                // vo._pull contains 'reftable.col' => [row]
                // but if reftable is unique: usually the case unless reftable has multiple foreign key relationship with this table
                // then for easy access, we compact vo._pull to 'table' => row, or 'table' => [rows]
                var compacted = {};
                _(vo._pull).each(function(v,k) {
                    var tableName = /^(.*?)\./.exec(k)[1];
                    if (tableName in compacted) delete compacted[tableName];
                    else compacted[tableName] = [k, (v.length==1?v[0]:v)];
                });
                _(compacted).each(function(v,k) {
                    delete vo._pull[ v[0] ];
                    vo._pull[k] = v[1];
                });

                // for recursion, iterate through all pull-ed vos and call pull again
                this(null, _(this.args).flatten());
            })
            .flatten(false)
            .seqEach( function(vo, i) {
                var seq = this;
                dal.pull(vo, function(err) {
                    seq(err);
                }, level-1);
            })
            .seq(function() {
                externcb(null);
            })
            .catch(function(err) {  
                externcb(err);
            })
            ;
    },
    
    // param condition: the pick condition, must be a subset of first table in parameter path
    // paths:  an array of tables that are connected (adjacent tables has a foreign key relationship)
    // cb:  function(err, rows), rows is a list of vos read from last table in path
    // return : undefined
    // example:
    //    dal.pick({id: 1}, ['table1', 'table2', 'table3'], function(err, table3_rows) {} );
    //
    // pick can be used in place of sql join, it has these advantages over native sql join
    // 1. clean simple interface, no need to write the join query
    // 2. can take advantages of read cache if used
    //
    pick : function(condition, paths, cb) {
        var dal = this;
        
        var anchorTable = paths[0];
        var voCondition = new dal.db.vo[anchorTable](condition);
        dal.read(voCondition, function(err, rows) {
            
            if (err)
                cb(err);
            else if (paths.length == 1) {
                cb(err, rows);
            }
            else {
                var leftTableDef = dal.db.schema.tables[anchorTable];
                var rightTableDef = dal.db.schema.tables[paths[1]];
                
                function findRelation(table1, table2) {
                    
                    for (var i=0,l=table1.foreignKeys.length; i<l; i++) {
                        var pair = table1.foreignKeys[i];
                        var leftCols  = pair[0],
                            rightCols = pair[1];
                            
                        if (rightCols[0].tableName == table2.tableName) {
                            return [_(leftCols).reduce(function(all, c) { all.push(c.colName); return all; }, []), 
                                    _(rightCols).reduce(function(all, c) { all.push(c.colName); return all; }, [])];
                        }
                    };
                    return [null,null];
                }

                // relation => [ leftcols, rightcols ]
                // where leftcols: an array of col names in table specified by paths[0]
                //      rightcols: an array of col names in table specified by paths[1], 
                // leftcols and rightcols have foreign key relationship, direction could be either way
                var relation = findRelation(leftTableDef, rightTableDef);
                if (!relation[0])
                    relation = _(findRelation(rightTableDef, leftTableDef)).swapAt(0,1);
                
                if (!relation[0]) {
                    cb(new Error(_("No relationship between %s and %s").format(anchorTable, paths[1])));
                }
                else {
                    var subPaths = paths.slice(1);
                    
                    var results = [];  // result is saved here
                    
                    seq(rows)
                    .seqEach(function(row) {
                        var seq = this;
                        // translate lhs result to rhs table condition
                        var leftCond = _(row).pickKeys(relation[0]);

                        var rightCond = _(relation[0]).reduce(function(memo, leftKey, i) {
                            memo[relation[1][i]] = leftCond[leftKey];
                            return memo;
                        }, {});
                        
                        dal.pick(rightCond, subPaths, function(err, prows) {
                            if (!err) _(results).append(prows);
                            seq(err);
                        });
                    })
                    .seq(function() {
                        cb(null, results);
                    })
                    .catch(function(err){
                        cb(err);
                    })
                    ;
                }
            }
        });
    },
    
    // place all dependent vos in a single array
    flattenPulledVos : function(vo) {
        if (!vo._pull) return [];
        var dal = this;
        var directDependents = _(vo._pull).chain().values().flatten().value();
        return directDependents.reduce(function(all, vo) {return all.concat(dal.flattenPulledVos(vo)) }, directDependents);
    },
    
    // delete vo and all other vo directly or in-directly reference it
    // this is similar to 'ON DELETE CASCADE' option in sql definition.
    // purge delete ensure referential integrity is preserved when row is deleted from database
    // !! be careful with purge, you may inadvertently delete more than what you mean
    // * To get preview of what you may delete, use code:
    //   dal.pull(vo, function(err) {
    //       console.log(dal.flattenPulledVos(vo));
    //   })
    purge : function(vo, externcb/*function (error) */ ) {
        var dal = this;
        this.pull(vo, function(err) {
            if (!err) {
                seq()
                .seq( function() {
                    this(null, [vo].concat(dal.flattenPulledVos(vo)).reverse());
                })
                .flatten(false)
                .seqEach(function(vo, i){
                    dal.delete(vo, this);
                })
                .seq(function() {
                    externcb(null);
                })
                .catch(function(err) {  
                    externcb(err);
                })
                ;                
            }
            else
                externcb(err);
        })
    },    

    // convert (recursively if necessary) all 'vo' instances to native js object instances.
    // remove hidden properties _tableDef, _name from vo
    purifyVo : function(o) {
        var dal = this;
        
        if (_(o).isObject() && o._tableDef) {
            return _(o).reduce(function(newo, v, k) {
                if ((k[0]!='_'))
                    newo[k] = (_(v).isObject() && o._tableDef) ? dal.purifyVo(v) : v;
                return newo; 
            }, {});
        }
        else if (_(o).isArray())
            return _(o).reduce(function(ret, e) { ret.push(dal.purifyVo(e)); return ret;}, []);
        else
            return o;
    },

    __permanent : true
};

 
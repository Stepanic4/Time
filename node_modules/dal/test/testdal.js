var seq = require('seq');
var mysql = require('mysql');
var dal = require("../lib/dal");
var vo  = require("../lib/vo");
var assert = require('assert');
var _ = require("under_score");
var path = require("path");

function test(title, fn) {
    console.log(title + ": testing");
    try {
        fn();
        console.log(title + ": passed");
    }
    catch (e) {
        console.log(title + ": error");
        console.log(e);
    }
}

var connInfo = {
	driver: 'mysql',
	host: 'localhost',
	port: 3306,
	user: 'root',
	password: 'abcd1234',
};

// setup a test database, roughly:
// company -< user
// user >- address
// company >- address
// address - gps position
var setupSqls = [
    'drop database if exists dal_test',
    'create database dal_test',
    'use dal_test',
    'create table user (id int not null auto_increment primary key, name varchar(200), sex enum("male", "female") not null) engine=INNODB',
    'create table address (id int not null auto_increment primary key, value varchar(200)) engine=INNODB',
    'create table user_address(user_id int not null, address_id int not null, foreign key (user_id) references user(id), foreign key (address_id) references address(id)) engine=INNODB',
    'create table company (id int not null auto_increment primary key, name varchar(200), created datetime not null) engine=INNODB',
    'create table company_address(company_id int not null, address_id int not null, foreign key (company_id) references company(id), foreign key (address_id) references address(id)) engine=INNODB',
    // user >- company,  many to one, 1 company can have many users, user can belong to one company only 
    'create table company_user(company_id int not null, user_id int not null, foreign key (company_id) references company(id), foreign key (user_id) references user(id)) engine=INNODB',
    'create table address_gps(address_id int not null primary key, latitude double not null, longitude double not null, foreign key (address_id) references address(id)) engine=INNODB',
    
    // setup test data
    'insert into company (id, name,created) values (1, "acme",CURRENT_TIMESTAMP),(2, "simpson",CURRENT_TIMESTAMP)',
    'insert into user (id, name, sex) values (1, "joe", "male"),(2, "jane", "female"),(3,"bart", "male"),(4,"homer", "male")',
    'insert into company_user (company_id, user_id) values (1, 1),(1,2),(2,3),(2,4)',
    'insert into address (id,value) values (1,"joe place"),(2,"jane place"),(3,"simpson place"),(4, "acme place"),(5, "acme place2")',
    'insert into user_address (user_id, address_id) values (1,1),(2,2),(3,3),(4,3)',
    'insert into company_address (company_id, address_id) values (1,4),(1,5),(2,3)',
    'insert into address_gps (address_id, latitude, longitude) values (1,1,1),(2,2,2),(3,3,3),(4,4,4)',
];

var client = require("mysql").createConnection(connInfo);

module.exports["Setting up mysql database"] = function(test) {
    seq(setupSqls)
    .flatten(false)
    .seqEach(function(sql) {
        client.query(sql, this);
    })
	.seq(function() {
		test.done();
	})
    .catch(function(err) {
        test.ifError(err);
        test.done();
    })
    ;
}

module.exports["Testing dal..."] = function(test) {
seq()
    .seq(function() {
        connInfo.database = 'dal_test';
        dalMysqlTest(connInfo, this);
    })
	.seq(function() {
		test.done();
	})
    .catch(function(err) {
        test.ifError(err);
        test.done();
    })
    ;
}

module.exports["Testing mysql sql import..."] = function(test) {
seq()
	.seq(function() {
		importSqlTest(connInfo, this);
	})
	.seq(function() {
		test.done();
	})
    .catch(function(err) {
        test.ifError(err);
        test.done();
    })
    ;
}


function importSqlTest(connInfo, cb) {	
    dal.init({
        'test': connInfo
    }, function(err) {
        if (err) {
            console.log("Errr in initializing schema:")
            console.log(err);
        }
        else {
            var fs = require("fs");
            var file = path.join(__dirname, "testimport.sql");
            seq()
            .seq(function() {
                fs.writeFile(file, 
                "create table test_import_table1(id int auto_increment primary key, name varchar(255));\n"
                ,
                this);
            })
            .seq(function() {
                dal.db.test.driver.importSql(file, this);
            })
            .seq(function() {
                fs.writeFile(file, 
                "create table test_import_table1(id int auto_increment primary key, name varchar(255));\n" +
                "#== PatchBegin\n" +
                _("#== %s\n").format(dal.db.test.driver.dbSignature()) + 
                "alter table test_import_table1 add description varchar(255) after name;"
                ,
                this);
                
            })
            .seq(function() {
                dal.db.test.driver.importSql(file, this);
            })
            .seq(function() {
                cb();
            })
            .catch(function(err) {
                cb(err);
            });
		}
	});
}
	
function dalMysqlTest(connInfo, cb) {
    dal.init({
        'test': connInfo
    }, function(err) {
        if (err) {
            console.log("Errr in initializing schema:")
            console.log(err.stack || err);
        }
        else {
            
            test('verify relationship', function() {
                var relationTable = {};
                _(dal.db.test.schema.tables).each(function(table, name){
                    if (table.refByKeys.length>0) {
                        _(table.refByKeys).each (function(pair) {
                            relationTable[_("%s.%s").format(pair[0][0].tableName, pair[0][0].colName)] =
                            _("%s.%s").format(pair[1][0].tableName, pair[1][0].colName); 
                        });
                    }
                });
                assert.equal(relationTable['address_gps.address_id'] ,'address.id');
                assert.equal(relationTable['company_user.company_id'], 'company.id');
                assert.equal(relationTable['user_address.user_id'], 'user.id');
            });
            
    
            function onChain(seq, err) {
                if (err)
                    console.log(err.stack || err);
                //seq(err);
                seq(null);  // continue on to next test case
            }
            
            seq()
            .seq( function()  {
                var seq = this;
                console.log('insert: testing');
                var vo = new dal.db.test.vo.company({ name : 'test company', created: new Date() });
                dal.insert(vo, function(err, info){
                    seq.vars.vo = vo;
                    assert.equal(typeof vo.id, 'number');
                    onChain(seq, err);
                } );
            })
            .seq( function() {
                var seq = this;
                console.log('read: testing');
                dal.read(dal.db.test.vo.company.key({name : 'test company'}), function(err, vos) {
                    assert.equal(vos.length, 1);
                    onChain(seq, err);
                });
            })
            .seq( function() {
                var seq = this;
                seq.vars.vo.name = 'updated test company';
                console.log('update: testing');
                dal.update(seq.vars.vo, function(err, info) {
                    onChain(seq, err);
                });
            })
            .seq( function() {
                var seq = this;
                console.log('delete: testing');
                dal.delete(seq.vars.vo, function(err, info) {
                    onChain(seq, err);
                });
            })
            .seq( function() {
                var seq = this;
                console.log('insert many: testing');
                
                seq.vars.vos = [
                    new dal.db.test.vo.user({id : 10, name: 't1', sex: 'male'}), 
                    new dal.db.test.vo.user({id : 11, name: 't2', sex: 'female'}),
                ];
                
                dal.insertMany(seq.vars.vos, function(err, info) {
                    onChain(seq, err);
                });
            })
            .seq( function() {
                var seq = this;
                console.log('write: testing');
                
                seq.vars.vos[0].name = 'written name'
                
                dal.write(seq.vars.vos[0], function(err, info) {
                    onChain(seq, err);
                });
            })
            .seq( function() {
                var seq = this;
                console.log('readAny: testing');
                
                dal.readAny(dal.db.test.vo.company.key({}), 'limit 10', function (err, rows) {
                    assert.equal(rows.length, 2);
                    onChain(seq, err);
                });
            })             
            .seq( function() {
                var seq = this;
                console.log('updateAny: testing');
                
                dal.updateAny(
                    new  dal.db.test.vo.company({name: 'update any'}),
                    dal.db.test.vo.company.key({}), 
                    'and id>2 limit 10', 
                    function (err, info) {
                        onChain(seq, err);
                    });
            })
            .seq( function() {
                var seq = this;
                console.log('readOne: testing');
                
                dal.readOne(
                    dal.db.test.vo.company.key({id:2}), 
                    function (err, company) {
                        assert.equal(company.name, 'simpson');
                        onChain(seq, err);
                    });
            })
            .seq( function() {
                var seq = this;
                console.log('deleteAny: testing');
                
                dal.deleteAny(
                    dal.db.test.vo.company.key({}),
                    "and id >= 10" ,
                    function (err, info) {
                        onChain(seq, err);
                    });
            })
            .seq( function() {
                var seq = this;
                console.log('pull: testing');
                
                 dal.readOne(dal.db.test.vo.address.key({id:3}), function(err, address){
                     dal.pull(address, function(err) {
                         assert.equal(dal.flattenPulledVos(address).length, 4);
                         assert.equal(address._pull.address_gps.latitude, 3);
                         onChain(seq, err);
                     })
                 } );
            })
            .seq( function() {
                var seq = this;
                console.log('dig: testing');
                
                 dal.readOne(dal.db.test.vo.company_address.key({company_id:2}), function(err, address){
                     dal.dig(address, function(err) {
                         assert.equal(address._dig.company_id.name, 'simpson');
                         onChain(seq, err);
                     })
                 } );
            })
            .seq( function() {
                var seq = this;
                console.log('pick: testing');
                
                dal.pick({id: 1}, ['user', 'user_address', 'address'], function(err, rows) {
                    if (!err) {
                        assert.equal(rows[0].value, 'joe place');
                    }
                    onChain(seq, err);
                });
            })
            .seq( function() {
                var seq = this;
                console.log('pick: testing 2');
                
                dal.pick({id: 1}, ['company', 'company_address', 'address'], function(err, rows) {
                    if (!err) {
                        assert.equal(rows[0].value, 'acme place');
                        assert.equal(rows[1].value, 'acme place2');
                    }
                    onChain(seq, err);
                });
            })
            .seq( function() {
				cb();
            })
            .catch(function(err) {  
                cb(err);
            })
            ;    
            
        }
    });
}

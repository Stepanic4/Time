create table test_import_table1(id int auto_increment primary key, name varchar(255));
#== PatchBegin
#== localhost:3306:dal_test
alter table test_import_table1 add description varchar(255) after name;
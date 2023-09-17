CREATE TABLE IF NOT EXISTS "config" ( id integer primary key , key char(80) unique , value char(200) );
CREATE TABLE IF NOT EXISTS "users" ( id integer primary key , email char(80) unique , password char(80) , active boolean );
CREATE TABLE IF NOT EXISTS "dag" ( id integer primary key autoincrement ,  name char(80) unique , active boolean , bfield text );
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE deleted_dag ( id integer , name char(80) , active boolean , bfield text , dt timestamp default current_timestamp );
CREATE TABLE IF NOT EXISTS "task" ( id integer primary key autoincrement , pid integer , name char(80) , operator  char(80) , body text , position  text , bfield text);
CREATE UNIQUE INDEX tname on task ( pid , name ) ;
CREATE TABLE IF NOT EXISTS "relation" ( id integer primary key autoincrement , pid integer , src char(80) , tgt char(80) );
CREATE TABLE IF NOT EXISTS "operator" ( id integer primary key autoincrement , name char(80) unique , tags char(80) , base char(40) , link char(40) , udfield text );
CREATE TABLE history_task ( id integer , pid integer , name char(80) , operator char(80) , body text , position text , bfield text , dt timestamp default current_timestamp , activity char(1) );
CREATE TABLE history_relation ( id integer , pid integer , src char(80) , tgt char(80) , dt timestamp default current_timestmap , activity char(1)  );

CREATE TRIGGER delete_dag BEFORE DELETE ON dag
BEGIN
    insert into history_dag ( id , name , active , bfield , activity ) select *,'D' from dag  where id = old.id ;
    insert into history_relation ( id , pid , src , tgt , activity ) select *,'D' from relation  where pid = old.id ;
    insert into history_task ( id , pid , name , operator , body , position , bfield , activity ) select *,'D' from task  where pid = old.id ;
    delete from relation where pid = old.id ;
    delete from task where pid = old.id ;
END;

CREATE TRIGGER update_operator AFTER UPDATE ON operator
BEGIN
  INSERT INTO history_operator ( id , name , tags , base , link , udfield , activity ) values ( new.id , new.name , new.tags , new.base , new.link , new.udfield , 'U' ) ;
END;

CREATE TABLE history_operator ( id integer , name char(80) , tags char(80) , base char(40) , link char(40) , udfield text , dt timestamp default current_timestamp , activity char(1) );
CREATE TABLE history_dag ( id integer , name char(80) , active boolean , bfield text , dt timestamp default current_timestamp , activity char(1) );

CREATE TRIGGER operator_rename AFTER UPDATE ON operator WHEN old.name <> new.name
BEGIN
	update task set operator = new.name where operator = old.name ;
	update operator set link = new.name where link = old.name ;
	update operator set base = new.name where base = old.name ;
END;

CREATE TRIGGER operator_update_baseop AFTER UPDATE ON operator WHEN old.baseop <> new.baseop
BEGIN
	update task set operator = new.name where operator = old.name ;
	update operator set link = new.name where link = old.name ;
	update operator set base = new.name where base = old.name ;
END;

CREATE TRIGGER op_delete AFTER DELETE ON operator
BEGIN
    update operator set base = null where base = old.name ;
END;

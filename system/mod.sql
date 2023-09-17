

-- select id,name,json_extract(udfield,'$.baseop.default') from operator where name = 'Clone_Python' ;

-- json_set(sensor_data2.data,'$.humidity',59)

-- select id , name , operator , json_extract ( body , '$.detail.baseop' ) from task where operator in ( select name from operator where link = 'Python' ) or operator = 'Python' ;


CREATE TABLE IF NOT EXISTS "dummy" (  val text  );
delete from dummy ;


drop trigger operator_change_udfield ;

-- update task -> body -> baseop -> value to this operator -> udfield -> baseop -> value *IF*  the task has the same old value
--		body = json_set ( body , '$.detail.baseop' , json_extract ( new.udfield , '$.default' ) )
---
--- if a task baseop is using the default operator/baseop,  when the operator/baseop changes , set the
--- task baseop to the new default.  if the task is not using default operator/baseop, leave it as is
---
CREATE TRIGGER operator_change_udfield AFTER UPDATE ON operator WHEN old.udfield <> new.udfield
BEGIN
    update task set 
		body = json_set ( body , '$.detail.baseop' , json_extract( new.udfield , '$.baseop.default' ) )
	where 
		( operator = new.name or operator in ( select name from operator where link = new.name )  )
		and	json_extract ( body , '$.detail.baseop' ) = json_extract ( old.udfield , '$.baseop.default' )  ;

    update task set 
		body = json_set ( body , '$.detail.runnable' , json_extract( new.udfield , '$.runnable.default' ) )
	where 
		( operator = new.name or operator in ( select name from operator where link = new.name )  )
		and	json_extract ( body , '$.detail.runnable' ) = json_extract ( old.udfield , '$.runnable.default' )  ;

	insert into dummy values ( json_extract( old.udfield , '$.baseop.default' ) ) ;
	insert into dummy values ( json_extract( new.udfield , '$.baseop.default' ) ) ;
	insert into dummy values ( json_extract( old.udfield , '$.runnable.default' ) ) ;
	insert into dummy values ( json_extract( new.udfield , '$.runnable.default' ) ) ;
END;

-- update operator set udfield = json_set ( udfield , '$.baseop.default' , 'aaa10' ) where name = 'Clone_Python' ;

-- select * from dummy ;


select * from task where operator = 'Clone_Python' ;

-- select * from operator where name = 'Clone_Python' ;



.exit 0 ;


drop table newtask ;

CREATE TABLE newtask ( id integer primary key autoincrement , pid integer , name char(80) , operator  char(80) , baseop text , runnable text , body text , position  text , bfield text);

insert into newtask ( id , pid , name , operator , body , position , bfield ) select id , pid , name , operator , body , position , bfield from task ;

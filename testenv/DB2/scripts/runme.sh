#!/usr/bin/bash

cat << 'EOF' > /tmp/create_table.sql
connect to sample ;

drop table cars ;

CREATE TABLE CARS (
      id integer not null GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) ,
      make     char(20) ,
      model    char(20) ,
      year     int ,
      price    decimal(7,2)
);

insert into CARS ( make , model , year , price ) values ( 'Toyota' , 'Prius' , 2020 , 24000 ) ;
insert into CARS ( make , model , year , price ) values ( 'Toyota' , 'Avensis' , 2013, 12000) ;
insert into CARS ( make , model , year , price ) values ( 'Toyota' , 'Camry' , 2022, 25000) ;

insert into CARS ( make , model , year , price ) values ( 'Honda' , 'Accord' , 2022, 25000) ;

insert into CARS ( make , model , year , price ) values ( 'Nissan' , 'Xtrail' , 2022, 25000) ;
EOF

cat <<EOF > /tmp/doit.sh
echo Output of runme

db2pd -

db2 update dbm cfg using SSL_SVR_KEYDB /keystore/mydbserver.kdb
db2 update dbm cfg using SSL_SVR_STASH /keystore/mydbserver.sth
db2 update dbm cfg using SSL_SVR_LABEL myselfsigned
db2 update dbm cfg using SSL_SVCENAME 60002

db2stop

db2set DB2COMM=SSL,TCPIP

db2start


db2 -tvf /tmp/create_table.sql
EOF



chmod +x /tmp/doit.sh

su - db2inst1 -c /tmp/doit.sh



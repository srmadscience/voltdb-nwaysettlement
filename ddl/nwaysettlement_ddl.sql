 
DROP PROCEDURE CompoundPayment IF EXISTS;

drop procedure bl_transaction_status if exists;

 DROP PROCEDURE GetStats__promBL IF EXISTS;
 
 DROP TASK EndOrphanedTransactionsTask IF EXISTS;
 

 
 DROP PROCEDURE EndOrphanedTransactions IF EXISTS;
 
 DROP PROCEDURE TimeoutTransactions IF EXISTS;
 
 DROP PROCEDURE StartTransactionPayee IF EXISTS;
 
 DROP PROCEDURE StartTransactionPayer IF EXISTS;
 
 DROP PROCEDURE EndSpecificTransactionWithErrors IF EXISTS;
 
 DROP PROCEDURE SetTransactionEntryDone IF EXISTS;
 
 DROP PROCEDURE SetPayerDone IF EXISTS;
 
 DROP STREAM transaction_failures IF EXISTS;
 
DROP STREAM transaction_fixes IF EXISTS;
 
DROP PROCEDURE GetBalance IF EXISTS;

drop view transaction_status if exists;

DROP VIEW vw_users_per_pending_transactions IF EXISTS;
DROP view transasction_net_changes IF EXISTS;
   
 DROP TABLE user_balances IF EXISTS;
 
DROP TABLE user_transactions IF EXISTS;
  
DROP table promBL_latency_stats IF EXISTS;
 
 
 CREATE TABLE promBL_latency_stats
 (statname varchar(128) not null
 ,stathelp varchar(128) not null
 ,event_type varchar(128) not null
 ,event_name varchar(128) not null
 ,statvalue  bigint not null
 ,lastdate timestamp not null
 ,primary key (statname, event_type, event_name))
 USING TTL 1 MINUTES ON COLUMN lastdate;
 
 CREATE INDEX pls_ix1 ON promBL_latency_stats(lastdate);
 
 PARTITION TABLE promBL_latency_stats ON COLUMN statname;
 
 
 Create table user_balances
(Userid bigint not null primary key
,Balance_amount bigint not null
,Balance_date   timestamp not null);

PARTITION TABLE user_balances ON COLUMN userid;


Create table user_transactions
(Userid bigint not null 
,Other_userid bigint not null
,tran_amount bigint not null
,Transaction_id bigint not null
,Effective_date timestamp not null
,stale_date timestamp not null
,tran_status varchar(20) not null
,tran_status_explanation varchar(1000) 
,queue_date timestamp
,insert_date TIMESTAMP DEFAULT NOW
,user_count bigint
,done_date timestamp
,PRIMARY KEY (Userid,Transaction_id));

CREATE INDEX ut_ix1 ON user_transactions (Transaction_id);


CREATE INDEX ut_ix3 ON user_transactions (tran_status,insert_date);

CREATE INDEX ut_ix4 ON user_transactions (tran_status,Effective_date);

CREATE INDEX ut_ix5 ON user_transactions (Effective_date,tran_status);

CREATE INDEX ut_ix6 ON user_transactions (userid,tran_status);

PARTITION TABLE user_transactions ON COLUMN userid;

CREATE VIEW vw_users_per_pending_transactions AS
SELECT transaction_id, effective_date, count(*) how_many
FROM user_transactions 
WHERE tran_status = 'PENDING'
GROUP BY transaction_id,effective_date;


CREATE INDEX vup_ix1 ON vw_users_per_pending_transactions(effective_date, Transaction_id);


create view transaction_status as 
select tran_status, min(Effective_date) min_effective_date, 
max(Effective_date) max_effective_date,
count (*) how_many 
from user_transactions group by tran_status;

create view transasction_net_changes as
select Transaction_id, sum(tran_amount) tran_amount, count(*) how_many
from user_transactions
where tran_status = 'DONE'
group by Transaction_id;

create index transasction_net_changes_idx1 on transasction_net_changes(tran_amount);

create procedure bl_transaction_status as select  'bl_transaction_status' statname
, 'a help message' stathelp 
,tran_status status 
,  how_many  statvalue from transaction_status;

CREATE STREAM transaction_failures 
PARTITION ON COLUMN Transaction_id 
EXPORT TO TOPIC transaction_failures_topic  
(Transaction_id bigint not null
,Effective_date timestamp 
,tran_status varchar(10) 
,desc varchar(1000));

CREATE STREAM transaction_fixes
PARTITION ON COLUMN Transaction_id 
export to TOPIC transaction_fixes_topic
(Transaction_id bigint not null
,Effective_date timestamp 
,tran_status varchar(10) 
,desc varchar(1000));



load classes ../jars/nwayprocs.jar;

CREATE PROCEDURE 
   PARTITION ON TABLE user_balances COLUMN userid
   FROM CLASS nwayprocedures.StartTransactionPayer;  
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_balances COLUMN userid
   FROM CLASS nwayprocedures.StartTransactionPayee; 

CREATE PROCEDURE 
   PARTITION ON TABLE user_balances COLUMN userid
   FROM CLASS nwayprocedures.EndSpecificTransactionWithErrors;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_balances COLUMN userid
   FROM CLASS nwayprocedures.SetTransactionEntryDone;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_balances COLUMN userid
   FROM CLASS nwayprocedures.SetPayerDone;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_balances COLUMN userid
   FROM CLASS nwayprocedures.GetBalance; 
   
CREATE PROCEDURE 
   FROM CLASS nwayprocedures.EndOrphanedTransactions;  
   
drop task EndOrphanedTransactionsTask if exists;
   
CREATE TASK EndOrphanedTransactionsTask 
ON SCHEDULE DELAY 500 MILLISECONDS
PROCEDURE  EndOrphanedTransactions
WITH ('100', '60000') 
ON ERROR LOG RUN ON DATABASE ENABLE;   



CREATE PROCEDURE GetStats__promBL AS
BEGIN
select  statname, stathelp, event_type, event_name, statvalue 
from promBL_latency_stats
order by statname, stathelp, event_type, event_name, statvalue ;
select  'bl_transaction_status' statname
, 'bl_transaction_status' stathelp 
,tran_status status 
,  how_many  statvalue from transaction_status;
END;

   
CREATE  PROCEDURE  FROM CLASS nwayprocedures.CompoundPayment;



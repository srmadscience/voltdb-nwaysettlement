load classes ../jars/nwayprocs.jar;

file voltdb-nwaysettlement-removeDB.sql;
 
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

CREATE INDEX ut_ix1 ON user_transactions (Transaction_id,tran_status);

CREATE INDEX ut_ix2 ON user_transactions (Userid,tran_status);

CREATE INDEX ut_ix3 ON user_transactions (tran_status,insert_date);

CREATE INDEX ut_ix4 ON user_transactions (tran_status,Effective_date);

PARTITION TABLE user_transactions ON COLUMN userid;

CREATE TABLE promBL_latency_stats
 (statname varchar(128) not null
 ,stathelp varchar(128) not null
 ,event_type varchar(128) not null
 ,event_name varchar(128) not null
 ,statvalue  bigint not null
 ,lastdate timestamp not null
 ,primary key (statname, event_type, event_name))
 USING TTL 2 MINUTES ON COLUMN lastdate;
 
 CREATE INDEX pls_ix1 ON promBL_latency_stats(lastdate);
 
 PARTITION TABLE promBL_latency_stats ON COLUMN statname;
 

-- used by TestClient
create view transaction_status as 
select tran_status, min(Effective_date) min_effective_date, 
max(Effective_date) max_effective_date,
sum(tran_amount) net_amount,
count (*) how_many 
from user_transactions group by tran_status;

-- Used to spot broken transactions
create view transaction_changes as
select Transaction_id, sum(tran_amount) net_tran_amount, sum(user_count) planned_participants, count(*) actual_participants
from user_transactions
WHERE tran_status IN ('PENDING','PAYERDONE','DONE')
group by Transaction_id;

-- Used for manaul checking after test runs
create view transaction_net_changes as
select Transaction_id, sum(tran_amount) tran_amount, count(*) how_many
from user_transactions
where tran_status = 'DONE'
group by Transaction_id;

create index transaction_net_changes_idx1 on transaction_net_changes(tran_amount);

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
,desc varchar(4000));

CREATE STREAM transaction_fixes
PARTITION ON COLUMN Transaction_id 
export to TOPIC transaction_fixes_topic
(Transaction_id bigint not null
,Effective_date timestamp 
,tran_status varchar(10) 
,desc varchar(4000));


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
   

CREATE COMPOUND PROCEDURE  FROM CLASS nwayprocedures.CompoundPayment;
   
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

CREATE PROCEDURE 
   FROM CLASS nwayprocedures.EndOrphanedTransactions;  
   
CREATE TASK EndOrphanedTransactionsTask 
ON SCHEDULE DELAY 100 MILLISECONDS
PROCEDURE  EndOrphanedTransactions
WITH ('100', '10000') 
ON ERROR LOG RUN ON DATABASE ENABLE;   

   



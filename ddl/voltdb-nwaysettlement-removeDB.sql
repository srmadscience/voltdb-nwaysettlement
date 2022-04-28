 
DROP TASK EndOrphanedTransactionsTask IF EXISTS;

DROP PROCEDURE CompoundPayment IF EXISTS;
drop procedure bl_transaction_status if exists;
DROP PROCEDURE GetStats__promBL IF EXISTS;
DROP PROCEDURE EndOrphanedTransactions IF EXISTS;
DROP PROCEDURE TimeoutTransactions IF EXISTS;
DROP PROCEDURE StartTransactionPayee IF EXISTS;
DROP PROCEDURE StartTransactionPayer IF EXISTS;
DROP PROCEDURE EndSpecificTransactionWithErrors IF EXISTS;
DROP PROCEDURE SetTransactionEntryDone IF EXISTS;
DROP PROCEDURE SetPayerDone IF EXISTS;
DROP PROCEDURE GetBalance IF EXISTS;

drop view transaction_status if exists;
DROP view transaction_net_changes IF EXISTS;
DROP view transaction_changes IF EXISTS;
   
DROP STREAM transaction_failures IF EXISTS;
DROP STREAM transaction_fixes IF EXISTS;

DROP TABLE user_balances IF EXISTS;
DROP TABLE user_transactions IF EXISTS;
DROP table promBL_latency_stats IF EXISTS;

 

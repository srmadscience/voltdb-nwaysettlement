<img title="Volt Active Data" alt="Volt Active Data Logo" src="http://52.210.27.140:8090/voltdb-awswrangler-servlet/VoltActiveData.png?repo=voltdb-nwaysettlement">


# N-way settlement demo #

## Problem Statement ##

Imagine we have a payments system for several hundred thousand users. Most are customers. A handful are either vendors or shippers. We thus have many transactions that look like this:


    User ALICE spends 3 Euros. 2 Goes to BIGCORP and 1 goes to WESHIP.
    
In a traditional RDBMS we'd have a user_balances table with 3 rows, each consisting of a name and a balance. A transaction would consist of locking ALICE's row, reducing the balance by 3 if possible, and then locking and incrementing the balances of BIGCORP and WESHIP. Then, and only them, is the transaction committed.  From the time the lock is taken out to when the transaction finishes nodody can make any changes to ALICE, BIGCORP, or WESHIP. To make things much worse there might be another transaction from FRED where he's using OTHERSHIP to get something from BIGCORP. A massive web of complex locking dependencies flares into existance, and all activity grinds to a halt at random intervals for no obvious reason. Research done by Dr. Stonebraker reveals that adding CPU cores or extra servers doesn't help this problem. Neither does NoSQL. 

## How we solve it ##

Instead of having a single table per user we have two. One is a Balance table, and the other is a Transaction Table. Your *actual* Balance is the amount in the Balance table plus any transactions that are marked as DONE. Transactions can be PENDING (new), FAILED or DONE. The transaction table also has an extra column called USER_COUNT. the person paying sets USER_COUNT to how many people are involved. "Effective_Date" is set to a value several milliseconds in the future. The client application creates a row in the Transaction Table for each person involved:

USER | TransactionId | Status | Amount  | Effective_Date
|:- |:- |:- |:- |:- | 
ALICE | 1 | PENDING | -3 |  calltime +3ms
BIGCORP | 1 | PENDING | 2 |  calltime +3ms
 WESHIP | 1 | PENDING | 1 |  calltime +3ms

The client program calls a Compound Procedure that in turn fires off async requests to the partitions that own each account and waits for responses.  Note the Effective_Date column - we allow time for all the rows to show up. The actual offset depends on your situation.  The  requests don't just blindly insert pending rows. They do sanity checking, such as whether ALICE does actually have 3 Euros to spend, and whether BIGCORP and WESHIP are real users that can accept payments. If the incoming request for a user doesn't make sense the procedure won't create a transaction record.

When the compound proc gets responses from all 3 requests it checks to see if they all claim to have worked and have a status of PENDING.

If not, the proc deletes the rows and returns control to the client, along with an error message. Normally records are pending for < 3ms.

If we are still good, a special step takes place - the compound proc calls another proc to update Alice's transaction to a status of PAYERDONE. This is a single call to a single partition procedure. Once this is done we are finished for practical purposes - even if the last step, in which we update the other records to DONE never happens, we have a scheduled tasks that will do it.

Note that the scheduled task is a multi partition transaction, which means it's sees everything at the same time. Normally it has nothing to do, so it doesn't impact performance.

![NWay](https://github.com/srmadscience/voltdb-nwaysettlement/blob/main/docs/nway.png "NWay")


This entire process is fast (<1ms) and looks like one event from the viewpoint of the client. The obvious question is how does it cope with node failures?

### Failure before the Payer has been marked as finished ###

If this happens the  [Compound Procedure](https://github.com/srmadscience/voltdb-nwaysettlement/blob/main/src/nwayprocedures/CompoundPayment.java) will attempt to undo the work it has done. This will happen if, for example, Alice doesn't have enough money.

If a node has failed then a database task (which volt will always have running on a surviving node) will do the same cleanup using a multi partition transaction within a few milliseconds.

### Failure after the Payer has been marked as finished ###

If this happens the then a database task (which volt will always have running on a surviving node) will forcibly finish the transaction using a [multi partition transaction](https://github.com/srmadscience/voltdb-nwaysettlement/blob/main/src/nwayprocedures/EndOrphanedTransactions.java) within a few milliseconds.

## Why it's fast ##

* The legwork of creating the transaction data is done by async single partition procedures.
* While we use a multi partition write to finish the transactions, this is only when something has gone wrong. Under normal circumstances the system is all single partition calls, as the multi partition cleanup task has nothing to do.
* Updating a user's balance is a single partition process that happens when a user reads their balance, at which point DONE transactions are accounted for.

## Why it works ##

* The individual inserts into the transactions table are done by a stored procedure that handles basic sanity related questions, such as whether the user exists and whether they have enough money. 
* The only way a transaction can go from PENDING to PAYERDONE to DONE is if the server side process updates the rows involved.
* This update only happens if the correct number of rows have been created.

## What an outside observer would see ##

* They use a client API that's a bit slow but either returns DONE, CANTSTART or CANTFINISH.
* CANTFINISH means the database task finished the transaction. 
* The database is ACID:
	* Atomic: Either all the elements of a transaction happen or none of them do.
	* Consistent: At no point can an outside observer look at the system and see DONE transaction whose Amount does not add up to zero and whose effective data is in the past. 
	* Isolation: With the obvious exception of a user running out of money the order transactions reach the system is immaterial. 
	* Durable: DONE and FAILED transactions are Durable. PENDING transactions have a life expectancy of around 3ms before changing to DONE or FAILED.


## Running Test Application

To run the test application, follow the the steps below:

```
cd ddl
```
Load the schema and jar file -

```
sqlcmd --servers=172.31.23.34,172.31.23.35,172.31.23.36 < nwaysettlement_ddl.sql 
```
```
cd ../jars
```
```
java -jar TestClient.jar volt-node-ip tpms #accounts #transactions delay executionTime #participants offset 10 10 60 2 0
```

* volt-node-ip - comma delimited ip cluster nodes
* tpms - throughput thousand transactions per second
* accounts - number of total accounts
* transactions - number of transactions to run
* delay - controls server side transaction processing 
* processingTime - controls client check regarding successfull processing of transaction
* participants - nunber of participants in single transaction
* offset - account id offset

For example - 

```
java -jar TestClient.jar 172.31.23.34,172.31.23.35,172.31.23.36  1 1000 10 10 60 2 0
```

## Prometheus Configuration

Add the following to the prometheus YAML configuration file:

```
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: 'Site0'

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.

    static_configs:
       - targets: ['vdb1:9100','vdb1:9101','vdb1:9102','vdb2:9100','vdb2:9101','vdb2:9102','vdb3:9100','vdb3:9101','vdb3:9102']
```

Where vdb1, vdb2 and vdb3 are the ip addresses of the node within the voltdb cluster.





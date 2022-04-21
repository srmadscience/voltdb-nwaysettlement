# N-way settlement demo #

## Problem Statement ##

Imagine we have a payments system for several hundred thousand users. Most are customers. A handful are either vendors or shippers. We thus have many transactions that look like this:


    User ALICE spends 103 Euros. 100 Goes to BIGCORP and 3 goes to WESHIP.
    
In a traditional RDBMS we'd have a user_balances table with 3 rows, each consisting of a name and a balance. A transaction would consist of locking ALICE's row, reducing the balance by 103 if possible, and then locking and incrementing the balances of BIGCORP and WESHIP. Then, and only them, is the transaction committed.  From the time the lock is taken out to when the transaction finishes nodody can make any changes to ALICE, BIGCORP, or WESHIP. To make things much worse there might be another transaction from FRED where he's using OTHERSHIP to get something from BIGCORP. A massive web of complex locking dependencies flares into existance, and all activity grinds to a halt at random intervals for no obvious reason. Research done by Dr. Stonebraker reveals that adding CPU cores or extra servers doesn't help this problem. Neither does NoSQL. 

## How we solve it ##

Instead of having a single table per user we have two. One is a Balance table, and the other is a Transaction Table. Your *actual* Balance is the amount in the Balance table plus any transactions that are marked as DONE. Transactions can be PENDING (new), FAILED or DONE. The transaction table also has an extra column called USER_COUNT. the person paying sets USER_COUNT to how many people are involved. "Effective_Date" is set to a value several milliseconds in the future. The client application creates a row in the Transaction Table for each person involved:

USER | TransactionId | Status | Amount | User_Count | Effective_Date
| :- |:- |:- |:- |:- | 
ALICE | 1 | PENDING | -103 | 3 | calltime +3ms
BIGCORP | 1 | PENDING | 100 | NULL| calltime +3ms
WESHIP | 1 | PENDING | 3 | NULL| calltime +3ms

The client program calls a Compound Procedure that in turn fires off async requests to the partitions that own each account and waits for responses.  Note the Effective_Date column - we allow time for all the rows to show up. The actual offset depends on you situation.  The  requests don't just blindly insert pending rows. They do sanity checking, such as whether ALICE does actually have 103 Euros to spend, and whether BIGCORP and WESHIP are real users that can accept payments. If the incoming request for a user doesn't make sense the procedure won't create a transaction record.

When the client program gets responses from all 3 requests it checks to see if they all claim to have worked. If not, we now know we have a problem and can maybe try later with a different transaction_id.

![NWay](https://github.com/srmadscience/voltdb-nwaysettlement/blob/main/docs/nway.png "NWay")






By now 3ms have passed, and a scheduled task running every 0.3ms on the server has woken up. It does several things:

* It finds the earliest PENDING transaction:

| Effective_Date |
| :- |
| calltime +3ms |

* It uses that effective_date to get records from that point in time up to now where user_count is not null:

USER | TransactionId | Status | Amount | User_Count | Effective_Date
| :- |:- |:- |:- |:- | 
ALICE | 1 | PENDING | -103 | 3 | calltime +3ms

* It then hits a view that groups the pending transactions by transction_id:

 TransactionId |  User_Count | Count(*)
| :- |:- |:- 
 1 | PENDING |  3 
 
It now knows all the most pressing transactions, how many rows they are supposed to have (column User_count in step 2) and how many rows they do have (column count(*) in step 3). If these two numbers match it update all the rows for that transaction_id to 'DONE'. If they don't we update them to 'FAILED'. In most cases all the transactions for a given millisecond are valid, so we just say:

    WHERE effective_date = ? AND transaction_status = 'PENDING'
    
in a single SQL Statement. 

If some of them have failed we pass in a list of successful transasations by adding

    AND transaction_id IN ?

to the SQL statement.

Some time this after the client - having waited for the record to be processed - queries its balance. Doing so is a single partition transaction that causes the DONE transactions to be added to their balance and deleted.

## Why it's fast ##

* The legwork of creating the transaction data is done by async single partition procedures.
* While we use a multi partition write to finish the transactions, it only has 3 trips across the JNI bridge and only issues 4 or 5 SQL statements each pass.
* Updating a user's balance is a single partition process that happens when a user reads their balance, at which point DONE transactions are accounted for.

## Why it works ##

* The individual inserts into the transactions table are done by a stored procedure that handles basic sanity related questions, such as whether the user exists and whether they have enough money. 
* The only way a transaction can go from PENDING to DONE is if the server side process updates the rows involved.
* This update only happens if the correct number of rows have been created.

## What an outside observer would see ##

* They use a client API that's a bit slow but either returns DONE or FAILED. 
* The database is ACID:
	* Atomic: Either all the elements of a transaction happen or none of them do.
	* Consistent: At no point can an outside observer look at the system and see DONE transaction whose Amount does not add up to zero. 
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





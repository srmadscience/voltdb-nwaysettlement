/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package nwayprocedures;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * End all transactions where we have reached the effective date.
 */
public class EndTransactions extends VoltProcedure {

	public static final SQLStmt getFirstTransactions = new SQLStmt(
			"SELECT effective_date FROM user_transactions t WHERE tran_status = 'PENDING' ORDER BY effective_date LIMIT 1; ");

	public static final SQLStmt getOpenTransactions = new SQLStmt(
			"SELECT * FROM user_transactions t WHERE effective_date BETWEEN ? AND ? AND queue_date <= NOW AND tran_status = 'PENDING' ORDER BY Transaction_id, userid; ");

	public static final SQLStmt getOpenUserCount = new SQLStmt(
			"SELECT * FROM vw_users_per_pending_transactions v WHERE effective_date BETWEEN  ? AND ? ORDER BY transaction_id,effective_date;");

	public static final SQLStmt finishTransactions = new SQLStmt(
			"UPDATE user_transactions SET tran_status = 'DONE', done_date = NOW, queue_date = null, user_count = null WHERE  effective_date BETWEEN ? AND ?  AND Transaction_id IN ? AND tran_status = 'PENDING';");

	public static final SQLStmt finishAllTransactions = new SQLStmt(
			"UPDATE user_transactions SET tran_status = 'DONE', done_date = NOW, queue_date = null, user_count = null WHERE  effective_date BETWEEN ? AND ? AND tran_status = 'PENDING';");

	public static final SQLStmt failTransactions = new SQLStmt(
			"UPDATE user_transactions SET tran_status = 'FAILED_QTY', done_date = NOW, queue_date = null, user_count = null WHERE  effective_date BETWEEN ? AND ?  AND Transaction_id IN ? AND tran_status = 'PENDING';");

	public static final SQLStmt failTransactionsWithoutAPayer = new SQLStmt(
			"UPDATE user_transactions SET tran_status = 'FAILED_NOPAYER', done_date = NOW, queue_date = null, user_count = null WHERE  effective_date  = ? AND tran_status = 'PENDING';");

	public static final SQLStmt reportFailures = new SQLStmt(
			"INSERT INTO transaction_failures(Transaction_id,Effective_date,tran_status,desc) VALUES (?,?,?,?);");

	public static final SQLStmt upsertStat = new SQLStmt(
			"UPSERT INTO promBL_latency_stats (statname, stathelp, event_type, event_name, statvalue,lastdate) VALUES (?,?,?,?,?,NOW);");

	private static final int RANDOM_STAT_INTERVAL = 50;

	Random r;

	public VoltTable[] run(long tranCount, long maxTranAgeMs) throws VoltAbortException {

		// Always use VoltDB's random number generator inside a procedure. We use it to
		// send
		// stats every RANDOM_STAT_INTERVAL calls.
		if (r == null) {
			r = this.getSeededRandomNumberGenerator();
		}

		// Find oldest pending record...
		voltQueueSQL(getFirstTransactions);
		VoltTable[] firstRecords = voltExecuteSQL();

		if (!firstRecords[0].advanceRow()) {
			// there are no pending records and thus nothing to do...

			// Report that there is nothing in the queue so the graph on the
			// dashboard goes to zero.
			if (reportStatsPseudoRandomly(0, 0)) {
				voltExecuteSQL(true);
			}

			// We didn't report anything, so just return something..
			return firstRecords;
		}

		// Figure out when to start getting records. Look for the earliest effective
		// date that isn't in the future.
		// There may not be one...
		final Date runDate = this.getTransactionTime();

		Date firstDate = firstRecords[0].getTimestampAsTimestamp("effective_date").asExactJavaDate();
		if (firstDate.after(runDate)) {
			firstDate = runDate;
		}

		// having picked our start date figure out our date range. Maye sure it
		// can never spill over into the future.
		Date endDate = new Date(firstDate.getTime() + maxTranAgeMs);

		if (endDate.after(runDate)) {
			endDate = runDate;
		}

		// We have two lists of transactions - one is based on the payer, and the
		// other is based on a materialized view that lists outstanding transactions.
		// Not all items in this view will have payer. Instead of doing a join we
		// use transactionUsercounts to glue thiee two lists together
		HashMap<Long, Long> transactionUsercounts = new HashMap<Long, Long>();

		// Later on we make decisions about which transactions worked and which
		// didn't. In normal use doneTransactions is full and failedTransactions
		// is empty.
		ArrayList<Long> doneTransactions = new ArrayList<Long>();
		ArrayList<Long> failedTransactions = new ArrayList<Long>();

		voltQueueSQL(getOpenTransactions, firstDate, endDate);
		voltQueueSQL(getOpenUserCount, firstDate, endDate);

		VoltTable[] openTransactions = voltExecuteSQL();

		if (openTransactions[0].getRowCount() == 0) {
			// The earliest record is for a transaction we don't have a payer for.
			// We know this because getOpenTransactions uses queue_date, which is
			// only filled in by payer records.
			voltQueueSQL(failTransactionsWithoutAPayer, firstDate);
			return voltExecuteSQL(true);
		}

		// Go through the records in the view and find out how many records
		// we *actually* have for each transaction. Add them to a HashMap.
		while (openTransactions[1].advanceRow()) {

			long viewTransactionid = openTransactions[1].getLong("Transaction_id");
			long viewTransactionUserCount = openTransactions[1].getLong("how_many");

			transactionUsercounts.put(viewTransactionid, viewTransactionUserCount);
		}

		while (openTransactions[0].advanceRow()) {

			long transactionid = openTransactions[0].getLong("Transaction_id");
			long originalTransactionParticipants = openTransactions[0].getLong("user_count");
			Long actualTransactionParticipants = transactionUsercounts.get(transactionid);

			if (actualTransactionParticipants == null) {
				actualTransactionParticipants = 0L;
			} else {
				// We won't need this any more
				transactionUsercounts.remove(transactionid);
			}

			// try to finish

			if (originalTransactionParticipants == actualTransactionParticipants) {
				doneTransactions.add(transactionid);
			} else {
				failedTransactions.add(transactionid);
			}

		}

		Iterator<Entry<Long, Long>> iterator = transactionUsercounts.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<Long, Long> entry = iterator.next();
			failedTransactions.add(entry.getValue());
		}

		// All of our transactions have the right number of participants.
		// This is the 99.9% of the time 'happy path'...
		if (doneTransactions.size() > 0 && failedTransactions.size() == 0) {

			voltQueueSQL(finishAllTransactions, firstDate, endDate);

		} else if (doneTransactions.size() > 0) {

			// Some of our transactions worked. Pass in a list of them
			// to finishTransactions
			Object[] doneArray = new Long[doneTransactions.size()];
			doneTransactions.toArray(doneArray);
			voltQueueSQL(finishTransactions, firstDate, endDate, doneArray);

		}

		// Some of our transactions failed. Pass in a list of them to
		// failTransactions, and itemize them with reportFailures
		if (failedTransactions.size() > 0) {

			Object[] failedArray = new Long[failedTransactions.size()];
			failedTransactions.toArray(failedArray);
			voltQueueSQL(failTransactions, firstDate, endDate, failedArray);

			for (int i = 0; i < failedArray.length; i++) {
				voltQueueSQL(reportFailures, failedArray[i], runDate, "FAILED", "Shortage of participants");
			}

		}

		// Every RANDOM_STAT_INTERVAL report stats...
		reportStatsPseudoRandomly((endDate.getTime() - firstDate.getTime()), doneTransactions.size());

		if (doneTransactions.size() > 0 || failedTransactions.size() > 0
				|| tranCount == openTransactions[0].getRowCount()) {
			return voltExecuteSQL(true);
		}

		return openTransactions;
	}

	private boolean reportStatsPseudoRandomly(long queuelagms, int tranCount) {

		if (r.nextInt(RANDOM_STAT_INTERVAL) == 0) {

			voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "queuelagms",
					queuelagms);
			voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "donetrancount",
					tranCount);
			return true;
		}

		return false;
	}

}

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

import java.util.Date;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * Find cases where table row count is out of line with targets.
 */
public class StartTransactionPayer extends VoltProcedure {
//
	public static final SQLStmt getUser = new SQLStmt(
			"select u.userid, u.balance_amount " + "from user_balances u " + "where  u.userid = ?;");

	public static final SQLStmt getDoneTransactions = new SQLStmt("select sum(t.tran_amount) completed_transactions "
			+ "from user_transactions t where t.userid = ? and t.tran_status = 'DONE';");

	public static final SQLStmt getReleventPendingTransactions = new SQLStmt(
			"select sum(t.tran_amount) completed_transactions "
					+ "from user_transactions t where t.userid = ? and t.tran_status = 'PENDING' AND t.tran_amount < 0;");

	public static final SQLStmt startTransaction = new SQLStmt(
			"INSERT INTO user_transactions (userid, other_userid, tran_amount, transaction_id, Effective_date, tran_status,queue_date,user_count, stale_date) "
					+ " VALUES " + "(?,?,?,?,?,'PENDING',?,?,DATEADD(SECOND, 2, ?));");

	public static final byte QUEUED = 0;
	public static final String QUEUED_MESSAGE = "queued.";

	public static final byte NO_SUCH_USER = 1;
	public static final String NO_SUCH_USER_MESSAGE = "No such user";

	public static final byte NOT_ENOUGH_MONEY = 2;
	public static final String NOT_ENOUGH_MONEY_MESSAGE = "not enough money:";

	public static final byte MISSED_EFFECTIVE_DATE = 3;
	public static final String MISSED_EFFECTIVE_DATE_MESSAGE = "arrived too late:";

	public static final long GRACE_MS = 4000;

	public VoltTable[] run(long payingUserId, long payingAmount, long txnId, TimestampType effectiveDate,
			int involvedUserCount) throws VoltAbortException {

		final Date cutoffDate = new Date(this.getTransactionTime().getTime() - StartTransactionPayer.GRACE_MS);

		if (effectiveDate.asExactJavaDate().before(cutoffDate)) {
			this.setAppStatusCode(StartTransactionPayer.MISSED_EFFECTIVE_DATE);
			this.setAppStatusString(StartTransactionPayer.MISSED_EFFECTIVE_DATE_MESSAGE);
			return voltExecuteSQL(true);
		}

		this.voltQueueSQL(getUser, payingUserId);
		this.voltQueueSQL(getDoneTransactions, payingUserId);
		this.voltQueueSQL(getReleventPendingTransactions, payingUserId);

		VoltTable[] userQueryResults = voltExecuteSQL();

		if (!userQueryResults[0].advanceRow()) {
			this.setAppStatusCode(NO_SUCH_USER);
			this.setAppStatusString(NO_SUCH_USER_MESSAGE);
			return voltExecuteSQL(true);
		}

		long availableCredit = userQueryResults[0].getLong("balance_amount");

		for (int i = 1; i < 2; i++) {
			if (userQueryResults[i].advanceRow()) {
				long transactionValue = userQueryResults[i].getLong("completed_transactions");

				if (!userQueryResults[i].wasNull()) {
					availableCredit = availableCredit + transactionValue;
				}

			}
		}

		if (availableCredit + payingAmount < 0) {
			this.setAppStatusCode(NOT_ENOUGH_MONEY);
			this.setAppStatusString(NOT_ENOUGH_MONEY_MESSAGE + " " + availableCredit);
			return voltExecuteSQL(true);
		}

		this.voltQueueSQL(startTransaction, payingUserId, payingUserId, payingAmount, txnId, effectiveDate,
				effectiveDate, involvedUserCount, effectiveDate);

		this.setAppStatusCode(StartTransactionPayer.QUEUED);
		this.setAppStatusString(StartTransactionPayer.QUEUED_MESSAGE);
		return voltExecuteSQL(true);

	}

}

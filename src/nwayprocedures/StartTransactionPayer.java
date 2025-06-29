/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
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

    public static final byte PAYERDONE_CODE = 1;
    public static final String PAYERDONE_MESSAGE = "PAYERDONE";

    public static final byte DONE_CODE = 0;
    public static final String DONE_MESSAGE = "DONE";

    public static final byte NO_SUCH_USER_CODE = 2;
    public static final String NO_SUCH_USER_MESSAGE = "No such user";

    public static final byte NOT_ENOUGH_MONEY_CODE = 3;
    public static final String NOT_ENOUGH_MONEY_MESSAGE = "not enough money:";

    public static final byte MISSED_EFFECTIVE_DATE_CODE = 4;
    public static final String MISSED_EFFECTIVE_DATE_MESSAGE = "arrived too late:";

    public static final long GRACE_MS = 4000;

    public static final byte PENDING_CODE = 5;
    public static final String PENDING_MESSAGE = "PENDING";

    public static final byte PAYEE_LIST_MISMATCH_CODE = 6;
    public static final String PAYEE_LIST_MISMATCH_MESSAGE = "Payee list and amount mismatch";

    public static final byte PAYEE_LIST_TOOLONG_CODE = 7;
    public static final String PAYEE_LIST_TOOLONG_MESSAGE = "Payee list must be 9 or less in length";

    public static final byte PARAMETER_NULL_CODE = 8;
    public static final String PARAMETER_NULL_MESSAGE = "Parameter can not be null";

    public static final String CANTFINISH = "CANTFINISH";
    public static final String CANTSTART = "CANTSTART";
    public static final String STALE = "STALE";

    public VoltTable[] run(long payingUserId, long payingAmount, long txnId, TimestampType effectiveDate,
            int involvedUserCount) throws VoltAbortException {

        final Date cutoffDate = new Date(this.getTransactionTime().getTime() - StartTransactionPayer.GRACE_MS);

        if (effectiveDate.asExactJavaDate().before(cutoffDate)) {
            this.setAppStatusCode(StartTransactionPayer.MISSED_EFFECTIVE_DATE_CODE);
            this.setAppStatusString(StartTransactionPayer.MISSED_EFFECTIVE_DATE_MESSAGE);
            return voltExecuteSQL(true);
        }

        this.voltQueueSQL(getUser, payingUserId);
        this.voltQueueSQL(getDoneTransactions, payingUserId);
        this.voltQueueSQL(getReleventPendingTransactions, payingUserId);

        VoltTable[] userQueryResults = voltExecuteSQL();

        if (!userQueryResults[0].advanceRow()) {
            this.setAppStatusCode(NO_SUCH_USER_CODE);
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
            this.setAppStatusCode(NOT_ENOUGH_MONEY_CODE);
            this.setAppStatusString(NOT_ENOUGH_MONEY_MESSAGE + " " + availableCredit);
            return voltExecuteSQL(true);
        }

        this.voltQueueSQL(startTransaction, payingUserId, payingUserId, payingAmount, txnId, effectiveDate,
                effectiveDate, involvedUserCount, effectiveDate);

        this.setAppStatusCode(StartTransactionPayer.PENDING_CODE);
        this.setAppStatusString(StartTransactionPayer.PENDING_MESSAGE);
        return voltExecuteSQL(true);

    }

}

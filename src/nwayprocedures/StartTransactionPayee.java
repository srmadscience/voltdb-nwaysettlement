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
public class StartTransactionPayee extends VoltProcedure {

    public static final SQLStmt getUser = new SQLStmt(
            "select u.userid " + "from user_balances u " + "where u.userid = ?;");

    public static final SQLStmt startTransaction = new SQLStmt(
            "INSERT INTO user_transactions (userid, other_userid, tran_amount, transaction_id, Effective_date, tran_status, stale_date) "
                    + " VALUES " + "(?,?,?,?,?,'PENDING',DATEADD(SECOND, 2, ?));");

    public VoltTable[] run(long paidUserId, long payingUserId, long paidAmount, long txnId, TimestampType effectiveDate)
            throws VoltAbortException {

        final Date cutoffDate = new Date(this.getTransactionTime().getTime() - StartTransactionPayer.GRACE_MS);

        if (effectiveDate.asExactJavaDate().before(cutoffDate)) {
            this.setAppStatusCode(StartTransactionPayer.MISSED_EFFECTIVE_DATE_CODE);
            this.setAppStatusString(StartTransactionPayer.MISSED_EFFECTIVE_DATE_MESSAGE);
            return voltExecuteSQL(true);
        }

        this.voltQueueSQL(getUser, paidUserId);

        VoltTable[] userQueryResults = voltExecuteSQL();

        if (!userQueryResults[0].advanceRow()) {
            this.setAppStatusCode(StartTransactionPayer.NO_SUCH_USER_CODE);
            this.setAppStatusString(StartTransactionPayer.NO_SUCH_USER_MESSAGE);
            return voltExecuteSQL(true);
        }

        this.voltQueueSQL(startTransaction, paidUserId, payingUserId, paidAmount, txnId, effectiveDate, effectiveDate);
        this.setAppStatusCode(StartTransactionPayer.PENDING_CODE);
        this.setAppStatusString(StartTransactionPayer.PENDING_MESSAGE);

        return voltExecuteSQL(true);

    }

}

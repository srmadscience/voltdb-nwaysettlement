/* This file is part of VoltDB.
 * Copyright (C) 2022 VoltDB Inc.
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

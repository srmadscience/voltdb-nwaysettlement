/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package nwayprocedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * End all transactions where we have reached the effective date.
 */
public class SetTransactionEntryDone extends VoltProcedure {

    public static final int TX_FINISH_GRACE_WINDOW_MS = 10000;

    public static final SQLStmt finishTransaction = new SQLStmt(
            "UPDATE user_transactions " + "SET tran_status = 'DONE', done_date = NOW, queue_date = null "
                    + "WHERE  userid = ? AND Transaction_id = ? " + "AND tran_status IN ('PENDING','PAYERDONE') "
                    + "AND DATEADD(MILLISECOND,?,insert_date) >= NOW;");

    public VoltTable[] run(long userId, long txnId) throws VoltAbortException {
        // Find oldest pending record...
        voltQueueSQL(finishTransaction, EXPECT_ONE_ROW, userId, txnId, TX_FINISH_GRACE_WINDOW_MS);

        this.setAppStatusCode(StartTransactionPayer.DONE_CODE);
        this.setAppStatusString(StartTransactionPayer.DONE_MESSAGE);

        VoltTable[] results = voltExecuteSQL();

        results[0].advanceRow();
        if (results[0].getLong("modified_tuples") != 1) {
            this.setAppStatusCode(StartTransactionPayer.MISSED_EFFECTIVE_DATE_CODE);
            this.setAppStatusString(StartTransactionPayer.MISSED_EFFECTIVE_DATE_MESSAGE);

        }

        return results;

    }

}

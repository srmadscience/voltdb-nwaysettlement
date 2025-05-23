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
public class SetPayerDone extends VoltProcedure {

    public static final SQLStmt updateTransaction = new SQLStmt(
            "UPDATE user_transactions SET tran_status = ? " + "WHERE  userid = ? " + "AND Transaction_id = ? "
                    + "AND tran_status = ?" + "AND DATEADD(MILLISECOND,3,insert_date) <= NOW;");

    public VoltTable[] run(long userId, long txnId, String oldStatus) throws VoltAbortException {
        // Find oldest pending record...
        voltQueueSQL(updateTransaction, EXPECT_ONE_ROW, StartTransactionPayer.PAYERDONE_MESSAGE, userId, txnId,
                oldStatus);

        this.setAppStatusCode(StartTransactionPayer.PAYERDONE_CODE);
        this.setAppStatusString(StartTransactionPayer.PAYERDONE_MESSAGE);

        return voltExecuteSQL(true);

    }

}

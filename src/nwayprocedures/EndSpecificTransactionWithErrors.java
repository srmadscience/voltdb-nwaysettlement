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
public class EndSpecificTransactionWithErrors extends VoltProcedure {

    public static final SQLStmt finishTransaction = new SQLStmt(
            "UPDATE user_transactions SET tran_status = ?, tran_status_explanation = ?, done_date = NOW, queue_date = null, user_count = null WHERE Transaction_id = ? ;");

    public static final SQLStmt reportFailures = new SQLStmt(
            "INSERT INTO transaction_failures(Transaction_id,Effective_date,tran_status,desc) VALUES (?,NOW,?,?);");

    public VoltTable[] run(long txnId, String status, String tranStatusExplanation) throws VoltAbortException {

        voltQueueSQL(finishTransaction, status, tranStatusExplanation, txnId);
        voltQueueSQL(reportFailures, txnId, status, tranStatusExplanation);

        return voltExecuteSQL(true);

    }

}

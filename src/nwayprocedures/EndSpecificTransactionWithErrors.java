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

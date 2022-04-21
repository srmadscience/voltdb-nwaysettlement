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
public class SetPayerDone extends VoltProcedure {

    public static final SQLStmt updateTransaction = new SQLStmt(
            "UPDATE user_transactions SET tran_status = ? WHERE  userid = ? AND Transaction_id = ? AND tran_status = ?;");

    public VoltTable[] run(long userId, long txnId, String oldStatus) throws VoltAbortException {
        // Find oldest pending record...
        voltQueueSQL(updateTransaction, EXPECT_ONE_ROW, StartTransactionPayer.PAYERDONE_MESSAGE, userId, txnId, oldStatus);
        
        this.setAppStatusCode(StartTransactionPayer.PAYERDONE_CODE);
        this.setAppStatusString(StartTransactionPayer.PAYERDONE_MESSAGE);
        
        return voltExecuteSQL(true);

    }

}

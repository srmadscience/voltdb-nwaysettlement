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
 * Find cases where table row count is out of line with targets.
 */
public class GetBalance extends VoltProcedure {
//
    public static final SQLStmt getUser = new SQLStmt(
            "select u.userid, u.balance_amount " + "from user_balances u " + "where  u.userid = ?;");

    public static final SQLStmt updateBalance = new SQLStmt(
            "update user_balances u " + "set balance_amount = balance_amount + ? " + "where  u.userid = ?;");

    public static final SQLStmt getDoneTransactions = new SQLStmt("select sum(t.tran_amount) completed_transactions "
            + "from user_transactions t  " + "where t.userid = ? " + "and   t.tran_status = 'DONE';");

    public static final SQLStmt deleteDoneTransactions = new SQLStmt(
            "delete " + "from user_transactions t  " + "where t.userid = ? " + "and   t.tran_status = 'DONE';");

    public static final SQLStmt getOtherTransactions = new SQLStmt("select tran_status, sum(t.tran_amount) value "
            + "from user_transactions t  " + "where t.userid = ? " + "group by tran_status order by tran_status;");

    private static final byte NO_SUCH_USER = 0;
    private static final String NO_SUCH_USER_MESSAGE = "No such user";

    public VoltTable[] run(long userid) throws VoltAbortException {

        this.voltQueueSQL(getUser, userid);
        this.voltQueueSQL(getDoneTransactions, userid);

        VoltTable[] userQueryResults = voltExecuteSQL();

        if (!userQueryResults[0].advanceRow()) {
            this.setAppStatusCode(NO_SUCH_USER);
            this.setAppStatusString(NO_SUCH_USER_MESSAGE);
            return voltExecuteSQL(true);
        }

        if (userQueryResults[1].advanceRow()) {
            long transactionValue = userQueryResults[1].getLong("completed_transactions");

            if (!userQueryResults[1].wasNull()) {
                this.voltQueueSQL(updateBalance, transactionValue, userid);
                this.voltQueueSQL(deleteDoneTransactions, userid);
                voltExecuteSQL();
            }

        }

        this.voltQueueSQL(getUser, userid);
        this.voltQueueSQL(getOtherTransactions, userid);

        return voltExecuteSQL(true);

    }

}

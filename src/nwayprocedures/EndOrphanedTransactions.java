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
import java.util.Random;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * End all transactions where we have reached the effective date.
 */
public class EndOrphanedTransactions extends VoltProcedure {

    public static final SQLStmt getPayerdoneTransactions = new SQLStmt(
            "SELECT userid, Transaction_id, effective_date, insert_date " + "FROM user_transactions t "
                    + "WHERE tran_status = 'PAYERDONE'  " + "ORDER BY insert_date, Transaction_id, userid LIMIT ?; ");

    public static final SQLStmt finishTransaction = new SQLStmt(
            "UPDATE user_transactions SET tran_status = 'DONE', done_date = NOW, queue_date = null, user_count = null "
                    + "WHERE  userid = ? AND Transaction_id = ? AND tran_status IN ('PENDING','PAYERDONE');");

    public static final SQLStmt getStalePendingTransactions = new SQLStmt(
            "SELECT userid, Transaction_id, effective_date, insert_date " + "FROM user_transactions t "
                    + "WHERE tran_status = 'PENDING' " + "ORDER BY insert_date, Transaction_id, userid LIMIT ?; ");

    public static final SQLStmt cancelTransaction = new SQLStmt(
            "UPDATE user_transactions SET tran_status = 'FAILED', done_date = NOW, queue_date = null, user_count = null "
                    + "WHERE  userid = ? AND Transaction_id = ? AND tran_status IN ('PENDING','PAYERDONE');");

    public static final SQLStmt reportFailures = new SQLStmt(
            "INSERT INTO transaction_failures(Transaction_id,Effective_date,tran_status,desc) VALUES (?,?,?,?);");

    public static final SQLStmt reportFixes = new SQLStmt(
            "INSERT INTO transaction_fixes(Transaction_id,Effective_date,tran_status,desc) VALUES (?,?,?,?);");

    public static final SQLStmt upsertStat = new SQLStmt(
            "UPSERT INTO promBL_latency_stats (statname, stathelp, event_type, event_name, statvalue,lastdate) VALUES (?,?,?,?,?,NOW);");

    private static final int RANDOM_STAT_INTERVAL = 50;

    Random r;

    public VoltTable[] run(long tranCount, long minTranAgeMs) throws VoltAbortException {

        TimestampType firstPayerDone = null;
        TimestampType firstStalePending = null;

        // Always use VoltDB's random number generator inside a procedure. We use it to
        // send
        // stats every RANDOM_STAT_INTERVAL calls.
        if (r == null) {
            r = this.getSeededRandomNumberGenerator();
        }

        // Cutoff Date

        final Date cutoffDate = new Date(this.getTransactionTime().getTime() - minTranAgeMs);

        // Find oldest payerdone record...
        voltQueueSQL(getPayerdoneTransactions, tranCount);
        VoltTable payerdoneRecords = voltExecuteSQL()[0];

        if (payerdoneRecords.getRowCount() > 0) {

            int skippedPayerDoneCount = 0;
            int finishedTxCount = 0;
            int finishedTxParties = 0;

            while (payerdoneRecords.advanceRow()) {

                TimestampType insertDate = payerdoneRecords.getTimestampAsTimestamp("insert_date");

                if (insertDate.asExactJavaDate().after(cutoffDate)) {
                    skippedPayerDoneCount = payerdoneRecords.getRowCount() - payerdoneRecords.getActiveRowIndex() + 1;
                    break;
                }

                if (firstPayerDone == null) {
                    firstPayerDone = insertDate;
                }

                voltQueueSQL(finishTransaction, payerdoneRecords.getLong("userid"),
                        payerdoneRecords.getLong("Transaction_id"));
                voltQueueSQL(reportFixes, payerdoneRecords.getLong("Transaction_id"), this.getTransactionTime(), "DONE",
                        "PAYERDONE, Completed by EndOrphanedTransactions");
            }

            VoltTable[] finishResults = voltExecuteSQL();

            finishedTxCount = finishResults.length;

            for (VoltTable finishResult : finishResults) {
                finishResult.advanceRow();
                finishedTxParties += finishResult.getLong(0);
            }

            finishedTxParties = finishedTxParties / 2;

            voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement",
                    "skippedPayerDoneCount", skippedPayerDoneCount);
            voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "forceFinishedTxs",
                    finishedTxCount);
            voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement",
                    "forceFinishedTxMembers", finishedTxParties);
            voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "payerDoneLag",
                    this.getTransactionTime().getTime() - firstPayerDone.asExactJavaDate().getTime());

            voltExecuteSQL();
        }

        // Find oldest pending record...
        voltQueueSQL(getStalePendingTransactions, tranCount);
        VoltTable staleRecords = voltExecuteSQL()[0];

        if (staleRecords.getRowCount() > 0) {

            int skippedDoneCount = 0;
            int staleTxCount = 0;
            int staleTxParties = 0;

            while (staleRecords.advanceRow()) {

                TimestampType insertDate = staleRecords.getTimestampAsTimestamp("insert_date");

                if (insertDate.asExactJavaDate().after(cutoffDate)) {
                    skippedDoneCount = staleRecords.getRowCount() - staleRecords.getActiveRowIndex() + 1;
                    break;
                }

                if (firstPayerDone == null) {
                    firstPayerDone = insertDate;
                }

                if (firstStalePending == null) {
                    firstStalePending = staleRecords.getTimestampAsTimestamp("effective_date");
                }

                voltQueueSQL(cancelTransaction, staleRecords.getLong("userid"), staleRecords.getLong("Transaction_id"));
                voltQueueSQL(reportFailures, staleRecords.getLong("Transaction_id"), this.getTransactionTime(),
                        StartTransactionPayer.STALE, "PENDING, Cancelled by EndOrphanedTransactions");

            }

            VoltTable[] finishResults = voltExecuteSQL();

            staleTxCount = finishResults.length;

            for (VoltTable finishResult : finishResults) {
                finishResult.advanceRow();
                staleTxParties += finishResult.getLong(0);
            }

            staleTxParties = staleTxParties / 2;

            voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "skippedDoneCount",
                    skippedDoneCount);
            voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "staleTxs",
                    staleTxCount);
            voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "staleTxMembers",
                    staleTxParties);
            if (firstStalePending != null) {
                voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "pendingLag",
                        this.getTransactionTime().getTime() - firstStalePending.asExactJavaDate().getTime());

            } else {
                voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "pendingLag", 0);

            }

        }

        if (payerdoneRecords.getRowCount() == 0 && staleRecords.getRowCount() == 0
                && r.nextInt(RANDOM_STAT_INTERVAL) == 0)

        {
            String[] statsToBeSetToZero = { "skippedPayerDoneCount", "forceFinishedTxs", "forceFinishedTxMembers",
                    "payerDoneLag", "skippedDoneCount", "staleTxs", "staleTxMembers", "pendingLag" };

            for (int i = 0; i < statsToBeSetToZero.length; i++) {
                voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement",
                        statsToBeSetToZero[i], 0);
            }
        }

        return voltExecuteSQL();

    }

}

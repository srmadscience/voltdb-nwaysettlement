/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package nwayprocedures;

import java.util.Date;
import java.util.Random;

import org.voltcore.logging.VoltLogger;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * End all transactions where we have reached the effective date.
 */
public class EndOrphanedTransactions extends VoltProcedure {

    public static final SQLStmt getPayerdoneTransactions = new SQLStmt(
            "SELECT userid, Transaction_id, insert_date " + "FROM user_transactions t "
                    + "WHERE tran_status = 'PAYERDONE'  " + "ORDER BY insert_date, Transaction_id, userid LIMIT ?; ");

    public static final SQLStmt getPayerdoneTransactionStatus = new SQLStmt(
            "SELECT * FROM transaction_changes WHERE Transaction_id = ? ; ");

    public static final SQLStmt finishTransaction = new SQLStmt(
            "UPDATE user_transactions SET tran_status = 'DONE', tran_status_explanation = ?, "
            + "done_date = NOW, queue_date = null "
                    + "WHERE  Transaction_id = ? AND tran_status IN ('PENDING','PAYERDONE');");

    public static final SQLStmt getStalePendingTransactions = new SQLStmt(
            "SELECT userid, Transaction_id, effective_date, insert_date " + "FROM user_transactions t "
                    + "WHERE tran_status = 'PENDING' " + "ORDER BY insert_date, Transaction_id, userid LIMIT ?; ");

    public static final SQLStmt cancelTransaction = new SQLStmt(
            "UPDATE user_transactions SET tran_status = 'FAILED', tran_status_explanation = ?, "
            + "done_date = NOW, queue_date = null, user_count = null "
                    + "WHERE Transaction_id = ? AND tran_status IN ('PENDING','PAYERDONE');");

    public static final SQLStmt reportFailures = new SQLStmt(
            "INSERT INTO transaction_failures(Transaction_id,Effective_date,tran_status,desc) VALUES (?,?,?,?);");

    public static final SQLStmt reportFixes = new SQLStmt(
            "INSERT INTO transaction_fixes(Transaction_id,Effective_date,tran_status,desc) VALUES (?,?,?,?);");

    public static final SQLStmt upsertStat = new SQLStmt(
            "UPSERT INTO promBL_latency_stats (statname, stathelp, event_type, event_name, statvalue,lastdate) VALUES (?,?,?,?,?,NOW);");

    private static final int RANDOM_STAT_INTERVAL = 50;

    static VoltLogger LOG = new VoltLogger("EndOrphanedTransactions");

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

        Date cutoffDate = new Date(this.getTransactionTime().getTime() - minTranAgeMs);

        if (cutoffDate.getTime() > System.currentTimeMillis() - SetTransactionEntryDone.TX_FINISH_GRACE_WINDOW_MS) {
            cutoffDate = new Date(System.currentTimeMillis() - SetTransactionEntryDone.TX_FINISH_GRACE_WINDOW_MS - 1);
        }

        // Find oldest payerdone record...
        voltQueueSQL(getPayerdoneTransactions, tranCount);
        VoltTable payerdoneRecords = voltExecuteSQL()[0];

        if (payerdoneRecords.getRowCount() > 0) {

            int skippedPayerDoneCount = 0;
            int finishedTxCount = 0;
            int cantFinishTxCount = 0;

            while (payerdoneRecords.advanceRow()) {

                TimestampType insertDate = payerdoneRecords.getTimestampAsTimestamp("insert_date");

                if (insertDate.asExactJavaDate().after(cutoffDate)) {
                    skippedPayerDoneCount++;

                } else {

                    if (firstPayerDone == null
                            || firstPayerDone.asExactJavaDate().after(insertDate.asExactJavaDate())) {
                        firstPayerDone = insertDate;
                    }

                    final long userId = payerdoneRecords.getLong("userid");
                    final long txId = payerdoneRecords.getLong("Transaction_id");

                    if (isSafeToFinish(txId)) {

                        voltQueueSQL(finishTransaction, "Completed by EndOrphanedTransactions", txId);
                        voltQueueSQL(reportFixes, txId, this.getTransactionTime(), "DONE",
                                "PAYERDONE, Completed by EndOrphanedTransactions");
                        finishedTxCount++;

                    } else {

                        voltQueueSQL(cancelTransaction, "Cancelled by EndOrphanedTransactions", txId);
                        voltQueueSQL(reportFailures, txId, this.getTransactionTime(), StartTransactionPayer.STALE,
                                "PAYERDONE, Cancelled by EndOrphanedTransactions. Started at " + insertDate.toString());
                        cantFinishTxCount++;
                    }

                }
            }

            voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement",
                    "skippedPayerDoneCount", skippedPayerDoneCount);
            voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "forceFinishedTxs",
                    finishedTxCount);
            voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "cantFinishTx",
                    cantFinishTxCount);
            if (firstPayerDone == null) {
                voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "payerDoneLag",
                        0);
               
            } else {
                voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", "payerDoneLag",
                        this.getTransactionTime().getTime() - firstPayerDone.asExactJavaDate().getTime());
               
            }

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
                TimestampType effectiveDate = staleRecords.getTimestampAsTimestamp("effective_date");

                if (insertDate.asExactJavaDate().after(cutoffDate)) {
                    skippedDoneCount = staleRecords.getRowCount() - staleRecords.getActiveRowIndex() + 1;
                    break;
                }

                if (firstStalePending == null
                        || firstStalePending.asExactJavaDate().after(effectiveDate.asExactJavaDate())) {
                    firstStalePending = effectiveDate;
                }

                voltQueueSQL(cancelTransaction,  "Cancelled by EndOrphanedTransactions",staleRecords.getLong("Transaction_id"));
                voltQueueSQL(reportFailures, staleRecords.getLong("Transaction_id"), this.getTransactionTime(),
                        StartTransactionPayer.STALE,
                        "PENDING, Cancelled by EndOrphanedTransactions. Started at " + insertDate.toString());
                LOG.error("txId " + staleRecords.getLong("Transaction_id") + "PENDING, Cancelled by EndOrphanedTransactions. Started at " + insertDate.toString());

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

            for (String element : statsToBeSetToZero) {
                voltQueueSQL(upsertStat, "EndTransactions", "EndTransactions", "internalmeasurement", element, 0);
            }
        }

        return voltExecuteSQL();

    }

    private boolean isSafeToFinish(long txId) {

        boolean safe = true;

        voltQueueSQL(getPayerdoneTransactionStatus, EXPECT_ONE_ROW, txId);
        VoltTable[] isSafeToFinishResults = voltExecuteSQL();
        int txRow = isSafeToFinishResults.length - 1;
        isSafeToFinishResults[txRow].advanceRow();

        final long netTranAmount = isSafeToFinishResults[txRow].getLong("net_tran_amount");
        final long plannedParticipants = isSafeToFinishResults[txRow].getLong("planned_participants");
        final long actualParticipants = isSafeToFinishResults[txRow].getLong("actual_participants");

        if (netTranAmount != 0) {
            LOG.error("txId " + txId + " has a net_tran_amount of " + netTranAmount);
            safe = false;

        } else if (plannedParticipants != isSafeToFinishResults[txRow].getLong("actual_participants")) {
            LOG.error("txId " + txId + " has a different number of planned and actual participants: "
                    + plannedParticipants + "/" + actualParticipants);
            safe = false;
        }

        return safe;
    }

}

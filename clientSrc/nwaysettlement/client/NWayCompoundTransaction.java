/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package nwaysettlement.client;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.voltutil.stats.SafeHistogramCache;

import nwayprocedures.StartTransactionPayer;

public class NWayCompoundTransaction implements ProcedureCallback {

    public static final int HISTOGRAM_SIZE_MS = 1000;
    long delay;
    long payerId;
    long txnId;
    Date startDate;
    Date startCheckDate;
    boolean isValid = true;
    SafeHistogramCache statsCache = SafeHistogramCache.getInstance();
    HashMap<Long, Long> payeeList = new HashMap<>();

    public NWayCompoundTransaction(long payerId, long txnId, long delay) {
        super();
        this.payerId = payerId;
        this.txnId = txnId;
        this.delay = delay;

    }

    public void addPayee(long payee, long amount) {
        payeeList.put(payee, amount);
    }

    public void createTransactions(Client c, long txnId) {

        startDate = new Date(System.currentTimeMillis());
        final Date effectiveDate = new Date(startDate.getTime() + delay);

        long payeeAmount = 0;

        statsCache.incCounter("Calls");

        long[] payees = new long[payeeList.size()];
        long[] payeeAmounts = new long[payeeList.size()];
        int listEntryid = 0;

        try {
            Iterator<Entry<Long, Long>> it = payeeList.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Long, Long> pair = it.next();
                payeeAmount = payeeAmount - pair.getValue();

                payees[listEntryid] = pair.getKey();
                payeeAmounts[listEntryid] = pair.getValue();
                listEntryid++;

            }

            c.callProcedure(this, "CompoundPayment", payerId, txnId, payees, payeeAmounts, effectiveDate);

        } catch (Exception e) {
            isValid = false;
            statsCache.reportLatency("ERROR_" + e.getMessage(), startDate.getTime(), "", HISTOGRAM_SIZE_MS);
        }
    }

    @Override
    public void clientCallback(ClientResponse response) throws Exception {

        if (response.getStatus() != ClientResponse.SUCCESS) {
            isValid = false;
            statsCache.reportLatency("ERROR_" + response.getStatusString(), startDate.getTime(), "", HISTOGRAM_SIZE_MS);
        } else if (!response.getAppStatusString().equals(StartTransactionPayer.DONE_MESSAGE)) {
            msg(response.getAppStatusString());
            isValid = false;
            statsCache.reportLatency("ERROR_" + response.getAppStatusString(), startDate.getTime(), "",
                    HISTOGRAM_SIZE_MS);
        }

        if (isValid) {
            statsCache.reportLatency("DONE", startDate.getTime(), "", HISTOGRAM_SIZE_MS);
            // TODO
            // theChecker.addTransactionToCheck(this);
        } else {

            statsCache.reportLatency("FAIL", startDate.getTime(), "", HISTOGRAM_SIZE_MS);

        }

    }

    public Date getMinCheckDate() {
        return new Date(startDate.getTime() + delay + 1);
    }

    public void startCheck() {
        startCheckDate = new Date(System.currentTimeMillis());

    }

    /**
     * @return the delay
     */
    public long getDelay() {
        return delay;
    }

    /**
     * @param delay the delay to set
     */
    public void setDelay(long delay) {
        this.delay = delay;
    }

    /**
     * @return the payerId
     */
    public long getPayerId() {
        return payerId;
    }

    /**
     * @param payerId the payerId to set
     */
    public void setPayerId(long payerId) {
        this.payerId = payerId;
    }

    /**
     * @return the txnId
     */
    public long getTxnId() {
        return txnId;
    }

    /**
     * @return the startDate
     */
    public Date getStartDate() {
        return startDate;
    }

    /**
     * @return the startCheckDate
     */
    public Date getStartCheckDate() {
        return startCheckDate;
    }

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

}

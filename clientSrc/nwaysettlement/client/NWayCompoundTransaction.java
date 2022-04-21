/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
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

package nwaysettlement.client;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.voltutil.stats.SafeHistogramCache;

import nwayprocedures.CompoundPayment;
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
    HashMap<Long, Long> payeeList = new HashMap<Long, Long>();
  

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
        } else if (! response.getAppStatusString().equals(CompoundPayment.STC_FINISH)) {
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

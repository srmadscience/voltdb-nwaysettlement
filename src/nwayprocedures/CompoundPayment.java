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

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltCompoundProcedure;
import org.voltdb.client.ClientResponse;

public class CompoundPayment extends VoltCompoundProcedure {
    
    public static final String STC_START = "STC_START";
    public static final String STC_LAUNCH = "STC_LAUNCH";
    public static final String STC_PAYERDONE = "STC_PAYERDONE";
    public static final String STC_ALLDONE = "STC_ALLDONE";
    public static final String STC_FINISH = "STC_FINISH";

    static VoltLogger LOG = new VoltLogger("CompoundPayment");

    long payerId, txnId, delay;
    long[] payeeId, amounts;
    Date effectiveDate;
    boolean errorsReported;

    public long run(long payerId, long txnId, long[] payeeId, long[] amounts, Date effectiveDate) {

        this.setAppStatusString(STC_START);

        // Save inputs
        this.payerId = payerId;

        this.txnId = txnId;
        this.payeeId = payeeId;
        this.amounts = amounts;
        this.effectiveDate = effectiveDate;

        if (amounts.length != payeeId.length) {
            LOG.error(StartTransactionPayer.PAYEE_LIST_MISMATCH_MESSAGE);
            this.setAppStatusCode(StartTransactionPayer.PAYEE_LIST_MISMATCH_CODE);
            this.setAppStatusString(StartTransactionPayer.PAYEE_LIST_MISMATCH_MESSAGE);
            return 0l;
        }

        // Build stages
        newStageList(this::launchTransactions).then(this::markPayerDone).then(this::finishPayees).then(this::finish).build();
        return 0L;
    }

    private void launchTransactions(ClientResponse[] unused) {
        
        this.setAppStatusString(STC_LAUNCH);

        long payerAmount = 0;

        for (int i = 0; i < amounts.length; i++) {
            payerAmount -= amounts[i];
            queueProcedureCall("StartTransactionPayee", payeeId[i], payerId, amounts[i], txnId, effectiveDate);
        }

        queueProcedureCall("StartTransactionPayer", payerId, payerAmount, txnId, effectiveDate, amounts.length + 1);

    }

    // Process results of first stage, i.e. lookups, and perform updates
    private void markPayerDone(ClientResponse[] resp) {
        
        this.setAppStatusString(STC_PAYERDONE);

        StringBuffer errorList = checkForErrors(resp, StartTransactionPayer.PENDING_CODE, payeeId.length + 1);

        if (errorList.length() == 0) {
            queueProcedureCall("SetPayerDone", payerId, txnId, StartTransactionPayer.PAYERDONE_MESSAGE);

        } else {
            reportError(StartTransactionPayer.CANTSTART, "Unable to finishPayer: " + errorList.toString());
        }

    }

    private void finishPayees(ClientResponse[] resp) {
        
        this.setAppStatusString(STC_ALLDONE);

        StringBuffer errorList = checkForErrors(resp, StartTransactionPayer.PAYERDONE_CODE, 1);

        if (errorList.length() == 0) {
            for (int i = 0; i < amounts.length; i++) {
                queueProcedureCall("SetTransactionEntryDone", payeeId[i], txnId);
            }

            queueProcedureCall("SetTransactionEntryDone", payerId, txnId);
            
        } else {
            reportError(StartTransactionPayer.CANTSTART, "Unable to finishPayees: " + errorList.toString());
        }

    }

    private void finish(ClientResponse[] resp) {

        this.setAppStatusString(STC_FINISH);

        StringBuffer errorList = checkForErrors(resp, StartTransactionPayer.DONE_CODE, payeeId.length + 1);

        if (errorList.length() == 0) {
            completeProcedure(0L);
        } else {
            reportError(StartTransactionPayer.CANTFINISH, "Unable to Finish Tx:" + errorList.toString());
        }

    }

    private StringBuffer checkForErrors(ClientResponse[] resp, byte okCode, int expectedResponses) {

        StringBuffer errorList = new StringBuffer();
        
        if (resp.length != expectedResponses) {
            errorList.append("Quantity Error: got " + resp.length + ", expected " + expectedResponses);
        }

        for (int i = 0; i < resp.length; i++) {

            if (resp[i].getStatus() != ClientResponse.SUCCESS) {
                this.setAppStatusCode(resp[i].getStatus());
                errorList.append("DB Error ");
                errorList.append(i);
                errorList.append(' ');
                errorList.append(resp[i].getStatusString());
                errorList.append(':');
            } else if (resp[i].getAppStatus() != okCode) {
                errorList.append("App Error ");
                errorList.append(i);
                errorList.append(' ');
                errorList.append(resp[i].getAppStatusString());
                errorList.append(':');
            }
        }

        if (errorList.length() > 0) {
            LOG.error(errorList.toString());
        }
        return errorList;
    }

    // Complete the procedure after reporting errors: check if we succeeded logging
    // them
    private void completeWithErrors(ClientResponse[] resp) {
        for (ClientResponse r : resp) {
            if (r.getStatus() != ClientResponse.SUCCESS) {
                this.setAppStatusString(String.format("Failed reporting errors: %s", r.getStatusString()));
                abortProcedure(String.format("Failed reporting errors: %s", r.getStatusString()));
            }
        }

        completeProcedure(-1L);
    }

    // Report execution errors to special topic. We:
    // 1. Change the stage list so as to abandon all incomplete stages
    // and set up a new final stage
    // 2. Queue up a request, to be executed after the
    // current stage, to update the special topic
    private void reportError(String status, String statusExplanation) {
        if (!errorsReported) {
            newStageList(this::completeWithErrors).build();
            errorsReported = true;
        }
        this.setAppStatusString(status);
        queueProcedureCall("EndSpecificTransaction", txnId, status, statusExplanation);
    }
}

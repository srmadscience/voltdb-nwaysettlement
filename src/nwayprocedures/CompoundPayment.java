/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package nwayprocedures;

import java.util.Arrays;
import java.util.Date;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltCompoundProcedure;
import org.voltdb.client.ClientResponse;

public class CompoundPayment extends VoltCompoundProcedure {

    private static final int MAX_PARAMETERS = 10;

    static VoltLogger LOG = new VoltLogger("CompoundPayment");

    long payerId, txnId, delay;
    long[] payeeId, amounts;
    Date effectiveDate;
    StringBuffer errorList = new StringBuffer();

    String returnAppStatus = StartTransactionPayer.DONE_MESSAGE;
    byte returnStatusByte = StartTransactionPayer.DONE_CODE;

    public long run(long payerId, long txnId, long[] payeeId, long[] amounts, Date effectiveDate) {

        // Save inputs
        this.payerId = payerId;
        this.txnId = txnId;
        this.payeeId = payeeId;
        this.amounts = amounts;
        this.effectiveDate = effectiveDate;

        if (amounts == null || payeeId == null || effectiveDate == null) {
            LOG.error(StartTransactionPayer.PARAMETER_NULL_CODE);
            this.setAppStatusCode(StartTransactionPayer.PARAMETER_NULL_CODE);
            this.setAppStatusString(StartTransactionPayer.PARAMETER_NULL_MESSAGE);
            abortProcedure(StartTransactionPayer.PARAMETER_NULL_MESSAGE);
        }

        if (amounts.length != payeeId.length) {
            LOG.error(StartTransactionPayer.PAYEE_LIST_MISMATCH_MESSAGE);
            this.setAppStatusCode(StartTransactionPayer.PAYEE_LIST_MISMATCH_CODE);
            this.setAppStatusString(StartTransactionPayer.PAYEE_LIST_MISMATCH_MESSAGE);
            abortProcedure(StartTransactionPayer.PAYEE_LIST_MISMATCH_MESSAGE);
        }

        if (amounts.length > MAX_PARAMETERS - 1) {
            LOG.error(StartTransactionPayer.PAYEE_LIST_TOOLONG_MESSAGE);
            this.setAppStatusCode(StartTransactionPayer.PAYEE_LIST_TOOLONG_CODE);
            this.setAppStatusString(StartTransactionPayer.PAYEE_LIST_TOOLONG_MESSAGE);
            abortProcedure(StartTransactionPayer.PAYEE_LIST_TOOLONG_MESSAGE);
        }

        // Build stages
        newStageList(this::launchTransactions).then(this::markPayerDone).then(this::finishPayees)
                .then(this::optionallyReportErrors).then(this::finish).build();
        return 0L;
    }

    /**
     * Launch payer and payee Tx's. These might fail for business reasons, e.g. no
     * money
     */
    private void launchTransactions(ClientResponse[] unused) {

        long payerAmount = 0;

        for (int i = 0; i < amounts.length; i++) {
            payerAmount -= amounts[i];
            queueProcedureCall("StartTransactionPayee", payeeId[i], payerId, amounts[i], txnId, effectiveDate);
        }

        queueProcedureCall("StartTransactionPayer", payerId, payerAmount, txnId, effectiveDate, amounts.length + 1);
    }

    /**
     * If all of the payer/payee things worked set the payers status to PAYERDONE,
     * which means tx is going to finish come hell or high water...
     */
    private void markPayerDone(ClientResponse[] resp) {

        if (hasNoErrors(resp, StartTransactionPayer.PENDING_CODE, payeeId.length + 1,
                StartTransactionPayer.CANTSTART)) {
            queueProcedureCall("SetPayerDone", payerId, txnId, StartTransactionPayer.PAYERDONE_MESSAGE);
        }

    }

    /**
     * If we have set the payer to PAYERDONE set everyone to DONE. Note that even if
     * we stop executing at this point a task will do this for us if we don't.
     */
    private void finishPayees(ClientResponse[] resp) {

        if (hasNoErrors(resp, StartTransactionPayer.PAYERDONE_CODE, 1, StartTransactionPayer.CANTSTART)) {

            for (int i = 0; i < amounts.length; i++) {
                queueProcedureCall("SetTransactionEntryDone", payeeId[i], txnId);
            }

            queueProcedureCall("SetTransactionEntryDone", payerId, txnId);

        }

    }

    /**
     * If we've broken things - e.g. someone didn't have the $, try to clean up.
     * Note that a task will do this even if we don't.
     */
    private void optionallyReportErrors(ClientResponse[] resp) {

        if (!hasNoErrors(resp, StartTransactionPayer.DONE_CODE, payeeId.length + 1, StartTransactionPayer.CANTFINISH)) {
            queueProcedureCall("EndSpecificTransactionWithErrors", txnId, returnAppStatus, this.toString());
        }

    }

    /**
     * Ignore resp - not much we can do now. Set return codes and exit.
     */
    private void finish(ClientResponse[] resp) {

        this.setAppStatusCode(returnStatusByte);
        this.setAppStatusString(returnAppStatus);

        if (hasErrors()) {
            this.setAppStatusString(errorList.toString());
            completeProcedure(-1L);
        }

        completeProcedure(0L);

    }

    private boolean hasErrors() {

        if (errorList.length() > 0) {
            return true;
        }

        return false;
    }

    private boolean hasNoErrors(ClientResponse[] resp, byte okCode, int expectedResponses, String errorStatus) {

        if (errorList.length() > 0) {
            return false;
        }

        if (resp.length != expectedResponses) {
            errorList.append("Quantity Error: got " + resp.length + ", expected " + expectedResponses);
        }

        for (int i = 0; i < resp.length; i++) {

            if (resp[i].getStatus() != ClientResponse.SUCCESS) {
                returnStatusByte = resp[i].getStatus();
                errorList.append("DB Error ");
                errorList.append(i);
                errorList.append(' ');
                errorList.append(returnStatusByte);
                errorList.append(':');
                errorList.append(resp[i].getStatusString());
                errorList.append(':');
            } else if (resp[i].getAppStatus() != okCode) {
                returnStatusByte = resp[i].getAppStatus();
                errorList.append("App Error ");
                errorList.append(i);
                errorList.append(' ');
                errorList.append(returnStatusByte);
                errorList.append(':');
                errorList.append(resp[i].getAppStatusString());
                errorList.append(':');
            }
        }

        if (errorList.length() > 0) {
            returnAppStatus = errorStatus;
            LOG.error(toString());
            return false;
        }

        return true;

    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CompoundPayment [payerId=");
        builder.append(payerId);
        builder.append(", txnId=");
        builder.append(txnId);
        builder.append(", delay=");
        builder.append(delay);
        builder.append(", payeeId=");
        builder.append(Arrays.toString(payeeId));
        builder.append(", amounts=");
        builder.append(Arrays.toString(amounts));
        builder.append(", effectiveDate=");
        builder.append(effectiveDate);
        builder.append(", errorList=");
        builder.append(errorList);
        builder.append(", returnAppStatus=");
        builder.append(returnAppStatus);
        builder.append(", statusByte=");
        builder.append(returnStatusByte);
        builder.append("]");
        return builder.toString();
    }
}

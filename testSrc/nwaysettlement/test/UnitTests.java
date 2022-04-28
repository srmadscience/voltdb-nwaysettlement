package nwaysettlement.test;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import nwayprocedures.StartTransactionPayer;
import nwaysettlement.client.TestClient;

class UnitTests {

    // final String HOSTNAME = "34.249.100.202";
    final String HOSTNAME = "localhost";
    final String[] tablesToDelete = { "user_balances", "user_transactions" };
    Client c = null;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {

        c = TestClient.connectVoltDB(HOSTNAME);
        deleteData();

    }

    @AfterEach
    void tearDown() throws Exception {

        c.drain();
        c.close();
        c = null;
    }

    @Test
    void nullParams() {

        msg("nullParams");

        long payerId = 1;
        long txnId = 1;
        long[] payeeId = null;
        long[] amounts = null;
        Date effectiveDate = new Date();

        try {
            @SuppressWarnings("unused")
            ClientResponse cr = c.callProcedure("CompoundPayment", payerId, txnId, payeeId, amounts, effectiveDate);
            fail("should not get here");
        } catch (IOException e) {
            fail(e);
        } catch (ProcCallException e) {
            if (!e.getMessage().endsWith(StartTransactionPayer.PARAMETER_NULL_MESSAGE)) {
                fail(e.getMessage());
            }
        }

    }

    @Test
    void paramListMismatch() {

        msg("paramListMismatch");

        long payerId = 1;
        long txnId = 1;
        long[] payeeId = { 0l };
        long[] amounts = { 0l, 1l };
        Date effectiveDate = new Date();

        try {
            @SuppressWarnings("unused")
            ClientResponse cr = c.callProcedure("CompoundPayment", payerId, txnId, payeeId, amounts, effectiveDate);
            fail("should not get here");
        } catch (IOException e) {
            fail(e);
        } catch (ProcCallException e) {
            if (!e.getMessage().endsWith(StartTransactionPayer.PAYEE_LIST_MISMATCH_MESSAGE)) {
                fail(e.getMessage());
            }
        }

    }

    @Test
    void noPayer() {

        msg("noPayer");

        long payerId = 1;
        long txnId = 1;
        long[] payeeId = { 0l };
        long[] amounts = { 1l };
        Date effectiveDate = new Date();

        try {
            ClientResponse cr = c.callProcedure("CompoundPayment", payerId, txnId, payeeId, amounts, effectiveDate);
            if (cr.getAppStatus() != StartTransactionPayer.NO_SUCH_USER_CODE) {
                fail("should not get here");
            }

        } catch (IOException e) {
            fail(e);
        } catch (ProcCallException e) {
            fail(e);
        }

    }

    @Test
    void staleDate() {

        msg("staleDate");

        long payerId = 1;
        long txnId = 1;
        long[] payeeId = { 2l, 3l };
        long[] amounts = { 100, 100 };
        Date effectiveDate = new Date(System.currentTimeMillis() - 10000);

        createBalance(payerId, 500);

        for (int i = 0; i < payeeId.length; i++) {
            createBalance(payeeId[i], amounts[i]);
        }

        try {
            ClientResponse cr = c.callProcedure("CompoundPayment", payerId, txnId, payeeId, amounts, effectiveDate);
            if (cr.getAppStatus() != StartTransactionPayer.MISSED_EFFECTIVE_DATE_CODE) {
                fail("should not get here");
            }

        } catch (IOException e) {
            fail(e);
        } catch (ProcCallException e) {
            fail(e);
        }

    }

    @Test
    void smokeTest() {

        msg("smokeTest");

        long payerId = 1;
        long txnId = 1;
        long[] payeeId = { 2l, 3l };
        long[] amounts = { 100, 50 };
        Date effectiveDate = new Date(System.currentTimeMillis() + 10000);

        createBalance(payerId, 5000);

        for (long element : payeeId) {
            createBalance(element, 500);
        }

        try {
            ClientResponse cr = c.callProcedure("CompoundPayment", payerId, txnId, payeeId, amounts, effectiveDate);
            if (cr.getAppStatus() != StartTransactionPayer.DONE_CODE) {
                msg("afterbad");
                fail("should not get here");
            }

            if (getBalance(payerId) != 4850) {
                fail("wrong balance");
            }

            msg("after2");
            if (getBalance(2) != 600) {
                fail("wrong balance");
            }

            if (getBalance(3) != 550) {
                fail("wrong balance");
            }

        } catch (IOException e) {
            fail(e);
        } catch (ProcCallException e) {
            fail(e);
        }

    }

    @Test
    void missingPayee() {

        msg("missingPayee");

        long payerId = 1;
        long txnId = 1;
        long[] payeeId = { 2l, 3l };
        long[] amounts = { 100, 50 };
        Date effectiveDate = new Date(System.currentTimeMillis() + 10000);

        createBalance(payerId, 5000);

        for (long element : payeeId) {
            createBalance(element, 500);
        }

        payeeId[1] = 42; // Payee 2 is wrong

        try {
            ClientResponse cr = c.callProcedure("CompoundPayment", payerId, txnId, payeeId, amounts, effectiveDate);
            if (cr.getAppStatus() != StartTransactionPayer.NO_SUCH_USER_CODE) {
                fail("should not get here");
            }

        } catch (IOException e) {
            fail(e);
        } catch (ProcCallException e) {
            fail(e);
        }

    }

    @Test
    void notEnoughMoney() {

        msg("notEnoughMoney");

        long payerId = 1;
        long txnId = 1;
        long[] payeeId = { 2l, 3l };
        long[] amounts = { 250, 251 };
        Date effectiveDate = new Date(System.currentTimeMillis() + 10000);

        createBalance(payerId, 499);

        for (long element : payeeId) {
            createBalance(element, 500);
        }

        try {
            ClientResponse cr = c.callProcedure("CompoundPayment", payerId, txnId, payeeId, amounts, effectiveDate);
            if (cr.getAppStatus() != StartTransactionPayer.NOT_ENOUGH_MONEY_CODE) {
                fail("should not get here");
            }

        } catch (IOException e) {
            fail(e);
        } catch (ProcCallException e) {
            fail(e);
        }

    }

    @Test
    void paramLengtMessage() {

        msg("paramLengtMessage");

        long payerId = 1;
        long txnId = 1;
        long[] payeeId = new long[11];
        long[] amounts = new long[11];
        Date effectiveDate = new Date();

        try {
            @SuppressWarnings("unused")
            ClientResponse cr = c.callProcedure("CompoundPayment", payerId, txnId, payeeId, amounts, effectiveDate);
            fail("should not get here");
        } catch (IOException e) {
            fail(e);
        } catch (ProcCallException e) {
            if (!e.getMessage().endsWith(StartTransactionPayer.PAYEE_LIST_TOOLONG_MESSAGE)) {
                fail(e.getMessage());
            }
        }

    }

    private void deleteData() {

        msg("deleteData");

        for (String element : tablesToDelete) {
            String deleteCommand = "DELETE FROM " + element + ";";
            msg(deleteCommand);
            try {
                c.callProcedure("@AdHoc", deleteCommand);
            } catch (Exception e) {
                fail(e);
            }
        }

    }

    private void createBalance(long acId, long balanceAmount) {
        try {
            c.callProcedure("user_balances.UPSERT", acId, balanceAmount, new Date());
        } catch (Exception e) {
            fail(e);
        }
    }

    private long getBalance(long acId) {

        long balanceAmount = -1;
        try {
            ClientResponse cr = c.callProcedure("GetBalance", acId);
            cr.getResults()[0].advanceRow();
            balanceAmount = cr.getResults()[0].getLong("balance_amount");
        } catch (Exception e) {
            fail(e);
        }

        return balanceAmount;
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

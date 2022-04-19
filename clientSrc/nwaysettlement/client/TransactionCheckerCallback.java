package nwaysettlement.client;

import java.util.Date;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.voltutil.stats.SafeHistogramCache;

public class TransactionCheckerCallback implements ProcedureCallback {

	SafeHistogramCache statsCache = SafeHistogramCache.getInstance();
	Date startDate;
	Date startCheckDate;
	long delay;
	long payerId;
	long txnId;

	public TransactionCheckerCallback(long payerId, long txnId, Date startDate, Date startCheckDate, long delay) {
		this.payerId = payerId;
		this.txnId = txnId;
		this.startDate = startDate;
		this.startCheckDate = startCheckDate;
		
		this.delay = delay;
	}

	@Override
	public void clientCallback(ClientResponse response) throws Exception {

		if (response.getStatus() != ClientResponse.SUCCESS) {
			statsCache.reportLatency("ERRORCHECK_" + response.getStatusString(), startDate.getTime(), "",
					NWayTransaction.HISTOGRAM_SIZE_MS);
		} else {

			VoltTable theStatusTable = response.getResults()[2];

			if (theStatusTable.advanceRow()) {

				String status = theStatusTable.getString("TRAN_STATUS");
				reportStatus(status);
				
			} else {
				reportStatus("DONE");
			}
		}

	}

	private void reportStatus(String status) {
		statsCache.reportLatency("TRANSACTION_E2E_" + status, startDate.getTime(), "",
				NWayTransaction.HISTOGRAM_SIZE_MS);
		statsCache.reportLatency("TRANSACTION_CHECK_" + status, startCheckDate.getTime(), "",
				NWayTransaction.HISTOGRAM_SIZE_MS);
	}

}

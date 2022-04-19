package nwaysettlement.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.voltdb.client.Client;

public class NwayTransactionChecker implements Runnable {

	ArrayList<NWayTransaction> theQueue = new ArrayList<NWayTransaction>();
	Client c;
	boolean keepGoing = true;

	long processingTime;
	
	public NwayTransactionChecker(Client c, long processingTime) {
		this.c = c;
		this.processingTime = processingTime;
	}

	public void addTransactionToCheck(NWayTransaction tranToCheck) {

		synchronized (theQueue) {
			theQueue.add(tranToCheck);
		}
		
		

	}

	@Override
	public void run() {

		while (keepGoing) {

			synchronized (theQueue) {

				NWayTransaction[] tempQueue = new NWayTransaction[theQueue.size()];
				tempQueue = theQueue.toArray(tempQueue);
				theQueue.clear();

				for (int i = 0; i < tempQueue.length; i++) {
					 
					Date minDate = new Date(tempQueue[i].getMinCheckDate().getTime() + processingTime);
					
					if (minDate.after(new Date())) {
						theQueue.add(tempQueue[i]);
					} else {
						NWayTransaction tranToCheck = tempQueue[i];
						tranToCheck.startCheck();

						try {
							c.callProcedure(
									new TransactionCheckerCallback(tranToCheck.getPayerId(), tranToCheck.getTxnId(),
											tranToCheck.startDate, tranToCheck.startCheckDate, tranToCheck.delay),
									"CheckTransactionStatus", tranToCheck.getPayerId(), tranToCheck.getTxnId());
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}

				//TestClient.msg("Q/2DX=" + theQueue.size() + "/" + checkQueue.size());
			}
			

			try {
				Thread.sleep(0, 100000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	
//	public void runOLD() {
//
//		while (keepGoing) {
//
//			ArrayList<NWayTransaction> checkQueue = new ArrayList<NWayTransaction>();
//
//			synchronized (theQueue) {
//
//				NWayTransaction[] tempQueue = new NWayTransaction[theQueue.size()];
//				tempQueue = theQueue.toArray(tempQueue);
//				theQueue.clear();
//
//				for (int i = 0; i < tempQueue.length; i++) {
//					 Date minDate = tempQueue[i].getMinCheckDate();
//					
//					if (minDate.after(new Date())) {
//						theQueue.add(tempQueue[i]);
//					} else {
//						checkQueue.add(tempQueue[i]);
//					}
//				}
//
//				//TestClient.msg("Q/2DX=" + theQueue.size() + "/" + checkQueue.size());
//			}
//			
//
//			while (checkQueue.size() > 0) {
//
//				NWayTransaction tranToCheck = checkQueue.remove(0);
//				tranToCheck.startCheck();
//
//				try {
//					c.callProcedure(
//							new TransactionCheckerCallback(tranToCheck.getPayerId(), tranToCheck.getTxnId(),
//									tranToCheck.startDate, tranToCheck.startCheckDate, tranToCheck.delay),
//							"CheckTransactionStatus", tranToCheck.getPayerId(), tranToCheck.getTxnId());
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//
//			}
//
//			try {
//				Thread.sleep(0, 100000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//
//	}
//	

	

	/**
	 * @param keepGoing the keepGoing to set
	 */
	public void setKeepGoing(boolean keepGoing) {
		this.keepGoing = keepGoing;
	}

//	public void checkNow(NWayTransaction tranToCheck) {
//		tranToCheck.startCheck();
//
//		try {
//			c.callProcedure(
//					new TransactionCheckerCallback(tranToCheck.getPayerId(), tranToCheck.getTxnId(),
//							tranToCheck.startDate, tranToCheck.startCheckDate, tranToCheck.delay),
//					"CheckTransactionStatus", tranToCheck.getPayerId(), tranToCheck.getTxnId());
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//	}

}

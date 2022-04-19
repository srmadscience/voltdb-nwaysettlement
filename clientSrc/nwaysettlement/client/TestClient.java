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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.voltutil.stats.SafeHistogramCache;
import org.voltdb.voltutil.stats.StatsHistogram;

public class TestClient {

	public static void main(String[] args) {
		try {

			msg(Arrays.toString(args));
			SafeHistogramCache statsCache = SafeHistogramCache.getInstance();

			Client c = connectVoltDB(args[0]);
			Client c2 = connectVoltDB(args[0]);
			int tpMs = Integer.parseInt(args[1]);
			int userCount = Integer.parseInt(args[2]);
			int tranCount = Integer.parseInt(args[3]);
			int delay = Integer.parseInt(args[4]);
			int processingtime = Integer.parseInt(args[5]);
			int participants = Integer.parseInt(args[6]);
			int offset = Integer.parseInt(args[7]);

			msg("Hosts=" + args[0]);
			msg("tpMs=" + tpMs);
			msg("userCount=" + userCount);
			msg("tranCount=" + tranCount);
			msg("delay=" + delay);
			msg("processingtime=" + processingtime);
			msg("participants=" + participants);
			msg("offset=" + offset);


			int tpThisMs = 0;
			long currentMs = System.currentTimeMillis();
			long lastQueryMs = System.currentTimeMillis();

			NwayTransactionChecker theChecker = new NwayTransactionChecker(c2, processingtime);

			NullCallback nc = new NullCallback();

			for (int i = 0; i < userCount; i++) {
				c.callProcedure(nc, "user_balances.UPSERT", i + offset, 1000000, new Date());

				if (tpThisMs++ > tpMs) {

					// but sleep if we're moving too fast...
					while (currentMs == System.currentTimeMillis()) {
						Thread.sleep(0, 50000);
					}

					currentMs = System.currentTimeMillis();
					tpThisMs = 0;
				}
			}

			c.drain();

			msg("DELETE FROM user_transactions;");
			c.callProcedure("@AdHoc", "DELETE FROM user_transactions;");

			msg("Waiting for 10 seconds");
			Thread.sleep(10000);

			Random r = new Random();

			Thread thread = new Thread(theChecker);
			thread.start();

			long txnId = 0;

			for (int i = 0; i < tranCount; i++) {

				txnId++;

				int randomFrom = r.nextInt(userCount) + offset;

				NWayTransaction newTran = new NWayTransaction(randomFrom, txnId, delay, theChecker);

				for (int j = 0; j < participants; j++) {
					long amount = r.nextInt(100);
					newTran.addPayee(((randomFrom + 1 + j) % userCount) + offset, amount);
				}

				newTran.createTransactions(c, txnId);

				if (tpThisMs++ > tpMs) {

					// but sleep if we're moving too fast...
					while (currentMs == System.currentTimeMillis()) {
						Thread.sleep(0, 50000);
					}

					currentMs = System.currentTimeMillis();
					tpThisMs = 0;
				}

				if (lastQueryMs + 15000 < System.currentTimeMillis()) {

					lastQueryMs = System.currentTimeMillis();

					msg("row " + i);
					ClientResponse cr = c.callProcedure("@AdHoc",
							"select * from TRANSACTION_STATUS order by tran_status;");
					while (cr.getResults()[0].advanceRow()) {

						String tranStatus = cr.getResults()[0].getString("TRAN_STATUS");
						long howMany = cr.getResults()[0].getLong("HOW_MANY");
						reportStats(c, "status", "status", tranStatus, "howmany", howMany);

					}

					reportStats(c, "tpms", "tpms", "client", "tpms", tpMs);
					reportStats(c, "tpms", "tpms", "client", "count", i);

					getStats(statsCache, c, delay, participants);
				}

			}
			c.drain();

			while (theChecker.theQueue.size() > 0) {
				msg("queue size=" + theChecker.theQueue.size());
				Thread.sleep(1000);
			}

			c2.drain();
			theChecker.setKeepGoing(false);

			for (int i = 0; i < 2; i++) {
				Thread.sleep(1000);
				ClientResponse cr = c.callProcedure("@AdHoc", "select * from TRANSACTION_STATUS order by tran_status;");
				while (cr.getResults()[0].advanceRow()) {

					String tranStatus = cr.getResults()[0].getString("TRAN_STATUS");
					long howMany = cr.getResults()[0].getLong("HOW_MANY");
					reportStats(c, "status", "status", tranStatus, "howmany", howMany);

				}
			}
			msg("done");

			getStats(statsCache, c, delay, participants);
			msg("waiting 20 seconds");
			Thread.sleep(20000);
			msg(statsCache.toString());
			getZeroedStats(statsCache, c);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void getStats(SafeHistogramCache statsCache, Client c, int delay, int participants)
			throws IOException, NoConnectionsException, ProcCallException {
		String[] statNames = { "INSERT_DONE", "INSERT_FAIL", "TRANSACTION_E2E_DONE", "TRANSACTION_E2E_PENDING",
				"TRANSACTION_FAILED_NOPAYER", "TRANSACTION_FAILED_QTY" };

		StatsHistogram doneHist = statsCache.get("INSERT_DONE");
		StatsHistogram failHist = statsCache.get("INSERT_FAIL");

		reportStats(c, "delay", "delay", "processing_lag_ms", "planned", delay);
		reportStats(c, "size", "size", "number_participants", "participants", participants);
		reportStats(c, "avg", "avg", "AVG_LATENCY", "INSERT_DONE", (long) doneHist.getLatencyAverage());
		reportStats(c, "avg", "avg", "AVG_LATENCY", "INSERT_FAIL", (long) failHist.getLatencyAverage());

		float[] pctiles = { 50, 90, 95, 99, 99.5f, 99.95f, 100 };

		for (int j = 0; j < statNames.length; j++) {

			StatsHistogram aHistogram = statsCache.get(statNames[j]);

			for (int i = 0; i < pctiles.length; i++) {
				reportStats(c, "lcy", "lcy", "LATENCY_" + pctiles[i], statNames[j],
						(long) aHistogram.getLatencyPct(pctiles[i]));
			}

			long count = (long) aHistogram.getEventTotal();

			reportStats(c, "count", "count", "COUNT_" + statNames[j], "COUNT_" + statNames[j], count);
		}
	}

	private static void getZeroedStats(SafeHistogramCache statsCache, Client c)
			throws IOException, NoConnectionsException, ProcCallException {
		String[] statNames = { "INSERT_DONE", "INSERT_FAIL", "TRANSACTION_E2E_DONE", "TRANSACTION_E2E_PENDING",
				"TRANSACTION_FAILED_NOPAYER", "TRANSACTION_FAILED_QTY" };

		reportStats(c, "delay", "delay", "processing_lag_ms", "planned", 0);
		reportStats(c, "size", "size", "number_participants", "participants", 0);

		reportStats(c, "avg", "avg", "AVG_LATENCY", "INSERT_DONE", 0);
		reportStats(c, "avg", "avg", "AVG_LATENCY", "INSERT_FAIL", 0);

		float[] pctiles = { 50, 90, 95, 99, 99.5f, 99.95f, 100 };

		for (int j = 0; j < statNames.length; j++) {

			StatsHistogram aHistogram = statsCache.get(statNames[j]);

			for (int i = 0; i < pctiles.length; i++) {
				reportStats(c, "lcy", "lcy", "LATENCY_" + pctiles[i], statNames[j], 0);
			}

			reportStats(c, "count", "count", "COUNT_" + statNames[j], "COUNT_" + statNames[j], 0);
		}
	}

	private static void reportStats(Client c, String statname, String stathelp, String eventType, String eventName,
			long statvalue) throws IOException, NoConnectionsException, ProcCallException {
		ComplainOnErrorCallback coec = new ComplainOnErrorCallback();

		c.callProcedure(coec, "promBL_latency_stats.UPSERT", statname, stathelp, eventType, eventName, statvalue,
				new Date());

	}

	private static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
		Client client = null;
		ClientConfig config = null;

		try {
			msg("Logging into VoltDB");

			config = new ClientConfig(); // "admin", "idontknow");
			config.setTopologyChangeAware(true);
			config.setReconnectOnConnectionLoss(true);

			client = ClientFactory.createClient(config);

			String[] hostnameArray = commaDelimitedHostnames.split(",");

			for (int i = 0; i < hostnameArray.length; i++) {
				msg("Connect to " + hostnameArray[i] + "...");
				try {
					client.createConnection(hostnameArray[i]);
				} catch (Exception e) {
					msg(e.getMessage());
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
		}

		return client;

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

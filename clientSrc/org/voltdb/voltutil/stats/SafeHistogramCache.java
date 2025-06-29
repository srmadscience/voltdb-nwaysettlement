package org.voltdb.voltutil.stats;

/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

import java.util.HashMap;

public class SafeHistogramCache {

    private static SafeHistogramCache instance = null;

    HashMap<String, StatsHistogram> theHistogramMap = new HashMap<>();
    HashMap<String, Long> theCounterMap = new HashMap<>();
    HashMap<String, SizeHistogram> theSizeHistogramMap = new HashMap<>();

    final int DEFAULT_SIZE = 100;

    long lastStatsTime = System.currentTimeMillis();

    protected SafeHistogramCache() {
        // Exists only to defeat instantiation.
    }

    public static SafeHistogramCache getInstance() {
        if (instance == null) {
            instance = new SafeHistogramCache();
        }
        return instance;
    }

    public void reset() {
        synchronized (theHistogramMap) {
            synchronized (theCounterMap) {
                synchronized (theSizeHistogramMap) {
                    theHistogramMap = new HashMap<>();
                    theCounterMap = new HashMap<>();
                    theSizeHistogramMap = new HashMap<>();

                }
            }
        }
    }

    public StatsHistogram get(String type) {
        StatsHistogram h = null;

        synchronized (theHistogramMap) {
            h = theHistogramMap.get(type);

            if (h == null) {
                h = new StatsHistogram(DEFAULT_SIZE);
                theHistogramMap.put(type, h);
            }
        }

        return h;
    }

    public void clear(String type) {

        StatsHistogram oldH = null;
        StatsHistogram newH = null;

        synchronized (theHistogramMap) {
            oldH = theHistogramMap.remove(type);

            if (oldH == null) {
                newH = new StatsHistogram(100);
            } else {
                newH = new StatsHistogram(oldH.maxSize);
            }
            theHistogramMap.put(type, newH);

        }

    }

    public SizeHistogram getSize(String type) {
        SizeHistogram h = null;

        synchronized (theSizeHistogramMap) {
            h = theSizeHistogramMap.get(type);

            if (h == null) {
                h = new SizeHistogram(type, DEFAULT_SIZE);
                theSizeHistogramMap.put(type, h);
            }
        }

        return h;
    }

    public long getCounter(String type) {
        Long l = new Long(0);

        synchronized (theCounterMap) {
            l = theCounterMap.get(type);
            if (l == null) {
                return 0;
            }
        }

        return l.longValue();
    }

    public void setCounter(String type, long value) {

        synchronized (theCounterMap) {
            Long l = theCounterMap.get(type);
            if (l == null) {
                l = new Long(value);

            }
            theCounterMap.put(type, l);
        }

    }

    public void incCounter(String type) {

        synchronized (theCounterMap) {
            Long l = theCounterMap.get(type);
            if (l == null) {
                l = new Long(0);
            }
            theCounterMap.put(type, l.longValue() + 1);
        }

    }

    public void report(String type, int value, String comment, int defaultSize) {

        synchronized (theHistogramMap) {
            StatsHistogram h = theHistogramMap.get(type);
            if (h == null) {
                h = new StatsHistogram(type, defaultSize);
                theHistogramMap.put(type, h);
            }
            h.report(value, comment);

        }

    }

    public void reportSize(String type, int size, String comment, int defaultSize) {

        synchronized (theSizeHistogramMap) {
            SizeHistogram h = theSizeHistogramMap.get(type);
            if (h == null) {
                h = new SizeHistogram(type, defaultSize);
                theSizeHistogramMap.put(type, h);
            }

            h.inc(size, comment);

        }

    }

    public void reportLatency(String type, long start, String comment, int defaultSize) {

        synchronized (theHistogramMap) {
            StatsHistogram h = theHistogramMap.get(type);
            if (h == null) {
                h = new StatsHistogram(type, defaultSize);

            }
            h.reportLatency(start, comment);
            theHistogramMap.put(type, h);
        }

    }

    public StatsHistogram subtractTimes(String bigHist, String smallHist, String name) {

        StatsHistogram hBig;
        StatsHistogram hSmall;
        synchronized (theHistogramMap) {
            hBig = theHistogramMap.get(bigHist);
            hSmall = theHistogramMap.get(smallHist);
        }
        StatsHistogram delta = StatsHistogram.subtract(name, hBig, hSmall);

        synchronized (theHistogramMap) {
            theHistogramMap.put(name, delta);
        }

        return delta;

    }

    /**
     * @return true if we have stats to report...
     */
    public boolean hasStats() {

        synchronized (theHistogramMap) {
            synchronized (theCounterMap) {
                synchronized (theSizeHistogramMap) {

                    if (!theHistogramMap.isEmpty() || !theCounterMap.isEmpty() || !theSizeHistogramMap.isEmpty()) {
                        return true;
                    }

                }
            }
        }
        return false;

    }

    @Override
    public String toString() {
        String data = "";
        synchronized (theHistogramMap) {
            synchronized (theCounterMap) {
                synchronized (theSizeHistogramMap) {

                    data = theHistogramMap.toString() + System.lineSeparator() + theCounterMap.toString()
                            + System.lineSeparator() + theSizeHistogramMap.toString();
                }
            }
        }

        return data;
    }

    public String toStringIfOlderThanMs(int statsInterval) {

        String data = "";

        if (lastStatsTime + statsInterval < System.currentTimeMillis()) {
            synchronized (theHistogramMap) {

                data = theHistogramMap.toString();
                lastStatsTime = System.currentTimeMillis();
            }

        }
        return data;
    }

    public void initSize(String name, int batchSize, String description) {

        synchronized (theSizeHistogramMap) {
            SizeHistogram h = theSizeHistogramMap.get(name);
            if (h == null) {

                h = new SizeHistogram(name, batchSize);
                h.setDescription(description);
                theSizeHistogramMap.put(name, h);

            }

        }

    }

    public void init(String name, int batchSize, String description) {

        synchronized (theHistogramMap) {
            StatsHistogram h = theHistogramMap.get(name);
            if (h == null) {
                h = new StatsHistogram(name, batchSize);
                h.setDescription(description);
                theHistogramMap.put(name, h);
            }

        }

    }

}
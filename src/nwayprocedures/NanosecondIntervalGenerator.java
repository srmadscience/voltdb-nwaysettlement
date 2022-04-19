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

package nwayprocedures;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.voltdb.task.*;
import org.voltdb.utils.CompoundErrors;

public class NanosecondIntervalGenerator implements IntervalGenerator {

	TaskHelper m_helper;
    long m_durationNs = -1;

    public static String validateParameters(TaskHelper helper, int interval, String timeUnit) {
        CompoundErrors errors = new CompoundErrors();
        if (interval <= 0) {
            errors.addErrorMessage("Interval must be greater than 0: " + interval);
        }
        switch (timeUnit) {
        default:
            errors.addErrorMessage("Unsupported time unit: " + timeUnit
                    + ". Must be NANOSECONDS");
            break;
        case "NANOSECONDS":
            break;
        }
        return errors.getErrorMessage();
    }

    public void initialize(TaskHelper helper, int interval, String timeUnit) {
        m_helper = helper;
        m_durationNs = TimeUnit.valueOf(timeUnit).toNanos(interval);
    }

	@Override
	public Interval getFirstInterval() {
		Function<ActionResult, Interval> m_callback  = this::getSecondInterval;
		return new Interval(m_durationNs, TimeUnit.NANOSECONDS, m_callback);
	}

	public Interval getSecondInterval(ActionResult ar) {
		Function<ActionResult, Interval> m_callback  = this::getSecondInterval;
		return new Interval(m_durationNs, TimeUnit.NANOSECONDS, m_callback);
	}

	

	  
}

/*
* Copyright 2015 Kratos Integral Systems Europe
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.kise.kairosdb.core.aggregator;

import com.kise.kairosdb.core.aggregator.JsFilterAggregator;
import com.kise.kairosdb.testing.ListDataPointGroup;
import static org.hamcrest.CoreMatchers.equalTo;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;

/**
 *
 * @author tlsfr23
 */
public class JsFilterAggregatorTest {
	
	
	@Before
	public void setUp() {
	}
	
	@Test
	public void testFilterZeros() {
		ListDataPointGroup group = new ListDataPointGroup("function");
		group.addDataPoint(new DoubleDataPoint(1, 1));
		group.addDataPoint(new DoubleDataPoint(2, 0));
		group.addDataPoint(new DoubleDataPoint(3, 3));
		group.addDataPoint(new DoubleDataPoint(4, 4));
		group.addDataPoint(new DoubleDataPoint(5, 0));
		
		JsFilterAggregator fAggregator = new JsFilterAggregator(new DoubleDataPointFactoryImpl());
		fAggregator.setScript(" value!=0;");
		DataPointGroup results = fAggregator.aggregate(group);
		
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getDoubleValue(), equalTo(1.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getDoubleValue(), equalTo(3.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(dp.getDoubleValue(), equalTo(4.0));
		
		assertFalse(results.hasNext());
	}
	
}

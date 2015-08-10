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

import com.kise.kairosdb.core.aggregator.JsFunctionAggregator;
import com.kise.kairosdb.testing.ListDataPointGroup;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RateAggregator;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;

public class JsFunctionAggregatorTest
{
	@Test(expected = NullPointerException.class)
	public void test_nullSet_invalid()
	{
		new RateAggregator(new DoubleDataPointFactoryImpl()).aggregate(null);
	}
	
	@Test
	public void test_OneFormula()
	{
		ListDataPointGroup group = new ListDataPointGroup("formula");
		group.addDataPoint(new DoubleDataPoint(1, 3.75));
		group.addDataPoint(new DoubleDataPoint(2, 4.75));
		group.addDataPoint(new DoubleDataPoint(3, 5.75));
		group.addDataPoint(new DoubleDataPoint(4, 6.75));
		
		JsFunctionAggregator fAggregator = new JsFunctionAggregator(new DoubleDataPointFactoryImpl());
		fAggregator.setScript("Math.pow((value-3.75),2)");
		DataPointGroup results = fAggregator.aggregate(group);
		
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getDoubleValue(), equalTo(0.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getDoubleValue(), equalTo(1.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getDoubleValue(), equalTo(4.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(dp.getDoubleValue(), equalTo(9.0));
	}
	
	
	@Test
	public void test_FormulaUsingTime()
	{
		ListDataPointGroup group = new ListDataPointGroup("formula");
		group.addDataPoint(new DoubleDataPoint(1, 1));
		group.addDataPoint(new DoubleDataPoint(2, 2));
		group.addDataPoint(new DoubleDataPoint(3, 3));
		group.addDataPoint(new DoubleDataPoint(4, 4));
		group.addDataPoint(new DoubleDataPoint(5, 10));
		
		JsFunctionAggregator fAggregator = new JsFunctionAggregator(new DoubleDataPointFactoryImpl());
		fAggregator.setScript("value/timestamp");
		DataPointGroup results = fAggregator.aggregate(group);
		
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getDoubleValue(), equalTo(1.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getDoubleValue(), equalTo(1.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getDoubleValue(), equalTo(1.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(dp.getDoubleValue(), equalTo(1.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(5L));
		assertThat(dp.getDoubleValue(), equalTo(2.0));
	}
	
	
	@Test
	public void test_CurrentPlusPrevious()
	{
		ListDataPointGroup group = new ListDataPointGroup("function");
		group.addDataPoint(new DoubleDataPoint(1, 1));
		group.addDataPoint(new DoubleDataPoint(2, 2));
		group.addDataPoint(new DoubleDataPoint(3, 3));
		group.addDataPoint(new DoubleDataPoint(4, 4));
		group.addDataPoint(new DoubleDataPoint(5, 6));
		
		JsFunctionAggregator fAggregator = new JsFunctionAggregator(new DoubleDataPointFactoryImpl());
		fAggregator.setScript("if( typeof this.prev_value === 'undefined'){ this.prev_value = value ; return value } \n else  {var tmp = this.prev_value; this.prev_value = value ; return tmp + this.prev_value;}");
		DataPointGroup results = fAggregator.aggregate(group);
		
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getDoubleValue(), equalTo(1.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getDoubleValue(), equalTo(3.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getDoubleValue(), equalTo(5.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(dp.getDoubleValue(), equalTo(7.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(5L));
		assertThat(dp.getDoubleValue(), equalTo(10.0));
	}
	
	@Test
	public void test_Accumulator()
	{
		ListDataPointGroup group = new ListDataPointGroup("function");
		group.addDataPoint(new DoubleDataPoint(1, 1));
		group.addDataPoint(new DoubleDataPoint(2, 2));
		group.addDataPoint(new DoubleDataPoint(3, 3));
		group.addDataPoint(new DoubleDataPoint(4, 4));
		group.addDataPoint(new DoubleDataPoint(5, 6));
		
		JsFunctionAggregator fAggregator = new JsFunctionAggregator(new DoubleDataPointFactoryImpl());
		fAggregator.setScript("if( typeof this.prev_value === 'undefined'){ this.prev_value = 0;} \n this.prev_value = value +  this.prev_value ; return this.prev_value;");
		DataPointGroup results = fAggregator.aggregate(group);
		
		DataPoint dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(1L));
		assertThat(dp.getDoubleValue(), equalTo(1.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(2L));
		assertThat(dp.getDoubleValue(), equalTo(3.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(3L));
		assertThat(dp.getDoubleValue(), equalTo(6.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(4L));
		assertThat(dp.getDoubleValue(), equalTo(10.0));
		
		dp = results.next();
		assertThat(dp.getTimestamp(), equalTo(5L));
		assertThat(dp.getDoubleValue(), equalTo(16.0));
	}
	
}

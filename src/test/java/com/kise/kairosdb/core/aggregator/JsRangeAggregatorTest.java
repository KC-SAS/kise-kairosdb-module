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

import com.kise.kairosdb.core.aggregator.JsRangeAggregator;
import com.kise.kairosdb.core.datapoints.DoubleArrayDataPoint;
import com.kise.kairosdb.testing.ListDataPointGroup;
import java.lang.reflect.InvocationTargetException;
import javax.script.ScriptException;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.AvgAggregator;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.AbstractDataPointGroup;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Sampling;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.groupby.GroupByResult;

/**
 *
 * @author Remi Dettai <rdettai at gmail.com>
 */
public class JsRangeAggregatorTest {
	
	
	@Before
	public void setUp() {
	}
	
	@Test
	public void testAverage() throws ScriptException, KairosDBException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new DoubleDataPoint(1, 10.0));
		group.addDataPoint(new DoubleDataPoint(1, 20.3));
		group.addDataPoint(new DoubleDataPoint(1, 3.0));
		group.addDataPoint(new DoubleDataPoint(2, 1.0));
		group.addDataPoint(new DoubleDataPoint(2, 3.2));
		group.addDataPoint(new DoubleDataPoint(2, 5.0));
		group.addDataPoint(new DoubleDataPoint(3, 25.1));
		
		JsRangeAggregator fAggregator = new JsRangeAggregator(new DoubleDataPointFactoryImpl(),1000);
		fAggregator.setScript("var sum = 0; for(var i=0; i<valueArray.size();i++){sum+=valueArray.get(i).getDoubleValue();} return sum/valueArray.size(); ");
		fAggregator.setAllocateArray(true);
		DataPointGroup results = fAggregator.aggregate(group);
		
		
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1L));
		assertThat(dataPoint.getDoubleValue(), equalTo(11.1));
		
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2L));
		assertThat(dataPoint.getDoubleValue(), closeTo(3.067, 2));
		
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3L));
		assertThat(dataPoint.getDoubleValue(), equalTo(25.1));
		
		assertThat(results.hasNext(), equalTo(false));
		
	}
	
	
	@Test
	public void testSum() throws ScriptException, KairosDBException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new DoubleDataPoint(1030, 10.0));
		group.addDataPoint(new DoubleDataPoint(1050, 20.3));
		group.addDataPoint(new DoubleDataPoint(1603, 3.0));
		group.addDataPoint(new DoubleDataPoint(2005, 1.0));
		group.addDataPoint(new DoubleDataPoint(2400, 3.2));
		group.addDataPoint(new DoubleDataPoint(2803, 5.0));
		group.addDataPoint(new DoubleDataPoint(3068, 25.1));
		
		JsRangeAggregator fAggregator = new JsRangeAggregator(new DoubleDataPointFactoryImpl(),1000);
		fAggregator.setSampling(new Sampling(1, TimeUnit.SECONDS));
		fAggregator.setStartTime(1000);
		fAggregator.setAlignStartTime(true);
		fAggregator.setAllocateArray(true);
		fAggregator.setScript("var count= 0.; \n" +
			"for(var i=0;i<valueArray.size();i++){\n" +
			"  count+=valueArray.get(i).getDoubleValue();\n" +
			"}\n" +
			"return count; ");
		DataPointGroup results = fAggregator.aggregate(group);
		
		
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1000L));
		assertThat(dataPoint.getDoubleValue(), equalTo(33.3));
		
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2000L));
		assertThat(dataPoint.getDoubleValue(), closeTo(9.2, 2));
		
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3000L));
		assertThat(dataPoint.getDoubleValue(), equalTo(25.1));
		
		assertThat(results.hasNext(), equalTo(false));
		
	}
	
	
	@Test
	public void testSumCount() throws ScriptException, KairosDBException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new DoubleDataPoint(1030, 10.0));
		group.addDataPoint(new DoubleDataPoint(1050, 20.3));
		group.addDataPoint(new DoubleDataPoint(1603, 3.0));
		group.addDataPoint(new DoubleDataPoint(2005, 1.0));
		group.addDataPoint(new DoubleDataPoint(2400, 3.2));
		group.addDataPoint(new DoubleDataPoint(2803, 5.0));
		group.addDataPoint(new DoubleDataPoint(3068, 25.1));
		
		JsRangeAggregator fAggregator = new JsRangeAggregator(new DoubleDataPointFactoryImpl(),1000);
		fAggregator.setSampling(new Sampling(1, TimeUnit.SECONDS));
		fAggregator.setStartTime(1000);
		fAggregator.setAlignStartTime(true);
		fAggregator.setAllocateArray(true);
		fAggregator.setArrayResult(true);
		fAggregator.setScript("var count= 0.; \n" +
			"for(var i=0;i<valueArray.size();i++){\n" +
			"  count+=valueArray.get(i).getDoubleValue();\n" +
			"}\n" +
			"return [count,valueArray.size()]; ");
		DataPointGroup results = fAggregator.aggregate(group);
		
		DoubleArrayDataPoint dataPoint = (DoubleArrayDataPoint) results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1000L));
		assertArrayEquals(dataPoint.getValueArray(), new double[]{33.3,3.},0);
		dataPoint = (DoubleArrayDataPoint)results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2000L));
		assertArrayEquals(dataPoint.getValueArray(), new double[]{9.2,3.},0);
		dataPoint = (DoubleArrayDataPoint)results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3000L));
		assertArrayEquals(dataPoint.getValueArray(), new double[]{25.1,1.},0);
		assertThat(results.hasNext(), equalTo(false));
	}
	
	
	@Test
	public void testSumStream() throws ScriptException, KairosDBException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		ListDataPointGroup group = new ListDataPointGroup("group");
		group.addDataPoint(new DoubleDataPoint(1030, 10.0));
		group.addDataPoint(new DoubleDataPoint(1050, 20.3));
		group.addDataPoint(new DoubleDataPoint(1603, 3.0));
		group.addDataPoint(new DoubleDataPoint(2005, 1.0));
		group.addDataPoint(new DoubleDataPoint(2400, 3.2));
		group.addDataPoint(new DoubleDataPoint(2803, 5.0));
		group.addDataPoint(new DoubleDataPoint(3068, 25.1));
		
		JsRangeAggregator fAggregator = new JsRangeAggregator(new DoubleDataPointFactoryImpl(),1000);
		fAggregator.setSampling(new Sampling(1, TimeUnit.SECONDS));
		fAggregator.setStartTime(1000);
		fAggregator.setAlignStartTime(true);
		fAggregator.setScript("var count= 0.; \n" +
			"while(values.hasNext()){\n" +
			"  count+=values.next().getDoubleValue();\n" +
			"}\n" +
			"return count; ");
		DataPointGroup results = fAggregator.aggregate(group);
		
		
		DataPoint dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(1000L));
		assertThat(dataPoint.getDoubleValue(), equalTo(33.3));
		
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(2000L));
		assertThat(dataPoint.getDoubleValue(), closeTo(9.2, 2));
		
		dataPoint = results.next();
		assertThat(dataPoint.getTimestamp(), equalTo(3000L));
		assertThat(dataPoint.getDoubleValue(), equalTo(25.1));
		
		assertThat(results.hasNext(), equalTo(false));
		
	}
	
	//@Test
	public void testPerf() throws ScriptException, KairosDBException{
		int totalPoints = 100000000;
		int batchSize = 1000000;
		System.out.println("Task: "+totalPoints+" points in batches of "+batchSize);
		
		// JS in STREAMING mode
		RandomDataPointGroup dpg = new RandomDataPointGroup(totalPoints, 1000);
		JsRangeAggregator fAggregator = new JsRangeAggregator(new DoubleDataPointFactoryImpl(),totalPoints);
		fAggregator.setSampling(new Sampling(batchSize, TimeUnit.SECONDS));
		fAggregator.setStartTime(0);
		fAggregator.setAlignStartTime(true);
		fAggregator.setScript("var sum = 0; while(values.hasNext()){var dp= values.next(); sum+=dp.getDoubleValue();} return sum; ");
		long startTime = System.currentTimeMillis();
		DataPointGroup results = fAggregator.aggregate(dpg);
		while(results.hasNext()){
			results.next();
		}
		System.out.println("JS aggregator: streaming mode "+(System.currentTimeMillis()-startTime) );
		
		// JS allocated in JS
		dpg = new RandomDataPointGroup(totalPoints, 1000);
		fAggregator = new JsRangeAggregator(new DoubleDataPointFactoryImpl(),totalPoints);
		fAggregator.setSampling(new Sampling(batchSize, TimeUnit.SECONDS));
		fAggregator.setStartTime(0);
		fAggregator.setAlignStartTime(true);
		fAggregator.setAllocateArray(false);
		fAggregator.setScript("var sum = 0; var a = new Array(); while(values.hasNext()){var dp= values.next(); sum+=dp.getDoubleValue();a.push(dp);} return sum; ");
		startTime = System.currentTimeMillis();
		results = fAggregator.aggregate(dpg);
		while(results.hasNext()){
			results.next();
		}
		System.out.println("JS aggregator: allocated in JS "+(System.currentTimeMillis()-startTime) );
		
		// JS allocated in java
		dpg = new RandomDataPointGroup(totalPoints, 1000);
		fAggregator = new JsRangeAggregator(new DoubleDataPointFactoryImpl(),totalPoints);
		fAggregator.setSampling(new Sampling(batchSize, TimeUnit.SECONDS));
		fAggregator.setStartTime(0);
		fAggregator.setAlignStartTime(true);
		fAggregator.setAllocateArray(true);
		fAggregator.setScript("var sum = 0; for(var i=0; i<valueArray.size();i++){sum+=valueArray.get(i).getDoubleValue();} return sum; ");
		startTime = System.currentTimeMillis();
		results = fAggregator.aggregate(dpg);
		while(results.hasNext()){
			results.next();
		}
		System.out.println("JS aggregator: allocated in java "+(System.currentTimeMillis()-startTime) );
		
		
		// NATIVE
		dpg = new RandomDataPointGroup(totalPoints, 1000);
		AvgAggregator avgAggregator = new AvgAggregator(new DoubleDataPointFactoryImpl());
		avgAggregator.setSampling(new Sampling(batchSize, TimeUnit.SECONDS));
		avgAggregator.setStartTime(0);
		avgAggregator.setAlignStartTime(true);
		startTime = System.currentTimeMillis();
		results = avgAggregator.aggregate(dpg);
		while(results.hasNext()){
			results.next();
		}	
		System.out.println("Native aggregator: "+(System.currentTimeMillis()-startTime) );
		
	}
	
	
	
	
	private class RandomDataPointGroup extends AbstractDataPointGroup{
		
		private final int nbPoint;
		private final long interval;
		private long currentPoint;
		
		public RandomDataPointGroup(int nbPoint, long interval){
			super("RandomDPG");
			this.nbPoint = nbPoint;
			this.interval = interval;
			this.currentPoint = 0;
		}
		
		@Override
		public void close() {
			
		}
		
		public boolean hasNext() {
			return currentPoint<nbPoint;
		}
		
		public DataPoint next() {
			currentPoint++;
			return new DoubleDataPoint(currentPoint*interval, Math.random());
		}
		
		
	}
}

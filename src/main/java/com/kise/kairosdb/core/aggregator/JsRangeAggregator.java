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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import static com.kise.kairosdb.core.aggregator.ScriptingAggregator.ERROR_IN_SCRIPT;
import static com.kise.kairosdb.core.aggregator.ScriptingAggregator.NAN_RESULT;
import com.kise.kairosdb.core.datapoints.DoubleArrayDataPoint;
import com.kise.kairosdb.core.http.rest.validation.ValidScript;
import com.kise.kairosdb.util.ScriptBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.Invocable;
import javax.script.ScriptException;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.exception.KairosDBException;

/**
 * Scripted implementation of the Range Aggregator
 * Some thinking has still to be put in the data structure used to accumulate points.
 * @author Remi Dettai
 */
@AggregatorName(name = "js_range", description = "A javascript custom range aggregator")
public class JsRangeAggregator extends RangeAggregator implements ScriptingAggregator{
	
	DoubleDataPointFactory m_dataPointFactory;
	private Invocable m_engine;
	
	@ValidScript(arguments = {"values","valueArray"})
	private String m_script;
	
	private boolean m_allocate =false;
	private boolean m_arrayResult =false;
	
	private final int maxBatch;
	
	@Inject
	public JsRangeAggregator(DoubleDataPointFactory dataPointFactory, @Named("MAXIMUM_SCRIPT_BATCH") int maxBatch) throws KairosDBException
	{
		m_dataPointFactory = dataPointFactory;
		this.maxBatch = maxBatch;
	}
	
	@Override
	protected RangeAggregator.RangeSubAggregator getSubAggregator()
	{
		return (new JsRangeDataPointAggregator());
	}
	
	@Override
	public boolean canAggregate(String groupType)
	{
		// could work with any value except that we specifically pass a double to the JS zhen calling dp.getDoubleValue()
		return DataPoint.GROUP_NUMBER.equals(groupType);
	}
	
	@Override
	public void setScript(String script)
	{
		m_script =  script;
		m_engine = null;
	}
	
	/**
	 * This allows to allocate an array for the datapoints in java which is faster than doing it in JS
	 * If set to true valueArray should be used in JS script
	 * If set to false values should be used in JS script
	 * @param allocate 
	 */
	public void setAllocateArray(boolean allocate){
		m_allocate = allocate;
	}
	
	public void setArrayResult(boolean allocate){
		m_arrayResult = allocate;
	}
	
	private Invocable getEngine() {
		if(m_engine==null){
			try {
				m_engine = ScriptBuilder.buildScript(m_script,getClass().getDeclaredField("m_script").getAnnotation(ValidScript.class).arguments());
			} catch (NoSuchFieldException ex) {
				Logger.getLogger(JsRangeAggregator.class.getName()).log(Level.SEVERE, "Invalid field name for annotation", ex);
			}
		}
		return m_engine;
	}
	
	private class JsRangeDataPointAggregator implements RangeAggregator.RangeSubAggregator
	{
		
		
		@Override
		public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
		{
			if(m_allocate){
				ArrayList<DataPoint> list = new ArrayList<DataPoint>();
				while (dataPointRange.hasNext())
				{
					
					DataPoint dp = dataPointRange.next();
					if (dp.isDouble())
					{
						list.add(dp);
						if(list.size()>maxBatch)
							throw new IllegalArgumentException("Number of points in aggregation range exceeds "
								+ "configured limit of "+maxBatch);
					}
				}
				if(m_arrayResult)
					return Collections.singletonList((DataPoint)new DoubleArrayDataPoint(returnTime, invokeArrayFunction(list)));
				else
					return Collections.singletonList(m_dataPointFactory.createDataPoint(returnTime, invokeFunction(list)));
			}
			else{
				Iterable<DataPoint> result;
				if(m_arrayResult)
					result = Collections.singletonList((DataPoint)new DoubleArrayDataPoint(returnTime, invokeArrayFunction(dataPointRange)));
				else
					result = Collections.singletonList(m_dataPointFactory.createDataPoint(returnTime, invokeFunction(dataPointRange)));
				// avoid infinite loop on scripts that forget to call next()
				while (dataPointRange.hasNext()) dataPointRange.next();
				return result;
			}
		}
	}
	
	
	private double invokeFunction(Object dataObject) {
		Object[] args = new Object[2];
		if(dataObject instanceof List)
			args[1]= dataObject;
		else if(dataObject instanceof Iterator)
			args[0]= dataObject;
		else
			throw new IllegalArgumentException("argument to invoke function should be "
				+ "a list or an iterator of DataPoint");
		Double returnVal =  Double.NaN;
		try {
			Object result = getEngine().invokeFunction("f", args);
			if(result instanceof Double)
				returnVal = (Double)result;
			else if(result instanceof Integer)
				returnVal = ((Integer)result).doubleValue();
			else
				throw new ClassCastException("Script result should be number, got "+result.getClass());
		} catch (ScriptException ex) {
			throw new IllegalArgumentException(ERROR_IN_SCRIPT, ex);
		} catch (NoSuchMethodException ex) {
			Logger.getLogger(JsRangeAggregator.class.getName()).log(Level.SEVERE, null, ex);
		}
		if(returnVal == Double.NaN){
			throw new IllegalStateException(NAN_RESULT);
		}
		
		return returnVal;
	}
	
	private double[] invokeArrayFunction(Object dataObject) {
		Object[] args = new Object[2];
		if(dataObject instanceof List)
			args[1]= dataObject;
		else if(dataObject instanceof Iterator)
			args[0]= dataObject;
		else
			throw new IllegalArgumentException("argument to invoke function should be "
				+ "a list or an iterator of DataPoint");
		
		double[] returnVal;
		try {
			Object result = getEngine().invokeFunction("f", args);
			final double[] finalReturnVal = new double[((ScriptObjectMirror)result).size()];
			((ScriptObjectMirror)result).forEach(new BiConsumer<String, Object>() {
				int inc=0;
				public void accept(String t, Object result) {
					if(result instanceof Double)
						finalReturnVal[inc]= (Double)result;
					else if(result instanceof Long)
						finalReturnVal[inc]= ((Long)result).doubleValue();
					else if(result instanceof Integer)
						finalReturnVal[inc]= ((Integer)result).doubleValue();
					else
						throw new ClassCastException("Script result should be number, got "+result.getClass());
					inc++;
				}
			});
			returnVal = finalReturnVal;
		} catch (ScriptException ex) {
			throw new IllegalArgumentException(ERROR_IN_SCRIPT, ex);
		} catch (NoSuchMethodException ex) {
			Logger.getLogger(JsRangeAggregator.class.getName()).log(Level.SEVERE, null, ex);
			returnVal =  new double[0];
		}
		
		return returnVal;
	}
	

}

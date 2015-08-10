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


import static com.google.common.base.Preconditions.checkNotNull;
import com.google.inject.Inject;
import com.kise.kairosdb.core.http.rest.validation.ValidScript;
import com.kise.kairosdb.util.ScriptBuilder;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.Invocable;
import javax.script.ScriptException;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;

@AggregatorName(name = "js_function", description = "Modifies value by applying a formula or a function")
public class JsFunctionAggregator implements ScriptingAggregator
{
	private DoubleDataPointFactory m_dataPointFactory;
	private Invocable m_engine;
	
	@ValidScript(arguments = {"value","timestamp"})
	private String m_script;
	
	@Inject
	public JsFunctionAggregator(DoubleDataPointFactory dataPointFactory)
	{
		m_dataPointFactory = dataPointFactory;
	}
	
	@Override
	public boolean canAggregate(String groupType)
	{
		// could work with any value except that we specifically pass a double to the JS zhen calling dp.getDoubleValue()
		return DataPoint.GROUP_NUMBER.equals(groupType);
	}
	
	@Override
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		checkNotNull(dataPointGroup);
		
		return new ScriptingDataPointGroup(dataPointGroup);
	}
	
	@Override
	public void setScript(String script)
	{
		m_script =  script;
		m_engine = null;
	}
	
	private Invocable getEngine() {
		if(m_engine==null){
			try {
				m_engine = ScriptBuilder.buildScript(m_script,getClass().getDeclaredField("m_script").getAnnotation(ValidScript.class).arguments());
			} catch (NoSuchFieldException ex) {
				Logger.getLogger(JsFunctionAggregator.class.getName()).log(Level.SEVERE, "Invalid field name for annotation", ex);
			}
		}
		return m_engine;
	}
	
	
	private class ScriptingDataPointGroup implements DataPointGroup
	{
		private DataPointGroup m_innerDataPointGroup;
		
		public ScriptingDataPointGroup(DataPointGroup innerDataPointGroup)
		{
			m_innerDataPointGroup = innerDataPointGroup;
		}
		
		@Override
		public boolean hasNext()
		{
			return (m_innerDataPointGroup.hasNext());
		}
		
		@Override
		public DataPoint next()
		{
			DataPoint dp = m_innerDataPointGroup.next();
			double result = invokeFunction(dp);
			dp = m_dataPointFactory.createDataPoint(dp.getTimestamp(), result);
			
			return (dp);
		}
		
		@Override
		public void remove()
		{
			m_innerDataPointGroup.remove();
		}
		
		@Override
		public String getName()
		{
			return (m_innerDataPointGroup.getName());
		}
		
		@Override
		public List<GroupByResult> getGroupByResult()
		{
			return (m_innerDataPointGroup.getGroupByResult());
		}
		
		@Override
		public void close()
		{
			m_innerDataPointGroup.close();
		}
		
		@Override
		public Set<String> getTagNames()
		{
			return (m_innerDataPointGroup.getTagNames());
		}
		
		@Override
		public Set<String> getTagValues(String tag)
		{
			return (m_innerDataPointGroup.getTagValues(tag));
		}
		
		private double invokeFunction(DataPoint dp) {
			double returnVal =  Double.NaN;
			try {
				Object result = getEngine().invokeFunction("f", dp.getDoubleValue(), dp.getTimestamp());
				if(result instanceof Double)
					returnVal = (Double)result;
				else if(result instanceof Integer)
					returnVal = ((Integer)result).doubleValue();
				else
					throw new ClassCastException("Script result should be number, got "+result.getClass());
			} catch (ScriptException ex) {
				throw new IllegalArgumentException(ERROR_IN_SCRIPT, ex);
			} catch (NoSuchMethodException ex) {
				Logger.getLogger(JsFunctionAggregator.class.getName()).log(Level.SEVERE, null, ex);
			}
			if(returnVal == Double.NaN){
				throw new IllegalStateException(NAN_RESULT);
			}
			return returnVal;
		}
	}
}
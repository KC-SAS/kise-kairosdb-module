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
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import com.kise.kairosdb.core.http.rest.validation.ValidScript;
import com.kise.kairosdb.util.ScriptBuilder;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.Invocable;
import javax.script.ScriptException;

@AggregatorName(name = "js_filter", description = "filters datapoints according to a boolean formula or function")
public class JsFilterAggregator implements ScriptingAggregator
{
	private DoubleDataPointFactory m_dataPointFactory;
	private Invocable m_engine;
	
	@ValidScript(arguments = {"value","timestamp"})
	private String m_script;
	
	@Inject
	public JsFilterAggregator(DoubleDataPointFactory dataPointFactory)
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
		
		return new FilteringDataPointGroup(dataPointGroup);
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
				Logger.getLogger(JsFilterAggregator.class.getName()).log(Level.SEVERE, "Invalid field name for annotation", ex);
			}
		}
		return m_engine;
	}
	
	
	private class FilteringDataPointGroup implements DataPointGroup
	{
		private DataPointGroup m_innerDataPointGroup;
		private DataPoint nextPoint;
		
		public FilteringDataPointGroup(DataPointGroup innerDataPointGroup)
		{
			m_innerDataPointGroup = innerDataPointGroup;
			nextPoint = null;
		}
		
		@Override
		public boolean hasNext()
		{
			if(nextPoint != null)
				return true;
			while(m_innerDataPointGroup.hasNext()){
				nextPoint = m_innerDataPointGroup.next();
				if(invokeFunction(nextPoint))
					break;
				else
					nextPoint=null;
			}
			return nextPoint!=null;
		}
		
		@Override
		public DataPoint next()
		{
			hasNext();
			DataPoint dp = nextPoint;
			nextPoint = null;
			return dp;
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
		
		private boolean invokeFunction(DataPoint dp) {
			boolean returnVal =  false;
			try {
                                    returnVal = (Boolean) getEngine().invokeFunction("f", dp.getDoubleValue(), dp.getTimestamp());
			} catch (ScriptException ex) {
				throw new IllegalArgumentException(ERROR_IN_SCRIPT, ex);
			} catch (NoSuchMethodException ex) {
				Logger.getLogger(JsFilterAggregator.class.getName()).log(Level.SEVERE, null, ex);
			}
			return returnVal;
		}
	}
}
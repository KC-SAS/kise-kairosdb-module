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
package com.kise.kairosdb.core.datapoints;

import java.io.DataOutput;
import java.io.IOException;
import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.datapoints.DataPointHelper;

/**
 *
 * @author Remi Dettai <rdettai at gmail.com>
 */
public class DoubleArrayDataPoint extends DataPointHelper
{
	private static final String API_TYPE = "array";
	private double[] m_values;

	public DoubleArrayDataPoint(long timestamp,  double[] values)
	{
		super(timestamp);
		m_values = values;
	}
	
	public double [] getValueArray() {
		return m_values;
	}

	@Override
	public void writeValueToBuffer(DataOutput buffer) throws IOException
	{
		//buffer.writeDouble(m_real);
		//buffer.writeDouble(m_imaginary);
	}

	@Override
	public void writeValueToJson(JSONWriter writer) throws JSONException
	{
		for(int i=0;i<m_values.length;i++)
			writer.value(m_values[i]);
	}

	@Override
	public String getApiDataType()
	{
		return API_TYPE;
	}

	@Override
	public String getDataStoreDataType()
	{
		return "array";
	}

	@Override
	public boolean isLong()
	{
		return false;
	}

	@Override
	public long getLongValue()
	{
		return 0;
	}

	@Override
	public boolean isDouble()
	{
		return false;
	}

	@Override
	public double getDoubleValue()
	{
		return 0;
	}
}

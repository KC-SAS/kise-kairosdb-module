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


package com.kise.kairosdb.core.groupby;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.formatter.FormatterException;
import org.kairosdb.core.groupby.GroupByResult;

/**
 *
 * @author lcoulet
 */
public class FiscalCalendarGroubByTest {
	
	public FiscalCalendarGroubByTest() {
	}
	
	@Before
	public void setUp() {
	}
	
	@After
	public void tearDown() {
	}

	/**
	 * Test of getGroupId method, of class FiscalCalendarGroubBy.
	 */
	@Test
	public void testGetGroupId() {
		FiscalCalendarGroubBy groupBy = new FiscalCalendarGroubBy();

		assertEquals("Should be 1st period 1970",197001,groupBy.getGroupId(new DoubleDataPoint(1000L, 1.0), null));
		assertEquals("Should be 1st period 2013",201301,groupBy.getGroupId(new DoubleDataPoint(1356951600000L, 1.0), null));
		assertEquals("Should be 1st period 2015",201501,groupBy.getGroupId(new DoubleDataPoint(1422273600000L, 1.0), null));
		assertEquals("Should be 11th period 2013",201311,groupBy.getGroupId(new DoubleDataPoint(1385809200000L, 1.0), null));
	}
	
	/**
	 * Test of getGroupId method, of class FiscalCalendarGroubBy.
	 */
	@Test
	public void testSetCalendarType() {
		FiscalCalendarGroubBy groupBy = new FiscalCalendarGroubBy();
		groupBy.setCalendarType("calendar_445");
		assertEquals("Should be 1st period 1970",197001,groupBy.getGroupId(new DoubleDataPoint(1000L, 1.0), null));
		assertEquals("Should be 1st period 2013",201301,groupBy.getGroupId(new DoubleDataPoint(1356951600000L, 1.0), null));
	        assertEquals("Should be 2nd period 2015",201502,groupBy.getGroupId(new DoubleDataPoint(1422273600000L, 1.0), null));
		assertEquals("Should be 11th period 2013",201311,groupBy.getGroupId(new DoubleDataPoint(1385809200000L, 1.0), null));
	}

	/**
	 * Test of getGroupByResult method, of class FiscalCalendarGroubBy.
	 */
	@Test
	public void testGetGroupByResult() throws FormatterException {
		FiscalCalendarGroubBy groupBy = new FiscalCalendarGroubBy();

		groupBy.getGroupId(new DoubleDataPoint(1000L, 1.0), null);
		GroupByResult groupByResult = groupBy.getGroupByResult(197001);

		assertThat(groupByResult.toJson(), equalTo("{\"name\":\"fiscal_period\",\"group\":{\"name\":\"FY1970-P1\",\"fiscal_year\":1970,\"quarter\":\"Q1\",\"fiscal_period\":1}}"));
	}
	

	
	
}

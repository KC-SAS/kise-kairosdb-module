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



package com.kise.kairosdb.util.fiscalperiod;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author lcoulet
 */
public class FiscalPeriod445CalendarTest {
    
    public FiscalPeriod445CalendarTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
    
    private static final SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");

    /**
     * Test of getFiscalYear method, of class FiscalPeriod445Calendar.
     */
    @Test
    public void testGetFiscalYearAndPeriod20140902() throws ParseException {
        
        String date="2014-09-02-12:00:00";
	long ts=dateFormat.parse(date).getTime();
        FiscalPeriod445Calendar instance = new FiscalPeriod445Calendar(ts);
        printResults(date, instance,ts);
        assertEquals(2014, instance.getFiscalYear());        
        assertEquals(9, instance.getFiscalPeriod());
        assertEquals(3, instance.getQuarter());
        
    }

   /**
     * Test of getFiscalYear method, of class FiscalPeriod445Calendar.
     */
    @Test
    public void testGetFiscalYearAndPeriod20141201() throws ParseException {
        
        String date="2014-12-01-12:00:00";
	long ts=dateFormat.parse(date).getTime();
        FiscalPeriod445Calendar instance = new FiscalPeriod445Calendar(ts);                
        printResults(date, instance,ts);
        assertEquals(2014, instance.getFiscalYear());        
        assertEquals(12, instance.getFiscalPeriod());
        assertEquals(4, instance.getQuarter());
        
    }
    
    /**
     * Test of getFiscalYear method, of class FiscalPeriod445Calendar.
     */
    @Test
    public void testGetFiscalYearAndPeriod20131201() throws ParseException {
        String date="2013-11-30-12:00:00";
	long ts=dateFormat.parse(date).getTime();
        FiscalPeriod445Calendar instance = new FiscalPeriod445Calendar(ts);
        printResults(date, instance,ts);
        assertEquals(2013, instance.getFiscalYear());        
        assertEquals(11, instance.getFiscalPeriod());
        assertEquals(4, instance.getQuarter());
        
    }
    
     /**
     * Test of getFiscalYear method, of class FiscalPeriod445Calendar.
     */
    @Test
    public void testGetFiscalYearAndPeriod20141229() throws ParseException {
        String date="2014-12-29-12:00:00";
	long ts=dateFormat.parse(date).getTime();
        FiscalPeriod445Calendar instance = new FiscalPeriod445Calendar(ts);
        printResults(date, instance,ts);
        assertEquals(2015, instance.getFiscalYear());        
        assertEquals(1, instance.getFiscalPeriod());
        assertEquals(1, instance.getQuarter());
        
    }

     /**
     * Test of getFiscalYear method, of class FiscalPeriod445Calendar.
     */
    @Test
    public void testGetFiscalYearAndPeriod20121231() throws ParseException {
        String date="2012-12-31-12:00:00";
	long ts=dateFormat.parse(date).getTime();
        FiscalPeriod445Calendar instance = new FiscalPeriod445Calendar(ts);                
        printResults(date, instance,ts);
        assertEquals(2013, instance.getFiscalYear());        
        assertEquals(1, instance.getFiscalPeriod());
        assertEquals(1, instance.getQuarter());
        
    }
    
    /**
     * Test of getFiscalYear method, of class FiscalPeriod445Calendar.
     */
    @Test
    public void testGetFiscalYearAndPeriod20150201() throws ParseException {
        String date="2015-02-01-12:00:00";
	long ts=dateFormat.parse(date).getTime();
        FiscalPeriod445Calendar instance = new FiscalPeriod445Calendar(ts);                
        printResults(date, instance,ts);
        assertEquals(2015, instance.getFiscalYear());        
        assertEquals(1, instance.getFiscalPeriod());
        assertEquals(1, instance.getQuarter());
        
    }
    
    private void printResults(String date, FiscalPeriod445Calendar instance, long timestamp) {
        System.out.println(date + "(" + timestamp + ") => FY" + instance.getFiscalYear() + " P"+instance.getFiscalPeriod() + " Q" + instance.getQuarter() + " W"+instance.getWeekOfYear() + " Month:" + instance.month);
    }

    
}

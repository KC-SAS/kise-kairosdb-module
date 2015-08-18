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

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 *
 * @author lcoulet
 */
public class FiscalPeriod445Calendar implements FiscalPeriod {

	private final int weekOfYear;
	final int month;

	private static final int[] weeks544 = {5, 4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 5};
	private static final int[] weeks454 = {4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5, 5};
	private static final int[] weeks445 = {4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 5, 5};

	/**
	 * Type of weekly-based periods fiscal calendars
	 */
	public enum CalendarType {

		CALENDAR_445(weeks445),
		CALENDAR_454(weeks454),
		CALENDAR_544(weeks544);

		private HashMap<Integer, Integer> nbWeeksPerPeriod = new HashMap<>();
		private HashMap<Integer, Integer> weekPeriodNb = new HashMap<>();

		CalendarType(int[] weeksPerPeriod) {
			for (int i = 0; i < weeksPerPeriod.length; i++) {
				nbWeeksPerPeriod.put(i + 1, weeksPerPeriod[i]);
			}
			int weekNumber = 1;
			for (int periodNumber = 1; periodNumber <= nbWeeksPerPeriod.size(); periodNumber++) {
				for (int i = 0; i < nbWeeksPerPeriod.get(periodNumber); i++) {
					weekPeriodNb.put(weekNumber, periodNumber);

					weekNumber++;
				}
			}
		}

		Map<Integer, Integer> getWeeksPerPeriod() {
			return nbWeeksPerPeriod;
		}

		;
	    
	     Map<Integer, Integer> getWeeksPeriodNumber() {
			return weekPeriodNb;
		}
	;

	}
    
	/**
	 * Creates a new FiscalCalenda rinformation for a timestamp
	 * @param calendarWeeks type of fiscal period calendar 
	 * @param timestamp the date timestamp for which fiscal period is required
	 */
    public FiscalPeriod445Calendar(CalendarType calendarWeeks, long timestamp) {
		this.WeekPeriodNb = calendarWeeks.weekPeriodNb;

		GregorianCalendar calendar = new GregorianCalendar(Locale.US);
		calendar.setTimeInMillis(timestamp);
		calendar.setFirstDayOfWeek(Calendar.MONDAY);

		weekOfYear = (calendar.get(Calendar.WEEK_OF_YEAR));
		month = calendar.get(Calendar.MONTH) + 1;

		fiscalPeriod = computeFiscalPeriod(calendar);
		fiscalYear = computeFiscalYear(calendar);
		quarter = computerQuarter(calendar);

		checkQuarterInFiscalyear();
	}

	/**
	 * Creates a new FiscalCalendar for a timestamp using a 5-4-4 model
	 *
	 * @param timestamp the date timestamp for which fiscal period is
	 * required
	 */
	public FiscalPeriod445Calendar(long timestamp) {
		this(CalendarType.CALENDAR_544, timestamp);
	}

	int fiscalYear;
	int fiscalPeriod;
	int quarter;

	private HashMap<Integer, Integer> WeekPeriodNb = new HashMap<>();

	@Override
	public int getFiscalYear() {
		return fiscalYear;
	}

	@Override
	public int getFiscalPeriod() {
		return fiscalPeriod;
	}

	@Override
	public int getQuarter() {
		return quarter;
	}

	private int computerQuarter(GregorianCalendar calendar) {
		return (int) Math.ceil(weekOfYear / 13.0);
	}

	private void checkQuarterInFiscalyear() {
		// case the quarter is greater than 4 (53 weeks year)
		if (quarter > 4) {
			quarter = 4;
		}
		// case the last days of December belong to next year
		if (weekOfYear == 1 && month >= 12) {
			fiscalYear += 1;
		}
		// case the first days of January belong to previous year
		if (weekOfYear >= 52 && month == 1) {
			fiscalYear -= 1;
		}

	}

	private int computeFiscalYear(GregorianCalendar calendar) {
		return calendar.get(Calendar.YEAR);
	}

	private int computeFiscalPeriod(GregorianCalendar calendar) {
		return WeekPeriodNb.get(weekOfYear);
	}

	@Override
	public int getWeekOfYear() {
		return weekOfYear;
	}

	@Override
	public int getID(){
		return fiscalYear*100+fiscalPeriod;
	}
}

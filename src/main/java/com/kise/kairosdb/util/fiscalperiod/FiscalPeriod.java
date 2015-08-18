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

/**
 * 
 * @author lcoulet
 */
public interface FiscalPeriod {
    
    public abstract int getFiscalYear();
    public abstract int getFiscalPeriod();
    public abstract int getQuarter();
    public abstract int getWeekOfYear();
    public abstract int getID();
    
}

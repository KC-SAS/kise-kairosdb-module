# KISE KairosDB Module

This module contains contributions to KairosDB time series database.
Particularly powerful and performant scripted aggregators are provided using Javascript language (using Java 8 Nashorn engine).

Module aggregators:

- `js_function`
- `js_filter`
- `js_range`

Module groupers:

- `fiscal_period`

This module will be updated with new aggregators and useful features.

The code is copyrighted but contributed in Open Source under Apache 2 license.  

Use this module as your own risks - We decline any liability or responsibility about this code and the usage that can be made out of it.


## Requirements

KairosDB V1.0+ running on Oracle Java 8+ runtime 


## How to start using this module

1. Add the jar in KairosDB lib directory
2. Add the properties file in KairosDB conf directory
3. Restart your KairosDB instance

Now you can send queries with scripting 


----

## Javascript aggregators

Fast scripted aggregators are provided using Javascript language, working in KairosDB streaming aggregation mode, using Java 8+ Nashorn JS scripting engine.

Nashorn JS engine is run without access to the Java API (--no-java), so it is relatively safe. 
But please keep in mind that this allows end useres to execute arbitrary Javascript code on the KairosDB server, if this can cause problems (e.g. infinite loops).  
For example, as they are provided now, those aggregators are not protected against bad use by the end user (e.g. infinite loops).


### js_function Aggregator

Applies an expression or a JS function to the values.

Aggregator parameters:

- `script`: a String containing the javascript to execute


Available parameters in the JS code:

- `value`: provides the value of the point
- `timestamp`: provides the timestamp (Unix milliseconds) of the point


A formula (e.g. multiply the value by 3), in JS the expression would be `value * 3.0` - simply add this aggregator in the KairosDB query:
	
	{
	  "name": "js_function",
	  "script": "value * 3.0"
	}
	

A function (e.g. multiply the value by 3):
	
	var periodic_element = Math.sin(timestamp)*2.0
	var calibrated_value = Math.log(value * 10)
	return periodic_element+calibrated_value

Function being included within the JSON query, simply add this aggregator in the KairosDB query :

	{
      name: "js_function",
      script: "var periodic_element = Math.sin(timestamp)*2.0\nvar calibrated_value = Math.log(value * 10)\nreturn periodic_element-calibrated_value"
    }



### js_filter Aggregator

Same mechanisms, except that the function or expression provided is a predicate returning a boolean. 

Aggregator parameters:

- `script`: a String containing the javascript to execute

Available parameters in the JS code:

- `value`: provides the value of the point
- `timestamp`: provides the timestamp (Unix milliseconds) of the point


Example - only return values greater than 5, in JS the expression would be `value > 5`:
 	{
      "name": "js_filter",
      "script": "value > 5"
    }
    


### js_range Aggregator

This aggregator is provided by default an iterator of DataPoint, but alternatively a buffer with all the values can be allocated (e.g. to perform several computations).

Note: The maximum size of this buffer is defined in the module configuration properties file as `kise.kairosdb.script_aggregator.max_batch`. 
If the number of data points for a time range passed to this aggregator in allocated arrayt mode exceeds this capacity an exception is raised and the query fails. 

Aggregator parameters:

- `script`: a String containing the javascript to execute
- (optional) `allocate_array`: true/false - if set to true an array is given to function rather than an iterator  
- Other parameters are same as KairosDB usual Range aggregators
	- `sampling`: the KairosDB sampling interval
	- (optional) `align_sampling`: true/false  
 	- (optional) `align_start_time`: true/false
 	
   


Available parameters in the JS code:

- `values`: if the mode is iterator - an iterator of DataPoint
- `valueArray`: if the mode is an array - an array of DataPoint

Example Javascript code for computing the sum (I know it's not interesting but it's for the example):

	var result=0;
	while( values.hasNext()){
	result += values.next().getLongValue();
	}
	return result;

Example JSON of the aggregator to put in a query:
		
	{
      "name": "js_range",
      "align_sampling": true,
      "align_start_time": true,
      "sampling": {
        "value": "1",
        "unit": "hours"
      },
      "script": "var result=0;\nwhile( values.hasNext()){\nresult += values.next().getLongValue();\n}\nreturn result;\n\n"
    },


----


## Group-By features

### Group-by fiscal_period 

This grouper uses fiscal period corresponding to [4-4-5 calendar](https://en.wikipedia.org/wiki/4%E2%80%934%E2%80%935_calendar) (more kinds of calendars could be added in the future).

It can be parametrized with the type of weeks-per-period.

Group-by parameters:

- `calendar_type` the type of calendar:
  - calendar_445
  - calendar_454
  - calendar_544 (by default if none specified) 
 
Group-by information:

- `name` : full period name e.g. "FY2015-P8"
-  `fiscal_year`: the fiscal year e.g. 2015
-  `quarter`: the quarter e.g. "Q3"
-  `fiscal_period`: the fiscal period number e.g. 1

Example usage:

	"group_by": [
        {
          "name": "fiscal_period",
          "calendar_type": "calendar_454"
        }
      ]


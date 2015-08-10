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

import org.kairosdb.core.aggregator.Aggregator;

/**
 * Java script aggregators allow to build new aggregators without modifying the server.
 * It is a bit less efficient than native aggregators but more flexible.
 * The design of the classes has suffered from difficulties to properly implement the
 * constraint validation mechanism. It may probably still be improved.
 * The verification of the presence of a return statement might still be improved (ScriptBuilder)
 * Input validation might also be improved.
 * @author Remi Dettai <rdettai at gmail.com>
 */
public interface ScriptingAggregator extends Aggregator{
	static final String ERROR_IN_SCRIPT = "Error during script execution, please check the syntax";
	/**
	 * This constant has nothing to do with the beloved Indian bread
	 */
	static final String NAN_RESULT = "Script result is not a number";
	
	
	public void setScript(String script) throws NoSuchFieldException;
}

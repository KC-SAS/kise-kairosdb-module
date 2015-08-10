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
package com.kise.kairosdb.util;

import java.util.regex.Pattern;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;

/**
 *
 * @author Remi Dettai <rdettai at gmail.com>
 */
public class ScriptBuilder {
	public static final String ERROR_IN_SCRIPT_PARSING = "Error during script parsing, please check the syntax";
	private static final Pattern PATTERN = Pattern.compile("^((.*)([^a-zA-Z\\\\d]))?(return)([ (])(.*)",Pattern.DOTALL);
	/**
	 * Bundles the script into a function f and builds it into an invocable object.
	 * @param script
	 * @param arguments
	 * @return null if the script could not be built
	 */
	public static Invocable buildScript(String script,String... arguments){
		ScriptEngine engine;
		try{
			engine = new NashornScriptEngineFactory().getScriptEngine(new String[] { "--no-java" });
			engine.eval(bundleAsFonctionF(script,arguments));
		} catch(ScriptException e){
			return null;
		}
		
		return (Invocable) engine;
		
	}
	
	public static String getScriptError(String script,String... arguments){
		try{
			ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine(new String[] { "--no-java" });
			engine.eval(bundleAsFonctionF(script,arguments));
		} catch(ScriptException e){
			return e.getMessage();
		}
		
		return null;
		
	}
	
	private static String bundleAsFonctionF(String script,String... arguments){
		String argString = "";
		for(int i=0;i<arguments.length-1; i++)
			argString+=(arguments[i]+",");
		if(arguments.length>0)
			argString+=arguments[arguments.length-1];
		
		String function;
		if(PATTERN.matcher(script).matches())
			function = "function f("+argString+"){ "+script+" }";
		else
			function = "function f("+argString+"){ return "+script+" }";
		return function;
	}
	
}

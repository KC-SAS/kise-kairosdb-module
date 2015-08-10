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
package com.kise.kairosdb.core.http.rest.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import com.kise.kairosdb.util.ScriptBuilder;

/**
 *
 * @author Remi Dettai <rdettai at gmail.com>
 */
public class ValidScriptValidator implements ConstraintValidator<ValidScript, String>
{
	private String[] arguments;
	@Override
	public void initialize(ValidScript validScript)
	{
		this.arguments = validScript.arguments();
	}
	
	@Override
	public boolean isValid(String aString, ConstraintValidatorContext context)
	{
		String scriptError = ScriptBuilder.getScriptError(aString,arguments);
		if (scriptError!=null)
		{
			scriptError = scriptError.replace("\n", "   ");
			scriptError = scriptError.replace("\r", "   ");
			context.disableDefaultConstraintViolation(); // disable violation message
			context.buildConstraintViolationWithTemplate("has an invalid syntax"+scriptError).addConstraintViolation();  // add message
			return false;
		}
		
		return true;
	}
}
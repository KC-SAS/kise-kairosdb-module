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

package com.kise.kairosdb.core;

import ch.qos.logback.classic.Logger;
import com.google.inject.name.Names;
import com.kise.kairosdb.core.aggregator.JsFilterAggregator;
import com.kise.kairosdb.core.aggregator.JsFunctionAggregator;
import com.kise.kairosdb.core.aggregator.JsRangeAggregator;
import java.util.Properties;
import org.kairosdb.core.CoreModule;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Remi Dettai
 */
public class KiseKairosDBModule extends CoreModule{
	//private Properties m_props;
	public static final Logger logger = (Logger) LoggerFactory.getLogger(KiseKairosDBModule.class);
	private Properties m_props;
	
	public KiseKairosDBModule(Properties props)
	{
		super(props);
		m_props = props;
	}
	
	@Override
	protected void configure() {
		bind(JsFilterAggregator.class);
		bind(JsFunctionAggregator.class);
		bind(JsRangeAggregator.class);
		
		if(m_props.getProperty("kise.kairosdb.script_aggregator.max_batch")!=null){
			Integer maxBatch = Integer.parseInt(m_props.getProperty("kise.kairosdb.script_aggregator.max_batch"));
			bindConstant().annotatedWith(Names.named("MAXIMUM_SCRIPT_BATCH")).to(maxBatch);
			logger.info("Max batch for script aggregator: "+maxBatch);
		}
		else {
			bindConstant().annotatedWith(Names.named("MAXIMUM_SCRIPT_BATCH")).to(1000);
			logger.info("Max batch for script aggregator: 1000 (default)");
		}
		
		
		
	}
}

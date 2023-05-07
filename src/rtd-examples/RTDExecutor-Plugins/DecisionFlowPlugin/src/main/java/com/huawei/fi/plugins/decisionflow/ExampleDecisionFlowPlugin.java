package com.huawei.fi.plugins.decisionflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

public class ExampleDecisionFlowPlugin extends Plugin
{
    private static final Logger LOG = LoggerFactory.getLogger(ExampleDecisionFlowPlugin.class);

    public ExampleDecisionFlowPlugin(PluginWrapper wrapper)
    {
        super(wrapper);
    }
    
    @Override
    public void start() { LOG.info("ExampleDecisionFlowPlugin.start()"); }
    
    @Override
    public void stop()
    {
        LOG.info("ExampleDecisionFlowPlugin.stop()");
    }
    
}

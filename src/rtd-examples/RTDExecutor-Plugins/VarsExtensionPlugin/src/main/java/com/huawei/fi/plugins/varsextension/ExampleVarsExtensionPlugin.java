package com.huawei.fi.plugins.varsextension;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

public class ExampleVarsExtensionPlugin extends Plugin
{
    private static final Logger LOG = LoggerFactory.getLogger(ExampleVarsExtensionPlugin.class);

    public ExampleVarsExtensionPlugin(PluginWrapper wrapper)
    {
        super(wrapper);
    }
    
    @Override
    public void start() { LOG.info("ExampleVarsExtensionPlugin.start()"); }
    
    @Override
    public void stop()
    {
        LOG.info("ExampleVarsExtensionPlugin.stop()");
    }
    
}

package com.huawei.fi.plugins.decisionflow;

import com.huawei.fi.rtdexecutor.RTDEnvironment;
import com.huawei.fi.rtdexecutor.common.ConfigService;
import com.huawei.fi.rtdexecutor.common.Constants;
import com.huawei.fi.rtdexecutor.config.EventSourceConfig;
import com.huawei.fi.rtdexecutor.plugin.DecisionFlowExtension;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.CallFilterRulesEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.CallProcRulesEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.ComputeVarsEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.DataPrepEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.DroolsScoreEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.DynamicVarsEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.EventInboundEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.EventOutboundEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.InferenceEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.InsertDataEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.JDBCCallFilterRulesEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.JDBCCallProcRulesEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.JDBCComputeVarsEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.JDBCInsertDataEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.ScoreModelEventHandler;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.WindowVarEventHandler;
import com.huawei.fi.rtdexecutor.rtdpipeline.RTDPipeline;

import org.pf4j.Extension;
import org.pf4j.RuntimeMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * on 2017/3/29.
 */
@Extension
public class ExampleDecisionFlowExtension extends DecisionFlowExtension
{
    private static final Logger LOG = LoggerFactory.getLogger(ExampleDecisionFlowExtension.class);

    @Override
    public int getPipelineDecisionServiceMode() {
        EventSourceConfig config = ConfigService.instance.getEventSourceConfig();
        if ((config.getDataSourceType() & Constants.DATA_SOURCE_TYPE_KAFKA) == Constants.DATA_SOURCE_TYPE_KAFKA) {
            return Constants.DECISION_SERVICE_MODE_NRT;
        } else {
            return Constants.DECISION_SERVICE_MODE_RT;
        }
    }

    @Override
    public void orchestrateDecisionFlow(RTDPipeline pipeline)
    {
        LOG.info("RuntimeMode: '" + RTDEnvironment.getProperty("plugin.RuntimeMode") + "'.");
        // for testing the development mode
        if (RuntimeMode.DEVELOPMENT.toString().equalsIgnoreCase(RTDEnvironment.getProperty("plugin.RuntimeMode")))
        {
            pipeline.setInboundEventHandler(new EventInboundEventHandler());
            pipeline.setOutboundEventHandler(new EventOutboundEventHandler());

            pipeline.addEventHandler(new DataPrepEventHandler());
        }
        else
        {
            pipeline.setInboundEventHandler(new EventInboundEventHandler());
            pipeline.setOutboundEventHandler(new EventOutboundEventHandler());
            // 使用说明：
            // 1、若使用自定义DataPreEventHandler来进行事件变量扩展，请注意导入DataPrepEventHandler类的包路径；
            //    注：该方式不支持在线动态更新。
            // 2、若使用VarsExtensionPlugin来进行事件变量扩展，
            // 3、DataPrepEventHandler需放在整个pipeline的最前面
            //    DataPrepEventHandler为华为jar内置类，客户逻辑在VarsExtensionPlugin插件的Extension里；
            //    注：该方式支持在线动态更新。
            pipeline.addEventHandler(new DataPrepEventHandler());

            if (Constants.DB_TYPE_MOT == ConfigService.getInstance().getTenantRtddbConfig().getDbType()) {
                pipeline.addEventHandler(new JDBCInsertDataEventHandler());
                pipeline.addEventHandler(new JDBCCallFilterRulesEventHandler());
                pipeline.addEventHandler(new JDBCComputeVarsEventHandler());
            } else {
                pipeline.addEventHandler(new InsertDataEventHandler());
                pipeline.addEventHandler(new CallFilterRulesEventHandler());
                pipeline.addEventHandler(new ComputeVarsEventHandler());
            }

            pipeline.addEventHandler(new WindowVarEventHandler());
            // 1、若使用VarsExtensionPlugin实现动态扩展变量，需要添加DynamicVarsEventHandler到pipeline里；
            // 2、若无动态扩展变量，则需要将该Handler从pipeline中删除；
            // 3、该DynamicVarsEventHandler的位置必须放在ComputeVarsEventHandler后。
            pipeline.addEventHandler(new DynamicVarsEventHandler());
            // add model compute
            pipeline.addEventHandler(new InferenceEventHandler());
            pipeline.addEventHandler(new ScoreModelEventHandler());
            if (Constants.DB_TYPE_MOT == ConfigService.getInstance().getTenantRtddbConfig().getDbType()) {
                pipeline.addEventHandler(new JDBCCallProcRulesEventHandler());
            } else {
                pipeline.addEventHandler(new CallProcRulesEventHandler());
            }
            // add drools
            pipeline.addEventHandler(new DroolsScoreEventHandler());
        }
    }
}

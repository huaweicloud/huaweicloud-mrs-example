package com.huawei.fi.plugins.decisionflow;

import com.huawei.fi.rtdexecutor.common.*;
import com.huawei.fi.rtdexecutor.config.EventSourceConfig;
import com.huawei.fi.rtdexecutor.pipeline.AbstractPipeline;
import com.huawei.fi.rtdexecutor.pipeline.EventHandlerException;
import com.huawei.fi.rtdexecutor.pipeline.PipelineContext;
import com.huawei.fi.rtdexecutor.rtdeventhandlers.RTDEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

public class ExampleScoreEventHandler extends RTDEventHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(ExampleScoreEventHandler.class);

    //用户自定义报文字段
    private static final String CUSTOM_USER_DATA_KEY = "custom_user_data_key";
    
    /**
     * 百分减模型参数初始化赋值
     */
    private int maxTotalScore = Integer.MAX_VALUE;
    
    private int minTotalScore = Integer.MIN_VALUE;
    
    private int baseTotalScore = 0;
    
    private long configVersion = -1;
    
    /**
     * 评分模型的数组表示, 0:min（区间最小值）, 1:max（区间最小值）, 2:scoreResult（区间结果）
     */
    private ArrayList<int[]> scoreModel = new ArrayList<int[]>();
    
    /**
     * 默认构造函数（禁止修改）
     */
    public ExampleScoreEventHandler()
    {
        super();
        reloadConfig();
    }
    
    /**
     * 支持异步链模式调用方法（禁止修改）
     * @param 
     * @return
     */
    @Override
    public void beginAsyncHandle(PipelineContext context) throws EventHandlerException
    {
        handle(context);
        
        endAsyncHandle(context);
    }
    
    /**
     * 同步模式调用方法（可修改此方法内计分逻辑）
     * @param 
     * @return
     */
    @Override
    public void handle(PipelineContext context) throws EventHandlerException
    {
        LOG.debug("ExampleScoreEventHandler begin.");
        reloadConfig();
        
        EventData eventData = (EventData)context.getUserData().getOrDefault(Constants.PC_KEY_EVENT_DATA, null);
        long score = baseTotalScore;
        
        //黑白名单过滤规则命中
        if ((eventData.getStatusCode()
            & EventDataStatusCode.OK_FILTER_RULE_HIT) == EventDataStatusCode.OK_FILTER_RULE_HIT)
        {
            Map<String, Object> filterRulesResults = eventData.getFilterRules();
            score = computeScoreOnlyMax(score, filterRulesResults);
        }
        
        //存储过程规则命中
        if ((eventData.getStatusCode() & EventDataStatusCode.OK_PROC_RULE_HIT) == EventDataStatusCode.OK_PROC_RULE_HIT)
        {
            Map<String, Object> procRulesResults = eventData.getProcRules();
            score = computeScoreOnlyMax(score, procRulesResults);
        }
        
        int scoreResult = -1;
        
        //给定一个分值，计算该分值所属的区间结果
        for (int[] entry : scoreModel)
        {
            if (score >= entry[0] && score <= entry[1])
            {
                scoreResult = entry[2];
                break;
            }
        }
        
        eventData.getRtdResults().put(RTDecisionResult.KEY_SCORE, score);
        eventData.getRtdResults().put(RTDecisionResult.KEY_SCORE_RESULT, scoreResult);
        
        //支持用户在UserData里添加自定义复杂报文，并作为Json输出（注意：添加的自定义字段不要与系统原有字段冲突）
        customizeUserMessage(eventData);
        
        LOG.debug("ExampleScoreEventHandler end.");
    }
    
    /**
     * 样例：支持用户在UserData里添加自定义复杂报文，并作为Json输出
     * 可仿照此方法对输出报文进行相关操作（注意：添加的自定义字段不要与系统原有字段冲突）
     * @param 
     * @return
     */
    private void customizeUserMessage(EventData eventData)
    {
        eventData.getUserdata().put(CUSTOM_USER_DATA_KEY, "CustomUserDataValue");
    }
    
    /**
     * 支持以reload方式更新配置文件中的分数模型（禁止修改）
     * @param 
     * @return
     */
    private void reloadConfig()
    {
        if (! ConfigService.instance.isUpdated(configVersion)) return;

        configVersion = ConfigService.instance.getVersion();
        EventSourceConfig config = ConfigService.instance.getEventSourceConfig();

        if (config == null) return;

        maxTotalScore = config.getMaxTotalScore();
        minTotalScore = config.getMinTotalScore();
        baseTotalScore = config.getBaseTotalScore();

        scoreModel.clear();
        scoreModel = config.generateScoreModelArray();
    }
    
    /**
     * 计分样例：只扣除最大规则分数的计分方式
     * @param 
     * @return
     */
    private long computeScoreOnlyMax(long score, Map<String, Object> rulesResults)
    {
        int maxScore = 0;
        for (Map.Entry<String, Object> entry : rulesResults.entrySet())
        {
            if (entry.getValue() != null)
            {
                int ruleScore = (int)entry.getValue();
                if (ruleScore > maxScore)
                {
                    maxScore = ruleScore;
                }
            }
        }
        
        score = score - maxScore;
        if (score < minTotalScore)
        {
            score = minTotalScore;
        }
        else if (score > maxTotalScore)
        {
            score = maxTotalScore;
        }
        
        return score;
    }
    
    /**
     * 该初始化在blu启动时触发，插件handle自定义资源根据需求可以放在该处。
     * @param 
     * @return
     */
    @Override
    public void init(AbstractPipeline pipeline)
    {
        super.init(pipeline);
        LOG.info("ExampleScoreEventHandler init.");
    }
    
    /**
     * blu反部署时触发， 要求资源释放必须与资源创建匹配
     * @param 
     * @return
     */
    @Override
    public void destroy()
    {
        super.destroy();
        LOG.info("ExampleScoreEventHandler destroy.");
    }
    
}

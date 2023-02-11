package com.huawei.fi.plugins.varsextension;

import com.huawei.fi.rtdexecutor.common.DynamicEventVar;
import com.huawei.fi.rtdexecutor.common.EventData;
import com.huawei.fi.rtdexecutor.pipeline.EventHandlerException;
import com.huawei.fi.rtdexecutor.pipeline.PipelineContext;
import com.huawei.fi.rtdexecutor.plugin.DynamicVarsExtension;
import org.apache.commons.lang3.StringUtils;
import org.pf4j.Extension;

import java.util.*;

/**
 * on 2017/3/30.
 */
@Extension
@SuppressWarnings({"rawtypes"})
public class ExampleDynamicVarsExtension extends DynamicVarsExtension
{
    /**
     * init your public resource at here,
     * plugins create or update, this method will be invoked
     *
     */
    @Override
    public void init()
    {
        LOG.info("{} init start ...", this.getClass().getName());

        super.init();
        // add your init code at here
        this.timeoutMS = 20L;

        LOG.info("{} init end ...", this.getClass().getName());
    }

    /**
     * recycle your resource at here,
     * plugins destroy or update, this method will be invoked
     *
     */
    @Override
    public void destroy()
    {
        LOG.info("{} destroy start ...", this.getClass().getName());

        super.destroy();
        // add your destroy code at here

        LOG.info("{} destroy end ...", this.getClass().getName());
    }

    @Override
    public List<String> addRequiredInputVars()
    {
        // define input vars which required in your code and built-in RTD platform,
        // include event vars, batch vars, runtime query vars
        List<String> inputVars = new ArrayList<>();
//        inputVars.add("ev_client_ip");
//        inputVars.add("rv_online_pay");
//        inputVars.add("bv_history_pay");

        return inputVars;
    }

    @Override
    public Map<String, DynamicEventVar<?>> defineDynamicEventVars()
    {
        DynamicEventVar<String> localCity = new DynamicEventVar.String("ev_d_local_city", "Boston");
        DynamicEventVar<Long> totalPay = new DynamicEventVar.Long("ev_d_total_pay", 11L);

        Map<String, DynamicEventVar<?>> defineDynamicEventVarMap = new HashMap<>(500);
        defineDynamicEventVarMap.put(localCity.getName(), localCity);
        defineDynamicEventVarMap.put(totalPay.getName(), totalPay);

        return defineDynamicEventVarMap;
    }

    @Override
    public void compute(PipelineContext ctx, EventData eventData) throws EventHandlerException
    {
        // compute dynamicEventVar by platform base vars
        DynamicEventVar<Long> totalPay = computeTotalPay(ctx, eventData, getDefineDynamicEventVars().get("ev_d_total_pay"));
        putComputeResult(ctx, totalPay.getName(), totalPay);

        DynamicEventVar<String> localCity = computeLocalCity(ctx, eventData, getDefineDynamicEventVars().get("ev_d_local_city"));
        putComputeResult(ctx, localCity.getName(), localCity);
    }
    
    private DynamicEventVar<Long> computeTotalPay(PipelineContext ctx, EventData eventData, DynamicEventVar defined) throws EventHandlerException
    {
        // get batch vars from eventData to compute dynamicEventVar
        Long historyPay = (Long) eventData.getBatchVars().get("bv_history_pay");
        historyPay = Objects.isNull(historyPay) ? 0L : historyPay;

        // get real time vars from eventData to compute dynamicEventVar
        Long onlinePay = (Long) eventData.getRtqVars().get("rv_online_pay");
        onlinePay = Objects.isNull(onlinePay) ? 0L : onlinePay;

        Long totalPay = historyPay + onlinePay;

        DynamicEventVar<Long> totalPayVar = new DynamicEventVar.Long(defined.getName(), (Long)defined.getValue());
        if(totalPay > 10L) {
            totalPayVar.setValue(totalPay);
        } else {
            totalPayVar.setException("required input vars maybe not exists or offline.");
        }

        checkTimeoutMS(ctx, timeoutMS, false);
        return totalPayVar;
    }

    private DynamicEventVar<String> computeLocalCity(PipelineContext ctx, EventData eventData, DynamicEventVar defined) throws EventHandlerException
    {
        // get event vars from eventData to compute dynamicEventVar
        String clientIP = (String)eventData.getEventVars().get("ev_client_ip");

        DynamicEventVar<String> localCityVar = new DynamicEventVar.String(defined.getName(), (String)defined.getValue());
        if(StringUtils.isNotEmpty(clientIP)) {
            localCityVar.setValue("New York");
        } else {
            localCityVar.setException("required input vars maybe not exists or offline.");
        }

        checkTimeoutMS(ctx, timeoutMS, false);
        return localCityVar;
    }

}

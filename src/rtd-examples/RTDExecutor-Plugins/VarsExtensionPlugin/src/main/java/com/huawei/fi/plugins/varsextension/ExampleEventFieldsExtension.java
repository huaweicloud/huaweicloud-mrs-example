package com.huawei.fi.plugins.varsextension;

import com.huawei.fi.rtdexecutor.common.EventData;
import com.huawei.fi.rtdexecutor.common.ExtendEventField;
import com.huawei.fi.rtdexecutor.pipeline.EventHandlerException;
import com.huawei.fi.rtdexecutor.pipeline.PipelineContext;
import com.huawei.fi.rtdexecutor.plugin.EventFieldsExtension;
import org.pf4j.Extension;

/**
 *  on 2017/3/31.
 */
@Extension
public class ExampleEventFieldsExtension extends EventFieldsExtension
{
    @Override
    public void init()
    {
        LOG.info("{} init start ...", this.getClass().getName());

        super.init();
        // add your init code at here
        this.timeoutMS = 20L;

        LOG.info("{} init end ...", this.getClass().getName());
    }

    @Override
    public void destroy()
    {
        LOG.info("{} destroy start ...", this.getClass().getName());

        super.destroy();
        // add your destroy code at here

        LOG.info("{} destroy end ...", this.getClass().getName());
    }

    @Override
    public void compute(PipelineContext ctx, EventData eventData) throws EventHandlerException
    {
        ExtendEventField<String> bankCustomerID = new ExtendEventField.String("BANK_CUSTOMER_ID");
        ExtendEventField<Integer> bankCustomerAge = new ExtendEventField.Integer("BANK_CUSTOMER_AGE");
        ExtendEventField<Double> bankCustomerSalary = new ExtendEventField.Double("BANK_CUSTOMER_SALARY");

        bankCustomerAge.setValue(32);
        bankCustomerID.setValue("0000001");

        String city = (String) eventData.getData().get("LBS_CITY");
        if("Boston".equals(city)) {
            bankCustomerSalary.setValue(7830.25);
        } else if("New York".equals(city)) {
            bankCustomerSalary.setValue(9765.40);
        } else {
            bankCustomerSalary.setValue(6300.62);
        }

        putComputeResult(ctx, bankCustomerID.getName(), bankCustomerID);
        putComputeResult(ctx, bankCustomerAge.getName(), bankCustomerAge);
        putComputeResult(ctx, bankCustomerSalary.getName(), bankCustomerSalary);
    }
}

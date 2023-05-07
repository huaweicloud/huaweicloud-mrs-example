package com.huawei.fi.rtd.drools.generator.builder;

import com.huawei.fi.rtd.drools.generator.RtdDroolsDecision;
import com.huawei.fi.rtd.drools.generator.RtdDroolsElement;
import com.huawei.fi.rtd.drools.generator.RtdDroolsElementsGroup;
import com.huawei.fi.rtd.drools.generator.exception.DroolsGeneratorException;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class RtdRuntimeMetaFileBuilder {
    /**
     * 解析RtdDroolsDecision对象中的元数据，生成properties，在blu侧使用
     *
     * @param droolsDecision droolsDecision
     * @return 生成的properties
     */
    public String buildMetaFile(RtdDroolsDecision droolsDecision) {
        String decisionName = droolsDecision.getDecisionName();
        if (StringUtils.isBlank(decisionName)) {
            throw new DroolsGeneratorException("Decision name can not null");
        }
        List<RtdDroolsElementsGroup> elementGroupSet = droolsDecision.getElementsGroups();
        if (elementGroupSet == null || elementGroupSet.size() == 0) {
            throw new DroolsGeneratorException("Element group can not null");
        }
        Map<String, Set<Map<String, Object>>> metaMap = new TreeMap<>();
        elementGroupSet.stream()
                .filter(group -> group.getElements() != null && group.getElements().size() > 0 && StringUtils.isNotBlank(group.getGroupName()))
                .forEach(group -> metaMap.put(group.getGroupName(),
                                group.getElements().stream().map(element -> convertElement2Map(element)).collect(Collectors.toSet())
                        )
                );
        return JSONObject.toJSONString(metaMap, SerializerFeature.PrettyFormat);
    }

    /**
     * 将element对象，输出为map
     */
    private Map<String, Object> convertElement2Map(RtdDroolsElement element) {
        Map<String, Object> elementMap = new HashMap();
        elementMap.put("name", element.getName());
        elementMap.put("weight", element.getWeight());
        elementMap.put("missingValue", element.getMissingValue());

        return elementMap;
    }
}

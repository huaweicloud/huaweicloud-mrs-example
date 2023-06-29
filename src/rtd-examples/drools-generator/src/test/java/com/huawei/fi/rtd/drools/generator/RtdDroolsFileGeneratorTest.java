package com.huawei.fi.rtd.drools.generator;

import com.alibaba.fastjson.JSONObject;
import com.huawei.fi.rtd.drools.generator.common.AlgorithmType;
import com.huawei.fi.rtd.drools.generator.common.ZipFileUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.kie.api.runtime.KieContainer;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class RtdDroolsFileGeneratorTest {

    private static final String RTD_DROOLS_K_MODULE_XML = "kmodule.xml";

    private static final String RTD_META_DATA_PROPERTIES = "rtd_meta_data.properties";

    private static final String RTD_DROOLS_DECISION_JSON = "drools_contract.json";

    private static final String RTD_DROOLS_FILE_PREFIX = "rtd_drools_decision_";

    private static final int BUFFER = 10 * 1024;

    private KieContainer kieContainer;

    private RtdDroolsDecision droolsDecision;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() {

        droolsDecision = new RtdDroolsDecision(AlgorithmType.AVERAGE.name());

        RtdDroolsElementsGroup weightGroupOne = new RtdDroolsElementsGroup("drools_group_01", AlgorithmType.WEIGHTING.name());
        weightGroupOne.setSalience(99);
        weightGroupOne.addElement(new RtdDroolsElement("pr_rule_01", 0.5));
        weightGroupOne.addElement(new RtdDroolsElement("pr_rule_02", 0.3));

        RtdDroolsElementsGroup weightGroupTwo = new RtdDroolsElementsGroup("drools_group_02", AlgorithmType.WEIGHTING.name());
        weightGroupTwo.setSalience(99);
        weightGroupTwo.addElement(new RtdDroolsElement("pr_rule_02"));
        weightGroupTwo.addElement(new RtdDroolsElement("pr_rule_03"));

        RtdDroolsElementsGroup averageGroup = new RtdDroolsElementsGroup("drools_group_03", AlgorithmType.AVERAGE.name());
        averageGroup.setSalience(99);
        averageGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        averageGroup.addElement(new RtdDroolsElement("pr_rule_03"));
        averageGroup.addElement(new RtdDroolsElement("pr_rule_04"));

        RtdDroolsElementsGroup maximumGroup = new RtdDroolsElementsGroup("drools_group_04", AlgorithmType.MAXIMUM.name());
        maximumGroup.setSalience(99);
        maximumGroup.addElement(new RtdDroolsElement("pr_rule_01"));
        maximumGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        maximumGroup.addElement(new RtdDroolsElement("pr_rule_03"));

        RtdDroolsElementsGroup minimumGroup = new RtdDroolsElementsGroup("drools_group_05", AlgorithmType.MINIMUM.name());
        minimumGroup.setSalience(99);
        minimumGroup.addElement(new RtdDroolsElement("pr_rule_01"));
        minimumGroup.addElement(new RtdDroolsElement("pr_rule_02"));
        minimumGroup.addElement(new RtdDroolsElement("pr_rule_03"));

        droolsDecision.addElementsGroup(weightGroupOne);
        droolsDecision.addElementsGroup(weightGroupTwo);
        droolsDecision.addElementsGroup(averageGroup);
        droolsDecision.addElementsGroup(maximumGroup);
        droolsDecision.addElementsGroup(minimumGroup);
    }

    @After
    public void tearDown() {

        if (kieContainer != null) {
            kieContainer.dispose();
        }

        kieContainer = null;
    }

    @Test
    public void input_invalid_contract_json_and_out_put_zip_file_then_compare_content_success() {

        RtdDroolsFileGenerator generator = RtdDroolsFileGenerator.INSTANCE;
        String jsonContract = JSONObject.toJSONString(droolsDecision);

        File result = null;
        try {
            File tempDirectory = new File("d:/");
            result = generator.outputZipFile(jsonContract, tempDirectory.getPath(), "rtd-drools-example.zip");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Create zip file fail ....");
        }

        ZipInputStream zipIn = null;
        try {
            zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(result)));
            compareZipFileContentOneByOne(zipIn);
        } catch (Exception e) {
            Assert.fail("Unzip file fail ....");
        } finally {
            ZipFileUtil.closeIOResource(zipIn);
        }

        boolean deleteSuccess = result.delete();
        Assert.assertTrue(deleteSuccess);
    }

    @Test
    public void input_invalid_contract_json_and_out_put_zip_stream_then_compare_content_success() {

        RtdDroolsFileGenerator generator = RtdDroolsFileGenerator.INSTANCE;
        String jsonContract = JSONObject.toJSONString(droolsDecision);

        ByteArrayOutputStream outputBaos = generator.outputZipStream(jsonContract);
        ByteArrayInputStream bais = new ByteArrayInputStream(outputBaos.toByteArray());
        ZipInputStream zipIn = null;
        try {
            zipIn = new ZipInputStream(new BufferedInputStream(bais));
            compareZipFileContentOneByOne(zipIn);
        } catch (Exception e) {
            Assert.fail("Unzip file fail ....");
        } finally {
            ZipFileUtil.closeIOResource(zipIn);
        }
    }

    private void compareZipFileContentOneByOne(ZipInputStream zipIn) throws IOException {

        for (ZipEntry zipEntry; (zipEntry = zipIn.getNextEntry()) != null; ) {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte data[] = new byte[BUFFER];
            for (int count; (count = zipIn.read(data, 0, BUFFER)) != -1; ) {
                baos.write(data, 0, count);
            }

            String fileContent = new String(baos.toByteArray());
            ZipFileUtil.closeIOResource(baos);

            if (zipEntry.getName().equals(RTD_DROOLS_K_MODULE_XML)) {
                Assert.assertTrue(fileContent.contains("rtdDecisionStateless"));
            } else if (zipEntry.getName().equals(RTD_META_DATA_PROPERTIES)) {
                Assert.assertTrue(fileContent.contains("drools_group_01"));
            } else if (zipEntry.getName().startsWith(RTD_DROOLS_FILE_PREFIX)) {
                Assert.assertTrue(fileContent.contains("package com.huawei.fi.rtd.drools.common"));
            } else if (zipEntry.getName().equals(RTD_DROOLS_DECISION_JSON)) {
                Assert.assertTrue(StringUtils.isNotBlank(fileContent));
            } else {
                Assert.fail("Invalid file name ....");
            }
        }
    }

}

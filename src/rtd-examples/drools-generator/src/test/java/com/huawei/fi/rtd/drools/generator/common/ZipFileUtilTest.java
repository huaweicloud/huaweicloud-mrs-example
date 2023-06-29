package com.huawei.fi.rtd.drools.generator.common;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipFileUtilTest {

    private static final Logger logger = LoggerFactory.getLogger(ZipFileUtilTest.class);

    private static final int BUFFER = 10 * 1024;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Map<String, String> fileContentMap = new HashMap<>();

    @Before
    public void setUp() {
        fileContentMap.put("module.xml", "some xml content");
        fileContentMap.put("drools.drl", "a lot of rules");
    }

    @Test
    public void input_valid_byte_array_output_stream_and_close_then_success() {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Assert.assertTrue(ZipFileUtil.closeIOResource(baos));
    }

    @Test
    public void input_valid_param_and_create_zip_file_then_unzip_to_compare_equals() {

        File tempZipFile = null;
        try {
            tempZipFile = temporaryFolder.newFile("output.zip");
            ZipFileUtil.outputZipFile(tempZipFile, fileContentMap);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Create zip file fail ....");
        }

        ZipInputStream zipIn = null;
        try {
            zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(tempZipFile)));
            compareZipFileContentOneByOne(zipIn);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unzip file fail ....");
        } finally {
            ZipFileUtil.closeIOResource(zipIn);
        }
    }

    @Test
    public void input_valid_param_and_create_zip_stream_then_unzip_to_compare_equals() {

        ByteArrayOutputStream outputBaos = new ByteArrayOutputStream();
        ZipFileUtil.outputZipStream(outputBaos, fileContentMap);

        ByteArrayInputStream bais = new ByteArrayInputStream(outputBaos.toByteArray());
        ZipInputStream zipIn = null;
        try {
            zipIn = new ZipInputStream(new BufferedInputStream(bais));
            compareZipFileContentOneByOne(zipIn);
        } catch (Exception e) {
            e.printStackTrace();
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
            logger.info(zipEntry.getName() + ": "+ fileContent);

            Assert.assertTrue(fileContentMap.get(zipEntry.getName()).equals(fileContent));
        }
    }

}

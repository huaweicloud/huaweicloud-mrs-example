package com.huawei.fi.rtd.drools.generator.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileUtil {

    private static final Logger logger = LoggerFactory.getLogger(ZipFileUtil.class);

    private static String ZIP_S = ".zip";

    private static int BUFFER = 8192;

    /**
     * 根据指定内容，创建zip文件
     * @param destFileFullName 输出文件完整文件名
     * @param fileMap key: 文件名；value: 文件内容
     * @return
     */
    public static boolean outputZipFile(File destFileFullName, Map<String, String> fileMap) {

        try {

            FileOutputStream fos = new FileOutputStream(destFileFullName);
            return ZipFileUtil.outputZipStream(fos, fileMap);
        } catch (FileNotFoundException e) {
            logger.info("Create zip file threw exception ...." , e);
        }

        logger.info("Create zip file success: " + destFileFullName);
        return false;
    }

    /**
     * 根据指定内容，创建zip文件
     * @param outputStream 输出文件流
     * @param fileMap key: 文件名；value: 文件内容
     * @return
     */
    public static boolean outputZipStream(OutputStream outputStream, Map<String, String> fileMap) {

        ZipOutputStream out = null;
        byte data[] = new byte[BUFFER];

        try {
            out = new ZipOutputStream(new BufferedOutputStream(outputStream));
            for(Map.Entry<String, String> fileEntry : fileMap.entrySet()) {

                logger.info("Adding: " + fileEntry.getKey() + "to the zip file.");
                byte[] fileBytes = fileEntry.getValue().getBytes();
                BufferedInputStream origin = new BufferedInputStream(new ByteArrayInputStream(fileBytes), BUFFER);
                ZipEntry entry = new ZipEntry(fileEntry.getKey());

                out.putNextEntry(entry);
                for(int count; (count = origin.read(data, 0, BUFFER)) != -1; ) {
                    out.write(data, 0, count);
                }
                entry.setSize(fileBytes.length);
                entry.setComment(fileEntry.getKey());

                origin.close();
            }
        } catch (Exception e) {
            logger.info("Create zip archive threw exception ...." , e);
            return false;
        } finally {
            return closeIOResource(out);
        }
    }

    /**
     * 关闭IO流资源
     *
     * @param closeable
     * @return
     */
    public static boolean closeIOResource(Closeable closeable) {

        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            logger.info("Close io resource threw exception ...." , e);
            return false;
        }

        return true;
    }

}

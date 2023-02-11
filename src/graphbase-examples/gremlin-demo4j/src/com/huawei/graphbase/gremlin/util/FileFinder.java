package com.huawei.graphbase.gremlin.util;

import org.apache.commons.io.FileUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class FileFinder {

    public static InputStream loadConfig(String fileName) throws IOException {
        InputStream in = null;
        in = FileFinder.class.getClassLoader().getResourceAsStream(fileName);
        if (in == null) {
            try {
                in = new FileInputStream(FileUtils.getFile(fileName));
            } catch (FileNotFoundException e) {
                throw new RuntimeException("can not find file " + fileName);
            }
        }
        return in;
    }

}

package com.huawei.bigdata.examples.tools;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class TarManager {

  /**
   * main object
   */
  private static String MAIN_CLASS =
		  "com.huawei.bigdata.mapreduce.examples.FemaleInfoCollector";

  /**
   * JAR
   */
  private static String JAR_NAME = "mapreduce-examples.jar";

  /**
   * JAR compress
   *
   * @param source File : source file
   * @param target JarOutputStream : OutputStream
   * @throws IOException
   */
  private static void add(File source, JarOutputStream target)
      throws IOException {
    BufferedInputStream in = null;
    try {
      if (source.isDirectory()) {

        for (File nestedFile : source.listFiles())
          add(nestedFile, target);

        return;
      }

      // bin dir remove(from com directory start to copy)
      String path = source.getPath();
      int index = path.indexOf("com");
      path = path.substring(index);

      JarEntry entry = new JarEntry(path.replace("\\", "/"));
      entry.setTime(source.lastModified());
      target.putNextEntry(entry);
      in = new BufferedInputStream(new FileInputStream(source));

      // Copy
      byte[] buffer = new byte[1024];
      while (true) {
        int count = in.read(buffer);
        if (count == -1)
          break;
        target.write(buffer, 0, count);
      }
      target.closeEntry();
    } finally {
      if (in != null)
        in.close();
    }
  }

  /**
   * before run,must create project jar
   *
   * @throws IOException
   */
  public static void createJar() throws Exception {
    // Relative path
    String classPath = "bin" + File.separator + "main" + File.separator
        + "com";

    // Check already compile
    File cpFile = new File(classPath);
    if (cpFile.exists() == false) {
      throw new IOException("the class path does not exist.");
    }

    String[] child = cpFile.list();
    if (child == null || child.length <= 0) {
      throw new Exception("Please complie the project,then do this.");
    }

    // Delete JAR
    File jarFile = new File(JAR_NAME);
    if (jarFile.exists()) {
      jarFile.delete();
    }

    // Create JAR
    File sourceFile = new File(classPath);
    File[] files =
        {sourceFile};

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION,
        "1.0");
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, MAIN_CLASS);

    // JAR name
    JarOutputStream target = new JarOutputStream(new FileOutputStream(
        JAR_NAME), manifest);

    // Compress process
    for (int i = 0; i < files.length; i++) {
      add(files[i], target);
    }

    target.close();
  }
}

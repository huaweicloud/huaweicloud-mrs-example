package com.huawei.bigdata.examples.tools;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * use to upload the file to hdfs
 */
public class FileUploader {

  /**
   * filesystem
   */
  private FileSystem fileSystem;

  /**
   * target path
   */
  private String destPath;

  /**
   * target file name
   */
  private String fileName;

  /**
   * source file have source path
   */
  private String sourcePath;

  /**
   * to HDFS outstream
   */
  private FSDataOutputStream hdfsOutStream;

  /**
   * HDFS BufferedOutputStream
   */
  private BufferedOutputStream bufferOutStream;

  /**
   * construct
   *
   * @param fileSystem FileSystem : file system
   * @param destPath   String : target path(remote file server)
   * @param fileName   String : target file name
   * @param sourcePath String : source file(absolute path native file)
   */
  public FileUploader(FileSystem fileSystem, String destPath,
                      String fileName, String sourcePath) {
    this.fileSystem = fileSystem;
    this.destPath = destPath;
    this.fileName = fileName;
    this.sourcePath = sourcePath;
  }

  /**
   * write to hdfs
   *
   * @param inputStream InputStream : inputStream
   * @throws IOException , ParameterException
   */
  public void doWrite(InputStream inputStream) throws IOException {
    // Initialize
    setWriteResource();
    try {
      // Write to hdfs
      outputToHDFS(inputStream);
    } finally {
      closeResource();
    }
  }

  /**
   * write to the target directory
   *
   * @param inputStream InputStream
   * @throws IOException
   */
  private void outputToHDFS(InputStream inputStream) throws IOException {
    final int countForOneRead = 1024; // 1024 Bytes each time
    final byte buff[] = new byte[countForOneRead];
    int count;
    while ((count = inputStream.read(buff, 0, countForOneRead)) > 0) {
      bufferOutStream.write(buff, 0, count);
    }

    bufferOutStream.flush();
    hdfsOutStream.hflush();
  }

  /**
   * init object
   *
   * @throws IOException
   */
  private void setWriteResource() throws IOException {
    Path filepath = new Path(destPath + File.separator + fileName);
    hdfsOutStream = fileSystem.create(filepath);
    bufferOutStream = new BufferedOutputStream(hdfsOutStream);
  }

  /**
   * close resource
   */
  private void closeResource() throws IOException {
    // Close hdfsOutStream
    if (hdfsOutStream != null) {
      hdfsOutStream.close();
    }

    // Close bufferOutStream
    if (bufferOutStream != null) {
      bufferOutStream.close();
    }
  }

  /**
   * upload file
   *
   * @return boolean : result
   */
  public void upload() throws IOException {
    // Create target file directory
    Path destFilePath = new Path(destPath);
    createPath(destFilePath);

    // Inputstream
    FileInputStream input = null;
    try {
      input = new FileInputStream(sourcePath);
      doWrite(input);
    } finally {
      close(input);
    }
  }

  /**
   * create a file path
   *
   * @param filePath Path : file path
   * @return boolean : result
   */
  private void createPath(Path filePath) throws IOException {
    if (!fileSystem.exists(filePath)) {
      // Create this directory
      fileSystem.mkdirs(filePath);
    }
  }

  /**
   * close stream
   *
   * @param stream Closeable : stream object
   */
  private void close(Closeable stream) throws IOException {
    if (stream == null) {
      return;
    }

    stream.close();
  }
}
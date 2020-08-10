package com.huawei.bigdata.hdfs.examples;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsWriter {

	private FSDataOutputStream hdfsOutStream;

	private BufferedOutputStream bufferOutStream;

	private FileSystem fSystem;
	private String fileFullName;

	public HdfsWriter(FileSystem fSystem, String fileFullName) throws ParameterException {
		if ((null == fSystem) || (null == fileFullName)) {
			throw new ParameterException("some of input parameters are null.");
		}

		this.fSystem = fSystem;
		this.fileFullName = fileFullName;
	}

	/**
	 * append the inputStream to a file in HDFS
	 * 
	 * @param inputStream
	 * @throws IOException
	 * @throws ParameterException
	 */
	public void doWrite(InputStream inputStream) throws IOException, ParameterException {
		if (null == inputStream) {
			throw new ParameterException("some of input parameters are null.");
		}

		setWriteResource();
		try {
			outputToHDFS(inputStream);
		} finally {
			closeResource();
		}
	}

	/**
	 * append the inputStream to a file in HDFS
	 * 
	 * @param inputStream
	 * @throws IOException
	 * @throws ParameterException
	 */
	public void doAppend(InputStream inputStream) throws IOException, ParameterException {
		if (null == inputStream) {
			throw new ParameterException("some of input parameters are null.");
		}

		setAppendResource();
		try {
			outputToHDFS(inputStream);
		} finally {
			closeResource();
		}
	}

	private void outputToHDFS(InputStream inputStream) throws IOException {
		final int countForOneRead = 10240; // 10240 Bytes each time
		final byte buff[] = new byte[countForOneRead];
		int count;

		while ((count = inputStream.read(buff, 0, countForOneRead)) > 0) {
			bufferOutStream.write(buff, 0, count);
		}

		bufferOutStream.flush();
		hdfsOutStream.hflush();
	}

	private void setWriteResource() throws IOException {
		Path filepath = new Path(fileFullName);
		hdfsOutStream = fSystem.create(filepath);
		bufferOutStream = new BufferedOutputStream(hdfsOutStream);
	}

	private void setAppendResource() throws IOException {
		Path filepath = new Path(fileFullName);
		hdfsOutStream = fSystem.append(filepath);
		bufferOutStream = new BufferedOutputStream(hdfsOutStream);
	}

	private void closeResource() {
		// close hdfsOutStream
		if (hdfsOutStream != null) {
			try {
				hdfsOutStream.close();
			} catch (IOException e) {
				System.out.println(e);
			}
		}

		// close bufferOutStream
		if (bufferOutStream != null) {
			try {
				bufferOutStream.close();
			} catch (IOException e) {
				System.out.println(e);
			}
		}
	}
}

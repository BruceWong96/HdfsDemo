package com.tarena;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class Testdemo {

	
	//下载hdfs中的文件
	@Test
	public void connect_get() throws IOException, URISyntaxException {
		//获取Hadoop环境变量对象
		Configuration conf = new Configuration();
		
		//可以通过此对象来配置环境
		//通过代码设定的优先级要高于文件配置的优先级 约定大于配置
		//代码配置的生效范围是局部
		conf.set("dfs.replication", "1");
		
		//连接HDFS文件系统
		FileSystem fileSystem = FileSystem.get(
				new URI("hdfs://192.168.1.130:9000"),
				conf);
		//获取HDFS指定文件的输入流
		InputStream inputStream = fileSystem.open(new Path("/part01/1.txt"));
		//获取本地输入流
		OutputStream outputStream = new FileOutputStream(new File("src/data.txt"));
		//通过Hadoop提供的工具类 完成数据传输
		IOUtils.copyBytes(inputStream, outputStream, conf);
	}
	
	
	//上传文件
	@Test
	public void connect_put() throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(
				new URI("hdfs://192.168.1.130:9000"),
				conf);
		//hdfs输出流
		OutputStream outputStream = fileSystem.create(new Path("/part02/2.txt"));
		//本地文件输入流
		InputStream inputStream = new FileInputStream(new File("src/data.txt"));

		IOUtils.copyBytes(inputStream, outputStream, conf);
		
	}
	
	
	@Test
	public void other() throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(
				new URI("hdfs://192.168.1.130:9000"),
				conf);
		
		//删除指定目录或者文件，true表示递归删除
//		fileSystem.delete(new Path("/part02"),true);

		//目录重命名
		fileSystem.rename(new Path("/part01"), new Path("/part02"));
		
		
	}
}

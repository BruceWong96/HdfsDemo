package com.tarena;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Testdemo {

	public void connect() throws IOException, URISyntaxException {
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
	}
}

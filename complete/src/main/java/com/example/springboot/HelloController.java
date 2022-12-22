package com.example.springboot;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;

@RestController
public class HelloController {

	@GetMapping("/")
	public String index(@RequestParam("id") int id, @RequestParam("sz") int sz) {

		String res = UUID.randomUUID().toString();
		
		try {
			String fileName = sz + "mb_" + id;

			String workingDir = System.getProperty("user.dir");
			String localSrc = workingDir + "\\..\\" + fileName + ".zip";
            // System.out.println("Working Directory = " + path);

			//Input stream for the file in local file system to be written to HDFS
			InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

			//Get configuration of Hadoop system
			Configuration configuration = new Configuration();
			configuration.set("fs.defaultFS", "hdfs://localhost:9000");
			// System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));

			//Destination file in HDFS
			String dst = "/" + res;
			FileSystem fs = FileSystem.get(URI.create(dst), configuration);
			OutputStream out = fs.create(new Path(dst));

			//Copy file from local to HDFS
			IOUtils.copyBytes(in, out, 4096, true);

			// System.out.println(dst + " copied to HDFS");
			// res = dst + " copied to HDFS";
		} catch (IOException e) {
			// res = e.getMessage();
		}
		return res;
	}

	@GetMapping("/clear")
	public String clear() { 

		String res = "all clean";
		
		try {
			//Get configuration of Hadoop system
			Configuration configuration = new Configuration();
			configuration.set("fs.defaultFS", "hdfs://localhost:9000");
			// System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));

			//Destination file in HDFS
			String dst = "/";
			FileSystem fs = FileSystem.get(URI.create(dst), configuration);
			// Path home = fs.getHomeDirectory();

			//the second boolean parameter here sets the recursion to true
			RemoteIterator<LocatedFileStatus> fileStatusListIterator = 
				fs.listFiles(new Path(dst), true);

			while(fileStatusListIterator.hasNext()) {
				LocatedFileStatus fileStatus = fileStatusListIterator.next();
				//do stuff with the file like ...
				fs.delete(fileStatus.getPath(), true);
			}

			
			
		} catch (IOException e) {
			res = e.getMessage();
		}
		return res;
	}
}

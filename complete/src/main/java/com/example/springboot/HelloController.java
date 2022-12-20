package com.example.springboot;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

@RestController
public class HelloController {

	@GetMapping("/")
	public String index() {

		long startTime = System.nanoTime();
// .....your program....


		String res = "Greetings from Spring Boot!";

		
		try {
			// Configuration configuration = new Configuration();
			// configuration.set("fs.defaultFS", "hdfs://localhost:9000");
			// // configuration.set("fs.defaultFS", "http://localhost:9000");
			// FileSystem fileSystem;
			// fileSystem = FileSystem.get(configuration);
			// String directoryName = "zone/readwriteexample";
			// Path path = new Path(directoryName);
			// fileSystem.mkdirs(path);

			// Configuration configuration = new Configuration();
			// configuration.set("fs.defaultFS", "hdfs://localhost:9000");
			// FileSystem fileSystem = FileSystem.get(configuration);
			// //Create a path
			// String fileName = "read_write_hdfs_example.txt";
			// Path hdfsWritePath = new Path("/user/zone/readwriteexample/" + fileName);
			// FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);

			// BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
			// bufferedWriter.write("Java API to write data in HDFS");
			// bufferedWriter.newLine();
			// bufferedWriter.close();
			// fileSystem.close();

			String workingDir = System.getProperty("user.dir");
			String localSrc = workingDir + "\\test.zip";
            // System.out.println("Working Directory = " + path);

			//Input stream for the file in local file system to be written to HDFS
			InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

			//Get configuration of Hadoop system
			Configuration configuration = new Configuration();
			configuration.set("fs.defaultFS", "hdfs://localhost:9000");
			// System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));

			//Destination file in HDFS
			String dst = "/biggzone22";
			FileSystem fs = FileSystem.get(URI.create(dst), configuration);
			OutputStream out = fs.create(new Path(dst));

			//Copy file from local to HDFS
			IOUtils.copyBytes(in, out, 4096, true);

			// System.out.println(dst + " copied to HDFS");
			res = dst + " copied to HDFS";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			res = e.getMessage();
		}
        
		long endTime   = System.nanoTime();
		long totalTime = endTime - startTime;
		// System.out.println(totalTime);

		// return res + " : " + totalTime;

		return "HJlald";
	}
}

package com.ingestion.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HDFSUtils {

	public static void copyFileLZToRaw(String srcF, String destF) throws IOException {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
		
		FileSystem filesystem = FileSystem.get(configuration);
		
		FileUtil.copy(filesystem, new Path(srcF), filesystem, new Path(destF), false, configuration);
	}
	
	public static Map<String, String> readHDFSFile(Path lPath) throws IOException, URISyntaxException {
		Map<String, String> schemaMap = new LinkedHashMap<String, String>();
		try (final DistributedFileSystem ldFS = new DistributedFileSystem() {
			{
				initialize(new URI("hdfs://localhost:9000"), new Configuration());
			}
		};
		final FSDataInputStream lstreamReader = ldFS.open(lPath);
		final Scanner lscanner = new Scanner(lstreamReader);) {
			System.out.println("LZ File Contents: ");
			while (lscanner.hasNextLine()) {
				String nextLine	=	lscanner.nextLine();
				String[] split = nextLine.split(" ");
				
				schemaMap.put(split[0], split[1]);
				System.out.println("---> " + nextLine);
			}
	    }
		return schemaMap;
	}
	
}

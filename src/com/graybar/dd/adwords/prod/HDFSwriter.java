package com.graybar.dd.adwords.prod;

import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import java.net.URI;

public abstract class HDFSwriter extends Configured implements Tool {

	public static final String FS_PARAM_NAME = "fs.defaultFS";
	
	public static void streamCreate(InputStream input, Path outputPath) throws Exception {
		URI SERVER_URI = outputPath.toUri();
		FSDataOutputStream os = null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs= FileSystem.get((SERVER_URI), conf, "hdfs");
			os = fs.create(outputPath,
				    new Progressable() {
				        public void progress() {
				        	System.out.print(".");
						}
					});
			if (input != null) {
				System.out.println("\nCreating " + outputPath.toString());
				IOUtils.copyBytes(input, os, conf);
				System.out.print(outputPath.getName() + " complete. \n");
			}
			else if (input == null) {
				System.out.println("Unable to write " +outputPath.getName()+ " to HDFS, inputstream is null.\n");
				os.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			os.close();
		}
    }
	public static void streamAppend(InputStream input, Path outputPath) throws Exception {
		URI SERVER_URI = outputPath.toUri();
		FSDataOutputStream os = null;
		try {
			Configuration conf = new Configuration();
//			Enable this command to assist with appending data in small datanode environments. Fixes bad and too few datanode errors.	
			conf.set("dfs.client.block.write.replace-datanode-on-failure.best-effort", "true");
			FileSystem fs= FileSystem.get((SERVER_URI), conf, "hdfs");
			os = fs.append(outputPath, 8192,
				    new Progressable() {
				        public void progress() {
				        	System.out.print(".");
						}
					});
			if (input != null) {
				System.out.println("\nAppending to " + outputPath.toString());
				IOUtils.copyBytes(input, os, conf);
				System.out.print(outputPath.getName() + " complete. \n");
			}
			else if (input == null) {
				System.out.println("Unable to write " +outputPath.getName()+ " to HDFS, inputstream is null.\n");
				os.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			input.close();
			os.close();
		}
    }

	public static boolean fileDelete(Path deletePath) throws Exception {
		try {
			URI SERVER_URI = deletePath.toUri();
			Configuration conf = new Configuration();
			FileSystem fs= FileSystem.get((SERVER_URI), conf, "hdfs");
			if (!fs.exists(deletePath)) {
				System.out.println("Unable to delete. " + deletePath.getName() + " does not exist.");
				return false;
			}
			System.out.println("Deleting "+ deletePath.getName() + "...");
			fs.delete(deletePath, false);
			System.out.print("done.");
			return true;
		} catch (Exception e) {
			System.out.println("Unable to delete " + deletePath.getName());
			e.printStackTrace();
		}
		return false;
	}
}

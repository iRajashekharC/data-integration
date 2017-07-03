package com.ingestion.access.realtime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.spark.FamilyHFileWriteOptions;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Run this example using command below:
 *
 *  SPARK_HOME/bin/spark-submit --master local[2] --class org.apache.hadoop.hbase.spark.example.hbasecontext.JavaHBaseBulkLoadExample
 *  path/to/hbase-spark.jar {path/to/output/HFiles}
 *
 * This example will output put hfiles in {path/to/output/HFiles}, and user can run
 * 'hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles' to load the HFiles into table to verify this example.
 */
final public class JavaHBaseBulkLoadExample {
  private JavaHBaseBulkLoadExample() {}

  public static void main(String[] args) {

    String tableName = "bulkload-table-test";
    String columnFamily1 = "f1";
    String columnFamily2 = "f2";

    SparkConf sparkConf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory","1g");

    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      List<String> list= new ArrayList<String>();
      // row1
      list.add("11," + columnFamily1 + ",b,1");
      // row3
      list.add("33," + columnFamily1 + ",a,2");
      list.add("33," + columnFamily1 + ",b,1");
      list.add("33," + columnFamily2 + ",a,1");
      /* row2 */
      list.add("22," + columnFamily2 + ",a,3");
      list.add("22," + columnFamily2 + ",b,3");

      JavaRDD<String> rdd = jsc.parallelize(list);

      Configuration conf = HBaseConfiguration.create();
      	conf.set("hbase.zookeeper.quorum", "localhost");
  		conf.set("hbase.zookeeper.property.clientPort", "2181");
  		conf.setInt("hbase.client.retries.number", 7);
  		conf.setInt("ipc.client.connect.max.retries", 3);

      JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);


      //hbaseContext.bulkLoad(rdd, TableName.valueOf(tableName),new BulkLoadFunction(), "/tmp/hbase_temp2",
          //new HashMap<byte[], FamilyHFileWriteOptions>(), false, HConstants.DEFAULT_MAX_FILE_SIZE);
      try {
		LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
		//load.doBulkLoad(new Path("/tmp/hbase_temp"), table);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    } finally {
      jsc.stop();
    }
  }

  public static class BulkLoadFunction implements Function<String, Pair<KeyFamilyQualifier, byte[]>> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
    public Pair<KeyFamilyQualifier, byte[]> call(String v1) throws Exception {
      if (v1 == null)
        return null;
      String[] strs = v1.split(",");
      if(strs.length != 4)
        return null;
      KeyFamilyQualifier kfq = new KeyFamilyQualifier(Bytes.toBytes(strs[0]), Bytes.toBytes(strs[1]),
          Bytes.toBytes(strs[2]));
      System.out.println("--------------------------------"+new Pair<KeyFamilyQualifier, byte[]>(kfq, Bytes.toBytes(strs[3])));
      return new Pair<KeyFamilyQualifier, byte[]>(kfq, Bytes.toBytes(strs[3]));
    }
  }
}
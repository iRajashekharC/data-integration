package com.ingestion.access.realtime;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.FamilyHFileWriteOptions;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.ingestion.configs.SparkJobContext;

/**
 * Created by sunilpatil on 1/11/17.
 */
public class ReadFromKafkaPushToHbase {
	
    public static void main(String[] argv) throws Exception{

        // Configure Spark to connect to Kafka running on local machine
    	JavaStreamingContext jssc = SparkJobContext.getStreamContext();
		// Configure Spark to listen messages in topic test
		Collection<String> topics = Arrays.asList("test-output");

        // Start reading messages from Kafka and get DStream
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), 
                                              ConsumerStrategies.<String,String>Subscribe(topics,SparkJobContext.getKafkaParams()));
        
        final JavaHBaseContext hbaseContext = new JavaHBaseContext(SparkJobContext.getSparkContext(), HBaseConfiguration.create());
        final String tableName = "bulkload-table-test";
        hbaseContext.streamBulkPut(stream,
                TableName.valueOf(tableName),
                new PutFunction());
        
        
        /*
        stream.foreachRDD(eachRDD -> {
	    	  hbaseContext.bulkLoad(eachRDD, TableName.valueOf(tableName), null, 
	    			  "/tmp/hbase_temp", new HashMap<byte[], FamilyHFileWriteOptions>(), false, HConstants.DEFAULT_MAX_FILE_SIZE);
	          
	      });
        
        */
	      jssc.start();
        jssc.awaitTermination();
    }
    public static class BulkLoadFunction implements Function<String, Pair<KeyFamilyQualifier, byte[]>> {

        @Override
        public Pair<KeyFamilyQualifier, byte[]> call(String v1) throws Exception {
          if (v1 == null)
            return null;
          String[] strs = v1.split(",");
          if(strs.length != 4)
            return null;
          KeyFamilyQualifier kfq = new KeyFamilyQualifier(Bytes.toBytes(strs[0]), Bytes.toBytes(strs[1]),
              Bytes.toBytes(strs[2]));
          return new Pair(kfq, Bytes.toBytes(strs[3]));
        }
      }
    public static class PutFunction implements Function<String, Put> {

        private static final long serialVersionUID = 1L;

        public  Put call(String v) throws Exception {
          String[] part = v.split(",");
          Put put = new Put(Bytes.toBytes(part[0]));

          put.addColumn(Bytes.toBytes(part[1]),
                  Bytes.toBytes(part[2]),
                  Bytes.toBytes(part[3]));
          return put;
        }

      }
}

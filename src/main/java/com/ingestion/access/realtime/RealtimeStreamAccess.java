package com.ingestion.access.realtime;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.spark.FamilyHFileWriteOptions;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.ingestion.configs.SparkJobContext;

import scala.Tuple2;

/**
 * Created by sunilpatil on 1/11/17.
 */
public class RealtimeStreamAccess {
    public static void main(String[] argv) throws Exception{

        // Configure Spark to connect to Kafka running on local machine
        
    	JavaStreamingContext jssc = SparkJobContext.getStreamContext();
		// Configure Spark to listen messages in topic test
		Collection<String> topics = Arrays.asList("fast-messages");

        // Start reading messages from Kafka and get DStream
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), 
                                              ConsumerStrategies.<String,String>Subscribe(topics,SparkJobContext.getKafkaParams()));

        // Read value of each message from Kafka and return it
        JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String,String>, String>() {
            //@Override
            public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
                return kafkaRecord.value();
            }
        });

        // Break every message into words and return list of words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
           // @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        // Take every word and return Tuple with (word,1)
        JavaPairDStream<String,Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
            //@Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        // Count occurance of each word
        JavaPairDStream<String,Integer> wordCount = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            // @Override
             public Integer call(Integer first, Integer second) throws Exception {
                 return first+second;
             }
         });

        //Print the word count
        wordCount.print();
        wordCount.foreachRDD(v1 -> {
        	String kafkaOpTopic = "test-output";
        	 Map<String, Object> props = new HashMap<String, Object>();
        	 props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        	 props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                     "org.apache.kafka.common.serialization.StringSerializer");
             props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      "org.apache.kafka.common.serialization.StringSerializer");
            
             v1.foreach(record -> {
            	 KafkaProducer<String, String>  producer1 = new KafkaProducer<>(props);
            	 String data = record.toString();
            	 ProducerRecord<String, String> message = new ProducerRecord<String, String>(kafkaOpTopic, null, data)   ;   
            	 producer1.send(message);
            	 producer1.close();
             });
             

        });
        jssc.start();
        jssc.awaitTermination();
    }
    
}

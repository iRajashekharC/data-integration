package com.ingestion.configs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkJobContext {
	private static Properties prop = null;
	private static JavaStreamingContext jssc;
	private static JavaSparkContext jsc;
	static SparkConf conf;
	
	public static Map<String, Object> getKafkaParams(){
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
		// kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		return kafkaParams;
	}
	public static void init() {
		InputStream is = null;
		try {
			prop = new Properties();
			ClassLoader loader = Thread.currentThread().getContextClassLoader(); 
			is = loader.getResourceAsStream("kafka.props");
			prop.load(is);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("1-----> " + prop.getProperty("bootstrap.servers"));

		

	}
	public static SparkConf getSparkConf() {
		if(conf == null){
			conf	= new SparkConf().setMaster("local[2]").setAppName("DataIntegrationRealtimeStream			")
					.set("spark.driver.allowMultipleContexts", "true");
		}
		
		return conf;
	}
	
	public static JavaStreamingContext getStreamContext() {
		init();
		if(jssc == null){
			jssc = new JavaStreamingContext(getSparkContext(), Durations.seconds(2));
		}
		
		return jssc;
	}
	
	public static JavaSparkContext getSparkContext() {
		init();
		if(jsc == null){
			jsc = new JavaSparkContext(SparkContext.getOrCreate(getSparkConf()));
		}
		
		return jsc;
	}
	
	public static void main(String[] args) {
		new SparkJobContext().init();
	}

}

package com.example.kafkaDemo.data;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams;
import com.example.kafkaDemo.data.AggregationResult;

public class KafkaStreamConsumer {
	
	public static void main(String args[]) {
		
		String inputTopic="wikipedia_changeevents";
		String outputTopic="Consume_topic";
		
		//System.setProperty("javax.net.debug", "ssl");
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "your-application-id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		
		StreamsBuilder builder=new StreamsBuilder();
		KStream<String, String> stream=builder.stream(inputTopic);
		
		KTable<Boolean,Long> bl=stream.selectKey((key,value)-> {
			JsonNode node1 = null;
			try {
				node1 = new ObjectMapper().readTree(value);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return node1.get("bot").asBoolean();
		}).groupByKey()
		.count();
		
		
		/*
		 * KStream<String,String> filteredStream=stream.filter((key,value)-> { try {
		 * JsonNode node1=new ObjectMapper().readTree(value); return
		 * node1.get("bot").asBoolean(); } catch(Exception e) { return false; } });
		 */
		
		
		bl.toStream()
		.to(outputTopic);
			
		
		//stream.to(outputTopic);
		
		KafkaStreams kafkaStreams=new KafkaStreams(builder.build(),props);
		kafkaStreams.start();
		
	
	}
}

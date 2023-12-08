package com.example.kafkaDemo.data;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;

public class WikimediaDataProducer {
	
	public static void main(String args[]) throws InterruptedException {
		 	
			String topic="wikipedia_changeevents";
			//System.setProperty("javax.net.debug", "ssl");
			Properties properties = new Properties();
	        // connect to Localhost
		
	        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
	      
	        // set producer properties
	        properties.setProperty("key.serializer", StringSerializer.class.getName());
	        properties.setProperty("value.serializer", StringSerializer.class.getName());
	   
	        
	        KafkaProducer<String,String> producer=new KafkaProducer<>(properties);
	       
	        
	        EventHandler event=new WikimediaChangeHandler(producer,topic);
	        String url="https://stream.wikimedia.org/v2/stream/recentchange";
	        EventSource.Builder builder=new EventSource.Builder(event,URI.create(url));
	        
	        EventSource source=builder.build();
	        
	        source.start();
	        
	        TimeUnit.MINUTES.sleep(10);
	        
	}
}

package com.example.kafkaDemo.data;

import java.io.IOException;
import java.util.Optional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

import ch.qos.logback.classic.Logger;


public class WikimediaChangeHandler implements EventHandler {
	
	private KafkaProducer<String,String> producer;
	private String topic;
	
	public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
		this.producer = producer;
		this.topic = topic;
	}
	
	private static final Logger log =(Logger) LoggerFactory.getLogger(WikimediaChangeHandler.class);
	
	@Override
	public void onOpen() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onClosed() throws Exception {
		// TODO Auto-generated method stub
		producer.close();
		
	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) throws Exception {
		log.info(messageEvent.getData());
		producer.send(new ProducerRecord<>(topic,"true",messageEvent.getData()));
	
	}

	@Override
	public void onComment(String comment) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onError(Throwable t) {
		// TODO Auto-generated method stub
		
	}
	
	

}

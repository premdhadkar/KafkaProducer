package com.infy.controller;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.infy.configurations.KafkaConfig;
import com.infy.model.Customer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class Controller {
	@Value("${topic}") 
	private String topic;
	
	@Autowired
	private KafkaTemplate<String, Customer>	kafkaProducer;
	
	@PostMapping(value =  "/producer")
	public void sendMsg (@RequestBody Customer customer) {
		while(true) {
			ProducerRecord<String, Customer> record = new ProducerRecord<String, Customer>(topic, customer);
			kafkaProducer.send(record);
			log.info("record sent successfully: "+customer);
		}
	}
	
}

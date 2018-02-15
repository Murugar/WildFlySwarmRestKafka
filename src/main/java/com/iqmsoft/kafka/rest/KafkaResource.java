package com.iqmsoft.kafka.rest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.iqmsoft.kafka.producer.ProdProperties;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/kafka")
@Stateless
public class KafkaResource {

	static int roundRobin = 0;

	private static String TOPIC = "kafka_test";

	@Inject
    ProdProperties properties;


	@GET
	@Path("/ping")
	@Produces(MediaType.TEXT_PLAIN)
	public Response testMessage() {

		return Response.ok("method service is running").build();
	}

	@GET
	@Path("/send")
	@Consumes(MediaType.TEXT_PLAIN)
	public void postClichedMessage() {
		System.out.println("Sending message");

		KafkaProducer producer = new KafkaProducer(properties.getProperties());

		ProducerRecord record = new ProducerRecord(TOPIC, String.valueOf(roundRobin), "Some message :)");
		roundRobin ++;

		try{
			producer.send(record);
			System.out.println("message sent to topic: "+TOPIC+", round robin: "+roundRobin);
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			producer.close();
		}
	}
}
package com.diviso.customer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import com.diviso.customer.avro.*;

@SpringBootApplication
public class CustomerApplication {

	public static final String CUSTOMER_REGISTRATION_TOPIC = "customer-registration";
	public static final String BOOTSTRAP_SERVER_URL = "localhost:9092";
	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	public static final String STATE_DIR = "/tmp/gatewayapp";

	public static void main(String[] args) {
	
		SpringApplication.run(CustomerApplication.class, args);

		final KafkaStreams streams = createStreams();
		streams.cleanUp();
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	public static KafkaStreams createStreams() {

		final Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "customer-service");
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

		final StreamsBuilder builder = new StreamsBuilder();

		final SpecificAvroSerde<customer> customerSerde = new SpecificAvroSerde<>();

		final Map<String, String> serdeConfig = Collections
				.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		final KStream<Long, customer> customerStream = builder.stream(CUSTOMER_REGISTRATION_TOPIC,
				Consumed.with(Serdes.Long(), customerSerde));
		customerSerde.configure(serdeConfig, true);
		return new KafkaStreams(builder.build(), streamsConfiguration);

	}
}

package com.diviso.customer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class CustomerApplication {

	static final String bootstrapServers = "localhost:9092";
	static final String schemaRegistryUrl = "http://localhost:8081";

	public static void main(String[] args) {

		final KafkaStreams streams = createStreams(bootstrapServers, schemaRegistryUrl,
				"/tmp/kafka-streams-global-tables");

		SpringApplication.run(CustomerApplication.class, args);

	}

	public static KafkaStreams createStreams(final String bootstrapServers, final String schemaRegistryUrl,
			final String stateDir) {

		final Properties streamsConfiguration = new Properties();
		// Give the Streams application a unique name. The name must be unique
		// in the Kafka cluster
		// against which the application is run.
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-tables-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "global-tables-example-client");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
		// Set to earliest so we don't miss any data that arrived in the topics
		// before the process
		// started
		streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		 final StreamsBuilder builder = new StreamsBuilder();

		return new KafkaStreams(builder.build(), new StreamsConfig(streamsConfiguration));
	}
}



package com.appsdeveloperblog.ws.emailnotification;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

@Configuration
public class KafkaConsumerConfiguration {

	@Autowired
	Environment environment;

	@Bean
	ConsumerFactory<String, Object> consumerFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
		config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);
		config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES,
				environment.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages"));
		config.put(JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS,
				environment.getProperty("spring.kafka.consumer.properties.spring.json.use.type.headers"));
		config.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE,
				environment.getProperty("spring.kafka.consumer.properties.spring.json.value.default.type"));
		config.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"));

		return new DefaultKafkaConsumerFactory<>(config);
	}

	@Bean
	ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
			ConsumerFactory<String, Object> consumerFactory, KafkaTemplate<String, Object> kafkaTemplate) {
		
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate,
				// negative partition lets Kafka pick one, so the DLT partition count doesn't matter
				(record, exception) -> new TopicPartition(record.topic() + ".DLT", -1)));

		ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setCommonErrorHandler(errorHandler);
		
		return factory;
	}
	
	@Bean
	KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
		return new KafkaTemplate<>(producerFactory);
	}
	
	@Bean
	ProducerFactory<String, Object> producerFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		// failed deserializations reach the DLT as raw byte[]; send those untouched instead of base64 JSON
		Map<Class<?>, Serializer<?>> valueSerializers = new LinkedHashMap<>();
		valueSerializers.put(byte[].class, new ByteArraySerializer());
		valueSerializers.put(Object.class, new JacksonJsonSerializer<>());

		return new DefaultKafkaProducerFactory<>(config, new StringSerializer(),
				new DelegatingByTypeSerializer(valueSerializers, true));
	}

	@Bean
	NewTopic deadLetterTopic() {
		return TopicBuilder.name("product-created-events-topic.DLT")
				.partitions(3)
				.replicas(3)
				.build();
	}
	
	
}
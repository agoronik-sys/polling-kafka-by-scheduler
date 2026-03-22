package com.example.pollingkafka.config;

import com.example.pollingkafka.model.IncomingData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Ручной commitSync в PriorityPollingService; иначе auto-commit + commitSync дают гонки и «перепрыгивание» offset’ов
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Ограничим количество сообщений за один poll (будет переопределено из application.yml при желании)
        props.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * ConsumerFactory для общего listener-а, который читает объекты IncomingData из топика processing-all.
     */
    @Bean
    public ConsumerFactory<String, IncomingData> incomingConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        JsonDeserializer<IncomingData> valueDeserializer = new JsonDeserializer<>(IncomingData.class);
        valueDeserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, IncomingData> incomingKafkaListenerContainerFactory(
            ConsumerFactory<String, IncomingData> incomingConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, IncomingData> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(incomingConsumerFactory);
        // concurrency = 1, как запрошено
        factory.setConcurrency(1);
        return factory;
    }

    /**
     * Producer / KafkaTemplate для записи IncomingData в системные топики
     * (processing-all.system-*-quick / slow).
     */
    @Bean
    public ProducerFactory<String, IncomingData> incomingProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, IncomingData> incomingKafkaTemplate(
            ProducerFactory<String, IncomingData> incomingProducerFactory) {
        return new KafkaTemplate<>(incomingProducerFactory);
    }
}


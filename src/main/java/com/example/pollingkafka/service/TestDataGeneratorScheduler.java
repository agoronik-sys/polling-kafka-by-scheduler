package com.example.pollingkafka.service;

import com.example.pollingkafka.config.ProcessingProperties;
import com.example.pollingkafka.config.ProcessingProperties.SystemConfig;
import com.example.pollingkafka.model.IncomingData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Тестовый шедуллер, который раз в минуту генерирует
 * случайные сообщения {@link IncomingData} и отправляет их
 * в общий топик processing-all.
 *
 * Нужен только для локальной отладки потока:
 * routing listener -> системные топики -> шедуллеры RPS.
 */
@Service
public class TestDataGeneratorScheduler {

    private static final Logger log = LoggerFactory.getLogger(TestDataGeneratorScheduler.class);

    private final KafkaTemplate<String, IncomingData> kafkaTemplate;
    private final ProcessingProperties processingProperties;

    private final List<String> systemCodes;

    public TestDataGeneratorScheduler(KafkaTemplate<String, IncomingData> kafkaTemplate,
                                      ProcessingProperties processingProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.processingProperties = processingProperties;
        this.systemCodes = processingProperties.getSystems() == null
                ? List.of("SYSTEM_A", "SYSTEM_B", "SYSTEM_C", "SYSTEM_D")
                : processingProperties.getSystems()
                                      .stream()
                                      .map(SystemConfig::getId)
                                      .collect(Collectors.toList());
    }

    /**
     * Раз в минуту генерируем от 1 до 50 тестовых сообщений
     * с разными systemCode и приоритетом (quick / slow)
     * и отправляем их в входной топик.
     */
    @Scheduled(fixedRateString = "60000")
    public void generateTestMessages() {
        if (systemCodes.isEmpty()) {
            return;
        }

        int count = ThreadLocalRandom.current().nextInt(1, 51);
        String topic = processingProperties.getIncomingTopic();
        String priorityHeaderName = processingProperties.getHeaders().getPriority();
        String quickValue = processingProperties.getHighPriorityValue(); // обычно "quick"

        for (int i = 0; i < count; i++) {
            String systemCode = randomSystemCode();
            boolean quick = ThreadLocalRandom.current().nextBoolean();
            String priorityValue = quick ? quickValue : "slow";

            IncomingData payload = new IncomingData();
            payload.setSystemCode(systemCode);
            payload.setData("data-" + System.currentTimeMillis() + "-" + i);
            payload.setOtherData("other-" + i);
            payload.setCustom(ThreadLocalRandom.current().nextBoolean());

            var message = MessageBuilder.withPayload(payload)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader(priorityHeaderName, priorityValue)
                    .build();

            kafkaTemplate.send(message);
        }

        log.info("Generated {} test messages into topic={}", count, topic);
    }

    private String randomSystemCode() {
        int idx = ThreadLocalRandom.current().nextInt(systemCodes.size());
        return systemCodes.get(idx);
    }
}


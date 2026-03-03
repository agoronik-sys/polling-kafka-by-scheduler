package com.example.pollingkafka.service;

import com.example.pollingkafka.config.ProcessingProperties;
import com.example.pollingkafka.model.IncomingData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Listener общего топика (processing-all), который читает IncomingData
 * и рассыпает его по системным топикам:
 * processing-all.system-a-quick / processing-all.system-a-slow / ...
 */
@Service
public class IncomingRoutingListener {

    private static final Logger log = LoggerFactory.getLogger(IncomingRoutingListener.class);

    private final KafkaTemplate<String, IncomingData> kafkaTemplate;
    private final ProcessingProperties properties;
    private final TopicNameResolver topicNameResolver;

    public IncomingRoutingListener(KafkaTemplate<String, IncomingData> kafkaTemplate,
                                   ProcessingProperties properties,
                                   TopicNameResolver topicNameResolver) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.topicNameResolver = topicNameResolver;
    }

    /**
     * Основной listener, читает общий топик и маршрутизирует сообщения.
     *
     * concurrency = 1 на уровне containerFactory (см. KafkaConsumerConfig).
     * groupId = processing.incoming-group-id (по умолчанию b-listen-all),
     * чтобы одинаковые экземпляры сервиса делили нагрузку и не дублировали сообщения.
     */
    @KafkaListener(
            topics = "#{@processingProperties.incomingTopic}",
            groupId = "#{@processingProperties.incomingGroupId}",
            containerFactory = "incomingKafkaListenerContainerFactory"
    )
    public void onMessage(@Payload IncomingData payload,
                          @Header(name = "X-Priority", required = false) String priorityHeader,
                          @Header(name = KafkaHeaders.RECEIVED_TOPIC, required = false) String sourceTopic) {

        if (payload == null) {
            return;
        }

        String systemCode = payload.getSystemCode();
        if (!topicNameResolver.isKnownSystem(systemCode)) {
            log.warn("Skip message from unknown systemCode={} topic={}", systemCode, sourceTopic);
            return;
        }

        boolean quick = isQuick(priorityHeader);
        String targetTopic = quick
                ? topicNameResolver.quickTopic(systemCode)
                : topicNameResolver.slowTopic(systemCode);

        kafkaTemplate.send(targetTopic, systemCode, payload);

        log.info(
                "Routed message systemCode={} priority={} from topic={} to topic={}",
                systemCode,
                quick ? "quick" : "slow",
                sourceTopic,
                targetTopic
        );
    }

    private boolean isQuick(String priorityHeader) {
        if (priorityHeader == null) {
            return false;
        }
        String expectedQuick = properties.getHighPriorityValue();
        return expectedQuick != null && expectedQuick.equalsIgnoreCase(priorityHeader);
    }
}


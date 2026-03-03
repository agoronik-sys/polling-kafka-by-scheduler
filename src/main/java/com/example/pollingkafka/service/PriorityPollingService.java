package com.example.pollingkafka.service;

import com.example.pollingkafka.config.ProcessingProperties;
import com.example.pollingkafka.config.ProcessingProperties.SystemConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Сервис, который читает сообщения из Kafka не через {@code @KafkaListener},
 * а через ручной {@link KafkaConsumer#poll(Duration)} по расписанию.
 * <p>
 * Для каждой внешней системы создаётся свой consumer с отдельным groupId.
 * Внутри каждого тика шедуллера мы:
 * <ul>
 *     <li>ограничиваем количество обрабатываемых сообщений значением RPS для системы;</li>
 *     <li>сначала обрабатываем сообщения с высоким приоритетом;</li>
 *     <li>если лимит RPS ещё не выбран — добираем сообщения с низким приоритетом.</li>
 * </ul>
 */
@Service
public class PriorityPollingService {

    private static final Logger log = LoggerFactory.getLogger(PriorityPollingService.class);

    private final ConsumerFactory<String, String> consumerFactory;
    private final ProcessingProperties properties;
    private final TopicNameResolver topicNameResolver;
    private final TaskScheduler taskScheduler;
    private final MeterRegistry meterRegistry;

    /**
     * Быстрые (quick) consumer-ы на каждую систему.
     */
    private final Map<String, KafkaConsumer<String, String>> quickConsumersBySystem = new ConcurrentHashMap<>();

    /**
     * Медленные (slow) consumer-ы на каждую систему.
     */
    private final Map<String, KafkaConsumer<String, String>> slowConsumersBySystem = new ConcurrentHashMap<>();

    /**
     * Счётчики обработанных сообщений (для Prometheus) по системе и приоритету.
     */
    private final Map<String, Counter> quickCountersBySystem = new ConcurrentHashMap<>();
    private final Map<String, Counter> slowCountersBySystem = new ConcurrentHashMap<>();

    /**
     * Gauge: сколько сообщений было обработано за последний тик для каждой системы.
     */
    private final Map<String, AtomicInteger> lastProcessedPerSystem = new ConcurrentHashMap<>();

    public PriorityPollingService(ConsumerFactory<String, String> consumerFactory,
                                  ProcessingProperties properties,
                                  TopicNameResolver topicNameResolver,
                                  TaskScheduler taskScheduler,
                                  MeterRegistry meterRegistry) {
        this.consumerFactory = consumerFactory;
        this.properties = properties;
        this.topicNameResolver = topicNameResolver;
        this.taskScheduler = taskScheduler;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    void init() {
        initConsumers();
        initSchedulers();
    }

    /**
     * Инициализация по два {@link KafkaConsumer} на каждую систему:
     * один для quick-топика, один для slow-топика.
     * <p>
     * Каждый consumer использует стабильный groupId, чтобы несколько
     * экземпляров сервиса делили партиции и не дублировали сообщения.
     */
    private void initConsumers() {
        if (properties.getSystems() == null || properties.getSystems().isEmpty()) {
            log.warn("No processing systems configured. Nothing will be polled from Kafka.");
            return;
        }

        for (SystemConfig system : properties.getSystems()) {
            String systemId = system.getId();
            if (systemId == null || systemId.isBlank()) {
                continue;
            }

            String quickTopic = topicNameResolver.quickTopic(systemId);
            String slowTopic = topicNameResolver.slowTopic(systemId);

            String quickGroupId = "processing-quick-" + systemId;
            String slowGroupId = "processing-slow-" + systemId;

            String quickClientIdPrefix = "processing-client-quick-" + systemId;
            String slowClientIdPrefix = "processing-client-slow-" + systemId;

            @SuppressWarnings("unchecked")
            KafkaConsumer<String, String> quickConsumer =
                    (KafkaConsumer<String, String>) consumerFactory.createConsumer(quickGroupId, quickClientIdPrefix, null);
            quickConsumer.subscribe(java.util.Collections.singletonList(quickTopic));

            @SuppressWarnings("unchecked")
            KafkaConsumer<String, String> slowConsumer =
                    (KafkaConsumer<String, String>) consumerFactory.createConsumer(slowGroupId, slowClientIdPrefix, null);
            slowConsumer.subscribe(java.util.Collections.singletonList(slowTopic));

            quickConsumersBySystem.put(systemId, quickConsumer);
            slowConsumersBySystem.put(systemId, slowConsumer);

            log.info("Initialized Kafka consumers for system {} quickTopic={} slowTopic={}",
                    systemId, quickTopic, slowTopic);

            // Метрики: конфигурированный RPS на систему
            Gauge.builder("processing_system_rps_configured", system, SystemConfig::getRps)
                    .description("Configured max messages per scheduler tick (RPS) for system")
                    .tag("system", systemId)
                    .register(meterRegistry);

            // Метрики: сколько обработано в последний тик
            AtomicInteger lastProcessed = new AtomicInteger(0);
            lastProcessedPerSystem.put(systemId, lastProcessed);
            Gauge.builder("processing_system_last_tick_processed", lastProcessed, AtomicInteger::get)
                    .description("Number of messages processed in last scheduler tick for system")
                    .tag("system", systemId)
                    .register(meterRegistry);

            // Счётчики: сколько всего обработано сообщений по системе и приоритету
            Counter quickCounter = Counter.builder("processing_system_messages_total")
                    .description("Total messages processed by scheduler per system and priority")
                    .tag("system", systemId)
                    .tag("priority", "quick")
                    .register(meterRegistry);
            Counter slowCounter = Counter.builder("processing_system_messages_total")
                    .description("Total messages processed by scheduler per system and priority")
                    .tag("system", systemId)
                    .tag("priority", "slow")
                    .register(meterRegistry);

            quickCountersBySystem.put(systemId, quickCounter);
            slowCountersBySystem.put(systemId, slowCounter);
        }
    }

    /**
     * Динамически создаёт по одному шедуллеру на каждую систему.
     * Каждый шедуллер с фиксированным интервалом вызывает pollAndProcessForSystem(..)
     * только для своей системы.
     */
    private void initSchedulers() {
        if (properties.getSystems() == null || properties.getSystems().isEmpty()) {
            return;
        }
        long intervalMs = properties.getPollIntervalMs();
        for (SystemConfig system : properties.getSystems()) {
            taskScheduler.scheduleAtFixedRate(
                    () -> {
                        try {
                            pollAndProcessForSystem(system);
                        } catch (Exception ex) {
                            log.error("Error during polling for system {}", system.getId(), ex);
                        }
                    },
                    java.time.Duration.ofMillis(intervalMs)
            );
            log.info("Scheduled polling task for system {} with interval {} ms", system.getId(), intervalMs);
        }
    }

    /**
     * Чтение и обработка сообщений для одной конкретной системы с учётом её RPS.
     *
     * @param system конфигурация системы (id и RPS)
     */
    private void pollAndProcessForSystem(SystemConfig system) {
        KafkaConsumer<String, String> quickConsumer = quickConsumersBySystem.get(system.getId());
        KafkaConsumer<String, String> slowConsumer = slowConsumersBySystem.get(system.getId());

        if (quickConsumer == null || slowConsumer == null) {
            return;
        }
        // Максимальное количество сообщений, которое можно обработать за один тик шедуллера
        // для данной системы (по сути "RPS" в рамках интервала poll-interval-ms).
        int capacity = system.getRps();
        if (capacity <= 0) {
            return;
        }

        // Счётчик, сколько сообщений уже обработано в рамках текущего тика для системы.
        int processed = 0;

        // Сначала выбираем сообщения с высоким приоритетом, пока не исчерпан лимит RPS.
        ConsumerRecords<String, String> quickRecords = quickConsumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : quickRecords) {
            if (processed >= capacity) {
                break;
            }
            handleRecord(system, record, true);
            Counter quickCounter = quickCountersBySystem.get(system.getId());
            if (quickCounter != null) {
                quickCounter.increment();
            }
            processed++;
        }

        // Если после high-priority ещё остался запас по RPS,
        // добираем его сообщениями с низким приоритетом из отдельного топика.
        if (processed < capacity) {
            ConsumerRecords<String, String> slowRecords = slowConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : slowRecords) {
                if (processed >= capacity) {
                    break;
                }
                handleRecord(system, record, false);
                Counter slowCounter = slowCountersBySystem.get(system.getId());
                if (slowCounter != null) {
                    slowCounter.increment();
                }
                processed++;
            }
        }

        AtomicInteger lastProcessed = lastProcessedPerSystem.get(system.getId());
        if (lastProcessed != null) {
            lastProcessed.set(processed);
        }

        log.debug("System {} processed {} messages in this tick (capacity={})",
                system.getId(), processed, capacity);
    }

    private void handleRecord(SystemConfig system,
                              ConsumerRecord<String, String> record,
                              boolean highPriority) {
        // Здесь можно вызывать внешние системы и т.п.
        log.info(
                "System={} priority={} partition={} offset={} key={} value={}",
                system.getId(),
                highPriority ? "HIGH" : "LOW",
                record.partition(),
                record.offset(),
                record.key(),
                record.value()
        );
    }
}


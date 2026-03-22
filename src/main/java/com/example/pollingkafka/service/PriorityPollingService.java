package com.example.pollingkafka.service;

import com.example.pollingkafka.config.ProcessingProperties;
import com.example.pollingkafka.config.ProcessingProperties.SystemConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
 * <p>
 * Офсеты: {@code enable.auto.commit=false}. После quick/slow {@code poll}: один раз в конце батча —
 * {@link KafkaConsumer#commitSync(java.util.Map)} только по <b>непрерывному префиксу</b> обработанных offset’ов
 * (с минимального offset’а в батче по партиции) и {@code seek} к первому необработанному — иначе хвост батча теряется.
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

    private void pollAndProcessForSystem(SystemConfig system) {

        KafkaConsumer<String, String> quickConsumer =
                quickConsumersBySystem.get(system.getId());

        KafkaConsumer<String, String> slowConsumer =
                slowConsumersBySystem.get(system.getId());

        if (quickConsumer == null || slowConsumer == null) {
            return;
        }

        int capacity = system.getRps();
        if (capacity <= 0) {
            return;
        }

        int processed = 0;

        // ---------- QUICK ----------

        ConsumerRecords<String, String> quickRecords =
                quickConsumer.poll(Duration.ofMillis(100));

        Map<TopicPartition, Set<Long>> quickProcessed = new HashMap<>();
        for (ConsumerRecord<String, String> record : quickRecords) {
            if (processed >= capacity) {
                break;
            }
            if (!handleRecordSafe(system, record, true)) {
                break;
            }
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            quickProcessed.computeIfAbsent(tp, k -> new HashSet<>()).add(record.offset());
            Counter qc = quickCountersBySystem.get(system.getId());
            if (qc != null) {
                qc.increment();
            }
            processed++;
        }
        commitAfterPartialBatch(quickConsumer, quickRecords, quickProcessed);

        // ---------- SLOW ----------

        if (processed < capacity) {
            ConsumerRecords<String, String> slowRecords =
                    slowConsumer.poll(Duration.ofMillis(100));

            Map<TopicPartition, Set<Long>> slowProcessed = new HashMap<>();
            for (ConsumerRecord<String, String> record : slowRecords) {
                if (processed >= capacity) {
                    break;
                }
                if (!handleRecordSafe(system, record, false)) {
                    break;
                }
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                slowProcessed.computeIfAbsent(tp, k -> new HashSet<>()).add(record.offset());
                Counter sc = slowCountersBySystem.get(system.getId());
                if (sc != null) {
                    sc.increment();
                }
                processed++;
            }
            commitAfterPartialBatch(slowConsumer, slowRecords, slowProcessed);
        }

        AtomicInteger lastProcessed =
                lastProcessedPerSystem.get(system.getId());

        if (lastProcessed != null) {
            lastProcessed.set(processed);
        }
    }

    /**
     * Один коммит на батч: только непрерывный префикс обработанных offset’ов с начала батча по партиции;
     * затем {@code seek} на первый необработанный в батче (позиция после {@code poll} уже за хвостом).
     */
    private void commitAfterPartialBatch(KafkaConsumer<String, String> consumer,
                                         ConsumerRecords<String, String> records,
                                         Map<TopicPartition, Set<Long>> processedOffsets) {
        if (records == null || records.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            var partRecords = records.records(tp);
            if (partRecords == null || partRecords.isEmpty()) {
                continue;
            }
            TreeSet<Long> batchOffsets = new TreeSet<>();
            for (ConsumerRecord<String, String> r : partRecords) {
                batchOffsets.add(r.offset());
            }
            Set<Long> proc = processedOffsets.getOrDefault(tp, Set.of());

            long first = batchOffsets.first();
            long last = batchOffsets.last();
            long nextCommitExclusive = first;
            for (long o = first; o <= last; o++) {
                if (!batchOffsets.contains(o)) {
                    break;
                }
                if (!proc.contains(o)) {
                    break;
                }
                nextCommitExclusive = o + 1;
            }
            if (nextCommitExclusive > first) {
                commits.put(tp, new OffsetAndMetadata(nextCommitExclusive));
            }
            for (long o : batchOffsets) {
                if (!proc.contains(o)) {
                    consumer.seek(tp, o);
                    break;
                }
            }
        }
        if (!commits.isEmpty()) {
            try {
                consumer.commitSync(commits);
            } catch (Exception ex) {
                log.error("commitSync failed after poll batch", ex);
            }
        }
    }

    private boolean handleRecordSafe(SystemConfig system,
                                     ConsumerRecord<String, String> record,
                                     boolean highPriority) {
        try {
            handleRecord(system, record, highPriority);
            return true;
        } catch (Exception ex) {
            log.error("handleRecord failed system={} offset={}", system.getId(), record.offset(), ex);
            return false;
        }
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


package com.example.pollingkafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Конфигурация обработки сообщений.
 * <p>
 * Настройки задаются в {@code application.yml} в секции {@code processing}.
 * Здесь описаны:
 * <ul>
 *     <li>имя входного топика с "сырыми" сообщениями (incomingTopic);</li>
 *     <li>groupId для общего listener-а (incomingGroupId);</li>
 *     <li>шаблон имён выходных топиков для систем и приоритетов (baseTopic);</li>
 *     <li>период опроса Kafka шедуллерами (pollIntervalMs);</li>
 *     <li>имена заголовков (для системы и приоритета);</li>
 *     <li>значение, обозначающее "высокий приоритет" (quick/slow по заголовку);</li>
 *     <li>набор систем с отдельным лимитом RPS для каждой.</li>
 * </ul>
 */
@Component
@ConfigurationProperties(prefix = "processing")
public class ProcessingProperties {

    /**
     * Входной общий топик, из которого читает {@code @KafkaListener}.
     * Например: {@code processing-all}.
     */
    private String incomingTopic;

    /**
     * GroupId для общего listener-а, читающего {@link #incomingTopic}.
     * Например: {@code b-listen-all}.
     */
    private String incomingGroupId;

    /**
     * Базовое имя для выходных топиков,
     * на основе которого формируются имена вида:
     * {@code <baseTopic>.system-a-quick}, {@code <baseTopic>.system-a-slow} и т.д.
     * Например: {@code processing-all}.
     */
    private String baseTopic;

    private long pollIntervalMs = 1000L;
    private Headers headers = new Headers();
    private String highPriorityValue = "quick";
    private List<SystemConfig> systems;

    public String getIncomingTopic() {
        return incomingTopic;
    }

    public void setIncomingTopic(String incomingTopic) {
        this.incomingTopic = incomingTopic;
    }

    public String getIncomingGroupId() {
        return incomingGroupId;
    }

    public void setIncomingGroupId(String incomingGroupId) {
        this.incomingGroupId = incomingGroupId;
    }

    public String getBaseTopic() {
        return baseTopic;
    }

    public void setBaseTopic(String baseTopic) {
        this.baseTopic = baseTopic;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public Headers getHeaders() {
        return headers;
    }

    public void setHeaders(Headers headers) {
        this.headers = headers;
    }

    public String getHighPriorityValue() {
        return highPriorityValue;
    }

    public void setHighPriorityValue(String highPriorityValue) {
        this.highPriorityValue = highPriorityValue;
    }

    public List<SystemConfig> getSystems() {
        return systems;
    }

    public void setSystems(List<SystemConfig> systems) {
        this.systems = systems;
    }

    /**
     * Настройки заголовков, по которым фильтруются сообщения.
     */
    public static class Headers {
        private String systemId = "X-System-Id";
        private String priority = "X-Priority";

        public String getSystemId() {
            return systemId;
        }

        public void setSystemId(String systemId) {
            this.systemId = systemId;
        }

        public String getPriority() {
            return priority;
        }

        public void setPriority(String priority) {
            this.priority = priority;
        }
    }

    /**
     * Конфигурация одной внешней системы, для которой нужно читать сообщения.
     */
    public static class SystemConfig {
        private String id;
        /**
         * Максимальное количество сообщений в секунду (RPS) для данной системы.
         * Фактически — лимит на количество обработанных сообщений
         * за один запуск шедуллера при значении poll-interval-ms = 1000 мс.
         */
        private int rps;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getRps() {
            return rps;
        }

        public void setRps(int rps) {
            this.rps = rps;
        }
    }
}


package com.example.pollingkafka.service;

import com.example.pollingkafka.config.ProcessingProperties;
import com.example.pollingkafka.config.ProcessingProperties.SystemConfig;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Утилита для построения имён топиков по системе и приоритету.
 *
 * Формат:
 *   <baseTopic>.system-a-quick
 *   <baseTopic>.system-a-slow
 *   ...
 */
@Component
public class TopicNameResolver {

    private final ProcessingProperties properties;
    private final Set<String> knownSystems;

    public TopicNameResolver(ProcessingProperties properties) {
        this.properties = properties;
        this.knownSystems = properties.getSystems() == null
                ? Set.of()
                : properties.getSystems()
                            .stream()
                            .map(SystemConfig::getId)
                            .collect(Collectors.toSet());
    }

    public boolean isKnownSystem(String systemCode) {
        return systemCode != null && knownSystems.contains(systemCode);
    }

    public String quickTopic(String systemCode) {
        return buildTopic(systemCode, true);
    }

    public String slowTopic(String systemCode) {
        return buildTopic(systemCode, false);
    }

    private String buildTopic(String systemCode, boolean quick) {
        String base = properties.getBaseTopic();
        if (base == null || base.isBlank()) {
            base = "";
        }
        String systemPart = normalizeSystem(systemCode);
        String priorityPart = quick ? "quick" : "slow";
        return base + "." + systemPart + "-" + priorityPart;
    }

    private String normalizeSystem(String systemCode) {
        if (systemCode == null) {
            return "unknown";
        }
        // SYSTEM_A -> system-a
        return systemCode.toLowerCase(Locale.ROOT).replace('_', '-');
    }
}


package com.huawei.bigdata.flink.examples.cep;

import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.PatternProcessorDiscovererTableFactory;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/* * Factory for {@link FromKafkaPatternProcessorDiscoverer}. */
public class FromKafkaPatternProcessorDiscovererFactory implements PatternProcessorDiscovererTableFactory {
    public static final String BOOTSTRAP_SERVERS_KEY = "from-kafka-pattern-discoverer.bootstrap.servers";
    public static final String GROUP_ID_KEY = "from-kafka-pattern-discoverer.group.id";
    public static final String TOPIC_KEY = "from-kafka-pattern-discoverer.topic";
    public static final ConfigOption<String> BOOTSTRAP_SERVERS_OPTION =
            ConfigOptions.key(BOOTSTRAP_SERVERS_KEY).stringType().noDefaultValue();
    public static final ConfigOption<String> GROUP_ID_OPTION =
            ConfigOptions.key(GROUP_ID_KEY).stringType().noDefaultValue();
    public static final ConfigOption<String> TOPIC_OPTION = ConfigOptions.key(TOPIC_KEY).stringType().noDefaultValue();

    @Override
    public PatternProcessorDiscoverer<RowData> createPatternProcessorDiscoverer(
            ClassLoader userCodeClassloader, Configuration config) throws Exception {
        return new FromKafkaPatternProcessorDiscoverer(
                config.getString(BOOTSTRAP_SERVERS_OPTION),
                config.getString(GROUP_ID_OPTION),
                config.getString(TOPIC_OPTION));
    }

    @Override
    public String factoryIdentifier() {
        return "from-kafka-pattern-discoverer";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(BOOTSTRAP_SERVERS_OPTION, GROUP_ID_OPTION, TOPIC_OPTION).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}

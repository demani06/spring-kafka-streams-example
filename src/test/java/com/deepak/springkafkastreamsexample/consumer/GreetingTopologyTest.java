package com.deepak.springkafkastreamsexample.consumer;

import com.deepak.springkafkastreamsexample.topology.GreeterTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class GreetingTopologyTest {

    private TestInputTopic<Void, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private TopologyTestDriver testDriver;

    @BeforeEach
    void setup(){

        final StreamsBuilder builder = configureTopology();
        Properties props = buildStreamDummyConfiguration();

        testDriver = new TopologyTestDriver(
                builder.build(),
                props
        );
        inputTopic = testDriver.createInputTopic("users",
                Serdes.Void().serializer(),
                Serdes.String().serializer()
        );
        outputTopic = testDriver.createOutputTopic("greetings",
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        );



    }
/*
    @BeforeEach
    void setup() {
        // build the topology
        Topology topology = GreeterTopology.build();

        // create a test driver. we will use this to pipe data to our topology
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(topology, props);

        // create the test input topic
        inputTopic =
                testDriver.createInputTopic(
                        "users", Serdes.Void().serializer(), Serdes.String().serializer());

        // create the test output topic
        outputTopic =
                testDriver.createOutputTopic(
                        "greetings", Serdes.Void().deserializer(), Serdes.String().deserializer());
    }*/


    private StreamsBuilder configureTopology() {
        final GreetingStreamsConsumer application = new GreetingStreamsConsumer();
        final StreamsBuilder builder = new StreamsBuilder();
        application.greeting(builder);
        return builder;
    }

    private Properties buildStreamDummyConfiguration() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.Serdes$StringSerde");
        return props;
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    @DisplayName("greetings topic should contain expected greeting")
    void testUsersGreeted() {
        // pipe the test record to our Kafka topic
        String value = "Izzy";

        inputTopic.pipeInput(value);

        assertThat(outputTopic.isEmpty()).isFalse();

        // save each record that appeared in the output topic to a list
        List<TestRecord<String, String>> outRecords = outputTopic.readRecordsToList();

        // ensure the output topic contains exactly one record
        assertThat(outRecords).hasSize(1);

        // ensure the generated greeting is the expected value
        String greeting = outRecords.get(0).getValue();
        assertThat(greeting).isEqualTo("Hello Izzy");
    }
}

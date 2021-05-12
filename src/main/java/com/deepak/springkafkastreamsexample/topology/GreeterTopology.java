package com.deepak.springkafkastreamsexample.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreeterTopology {

  public static String generateGreeting(String user) {
    return String.format("Hello %s", user);
  }

  public static Topology build() {
    StreamsBuilder builder = new StreamsBuilder();

    builder
        // stream data from the users topic
        .stream("users", Consumed.with(Serdes.String(), Serdes.String()))
        // don't greet Randy. we're mad at Randy
        .filterNot((key, value) -> value.toLowerCase().equals("randy"))
        // generate greetings for everyone except Randy
        .mapValues(GreeterTopology::generateGreeting)
        // write all of the greetings to the `greetings` topic
        .to("greetings", Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }

}

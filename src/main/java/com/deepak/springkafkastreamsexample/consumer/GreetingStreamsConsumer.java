package com.deepak.springkafkastreamsexample.consumer;

import com.deepak.springkafkastreamsexample.topology.GreeterTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GreetingStreamsConsumer {

    @Autowired
    public KStream<String,String> greeting(StreamsBuilder streamsBuilder){

        System.out.println("greetingStream running ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");

        var stringSerde = new Serdes.StringSerde();

        final KStream<String,String> greetingStream =  streamsBuilder
                // stream data from the users topic
                .stream("users", Consumed.with(Serdes.String(), Serdes.String()))
                // don't greet Randy. we're mad at Randy
                .filterNot((key, value) -> value.toLowerCase().equals("randy"))
                // generate greetings for everyone except Randy
                .mapValues(GreeterTopology::generateGreeting);

        greetingStream.to("greetings", Produced.with(Serdes.String(), stringSerde));

        return greetingStream;
    }
}

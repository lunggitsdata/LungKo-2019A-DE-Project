package com.kosmos.analytics;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ThroughputCheck {
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-stream-thruputcheck");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "10.0.0.5:9092,10.0.0.6:9092,10.0.0.7:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://10.0.0.6:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> lines = builder.stream("testinput");

        final SpecificAvroSerde<Transaction> transactionSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://10.0.0.6:8081");
        transactionSerde.configure(serdeConfig, false);

        KStream<String, String[]>[] buyOrSell = lines.mapValues(v -> v.split("\\^"))
                .branch((k,v)->(Double.parseDouble(v[11]) >= Double.parseDouble(v[8])),
                        (k,v)->(Double.parseDouble(v[11]) < Double.parseDouble(v[8])));

        buyOrSell[0]  // buy
                .mapValues((v)-> new String[] { v[1], v[6], v[7], v[8],
                        String.valueOf((int)(
                                1000*(Double.parseDouble(v[11])- Double.parseDouble(v[8]))/Double.parseDouble(v[8])))
                })
                //.filter((k,v)-> (Double.parseDouble(v[4]) > 0))
                // Transaction(symbol, date, time, price, shares)
                .mapValues(v -> new Transaction(v[0], v[1], v[2], Double.parseDouble(v[3]), Integer.parseInt(v[4])))
                .to("stocksoutput", Produced.with(Serdes.String(), transactionSerde));

        buyOrSell[1]  // sell
                .mapValues(v -> new Transaction(v[1], v[6], v[7], Double.parseDouble(v[8]), -1))
                .to("stocksoutput", Produced.with(Serdes.String(), transactionSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("stock-stream-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }
}

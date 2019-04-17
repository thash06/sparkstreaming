package com.thash.streaming;

import com.google.gson.Gson;
import com.thash.streaming.model.MeetupSchemaType;
import com.thash.streaming.model.MeetupStruct;
import com.thash.streaming.util.Combiner;
import com.thash.streaming.util.MeetupOffsetCommitCallback;
import com.thash.streaming.util.SchemaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;
import java.util.logging.Logger;

import static com.thash.streaming.model.MeetupSchemaType.PHOTO;
import static com.thash.streaming.model.MeetupSchemaType.RSVP;

/**
 * @author tarhashm
 */
public class SparkStreamingApplication {
    private static final Logger log = Logger.getLogger(SparkStreamingApplication.class.getName());
    private static final String HADOOP_HOME_DIR_VALUE = "/Volumes/Data-1/study/hadoop-2.6.0";
    //"C:/winutils";

    private static final String MEETUP_VIEW = "meetup_view";

    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";
    private static final String APPLICATION_NAME = "Kafka <- Spark(Dataset) -> MongoDb";
    private static final String CASE_SENSITIVE = "false";

    private static final int BATCH_DURATION_INTERVAL_MS = 5000;

    private static final Map<String, Object> KAFKA_CONSUMER_PROPERTIES;

    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_OFFSET_RESET_TYPE = "latest";
    private static final String KAFKA_GROUP = "meetupGroup";
    private static final String KAFKA_TOPIC = "meetupTopic";
    private static final Collection<String> TOPICS =
            Collections.unmodifiableList(Arrays.asList(KAFKA_TOPIC));

    static {
        Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP);
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_OFFSET_RESET_TYPE);
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KAFKA_CONSUMER_PROPERTIES = Collections.unmodifiableMap(kafkaProperties);

    }

    public static void main(String[] args) throws InterruptedException {

        System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR_VALUE);

        SparkConf conf = new SparkConf()
                .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                .setAppName(APPLICATION_NAME)
                .set("spark.sql.caseSensitive", CASE_SENSITIVE);
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf,
                new Duration(BATCH_DURATION_INTERVAL_MS));

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        sparkSession.sparkContext().setLogLevel("WARN");

        JavaInputDStream<ConsumerRecord<String, String>> meetupStream =
                KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(TOPICS, KAFKA_CONSUMER_PROPERTIES));

        JavaDStream<String> meetupStreamValues = meetupStream.map(ConsumerRecord::value);

        meetupStreamValues.foreachRDD((JavaRDD<String> meetupRDD) -> {
            if (!meetupRDD.isEmpty()) {

                JavaRDD<MeetupStruct> meetupStructJavaRDD = meetupRDD.mapPartitions(
                        (FlatMapFunction<Iterator<String>, MeetupStruct>) matrixEntryIterator -> {
                            List<MeetupStruct> newLowerEntries = new ArrayList<>();
                            //todo figure out how to not create Gson for all partitions
                            Gson localGson = new Gson();
                            while (matrixEntryIterator.hasNext()) {
                                String json = matrixEntryIterator.next();

                                Map flattenedJsonMap = localGson.fromJson(json, Map.class);
                                log.info("--Size of map: " + flattenedJsonMap.size());
                                log.info("--Map keys: " + flattenedJsonMap.keySet());
                                log.info("--Map values: " + flattenedJsonMap.values());
                                MeetupSchemaType schemaType = SchemaUtils.getSchemaType(flattenedJsonMap.keySet());
                                log.info("--Schema Type: " + schemaType);
                                if (RSVP == schemaType) {
                                    newLowerEntries.add(new MeetupStruct(RSVP, json));
                                } else if (MeetupSchemaType.COMMENT == schemaType) {
                                    newLowerEntries.add(new MeetupStruct(MeetupSchemaType.COMMENT, json));
                                } else {
                                    newLowerEntries.add(new MeetupStruct(PHOTO, json));
                                }

                            }

                            return newLowerEntries.iterator();
                        });


                Combiner<MeetupStruct> combine = new Combiner<>();
                JavaPairRDD<MeetupSchemaType, Tuple2<MeetupSchemaType, List<MeetupStruct>>> meetupSchemaTypeTuple2JavaPairRDD = meetupStructJavaRDD
                        .mapToPair(meetupStruct -> new Tuple2<>(meetupStruct.getSchemaType(), meetupStruct))
                        .combineByKey(combine.createGroup, combine.mergeElement, combine.mergeCombiner);
                System.out.println(" \n\n######### New Batch ########### ");
                meetupSchemaTypeTuple2JavaPairRDD.foreach(meetupSchemaTypeTuple2Tuple2 -> {
                    System.out.println("\nkey : " + meetupSchemaTypeTuple2Tuple2._1 + " Value: " + meetupSchemaTypeTuple2Tuple2._2);
                });


                Map<MeetupSchemaType, Tuple2<MeetupSchemaType, List<MeetupStruct>>> rsvpTuples =
                        meetupSchemaTypeTuple2JavaPairRDD
                                .filter(meetupSchemaTypeTuple2Tuple2 -> meetupSchemaTypeTuple2Tuple2._1 == PHOTO)
                                .collectAsMap();
                rsvpTuples.forEach((meetupSchemaType, meetupSchemaTypeListTuple2) -> System.out.println("Key: " + meetupSchemaTypeListTuple2._1 + " Values: " + meetupSchemaTypeListTuple2._2));

            }
        });

        // some time later, after outputs have completed
        meetupStream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> meetupRDD) -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) meetupRDD.rdd()).offsetRanges();

            ((CanCommitOffsets) meetupStream.inputDStream())
                    .commitAsync(offsetRanges, new MeetupOffsetCommitCallback());
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}




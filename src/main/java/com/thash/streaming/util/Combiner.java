package com.thash.streaming.util;

import com.thash.streaming.model.MeetupSchemaType;
import com.thash.streaming.model.MeetupStruct;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author tarhashm
 */
public class Combiner<T> implements Serializable {

    // function 1: create a combiner data structure
    public final Function<MeetupStruct, Tuple2<MeetupSchemaType, List<MeetupStruct>>> createGroup =
            (Function<MeetupStruct, Tuple2<MeetupSchemaType, List<MeetupStruct>>>) meetupStruct -> new Tuple2<>(meetupStruct.getSchemaType(), Arrays.asList(meetupStruct));

    // function 2: merge a value into a combined data structure
    public final Function2<Tuple2<MeetupSchemaType, List<MeetupStruct>>, MeetupStruct, Tuple2<MeetupSchemaType, List<MeetupStruct>>> mergeElement
            = (Function2<Tuple2<MeetupSchemaType, List<MeetupStruct>>, MeetupStruct, Tuple2<MeetupSchemaType, List<MeetupStruct>>>) (meetupSchemaTypeListTuple2, meetupStruct) -> {
        List<MeetupStruct> meetupStructs = new ArrayList<>(meetupSchemaTypeListTuple2._2);
        meetupStructs.add(meetupStruct);
        return new Tuple2<>(meetupStruct.getSchemaType(), meetupStructs);
    };


    // function 3: merge two combiner data structures
    public final Function2<Tuple2<MeetupSchemaType, List<MeetupStruct>>, Tuple2<MeetupSchemaType, List<MeetupStruct>>, Tuple2<MeetupSchemaType, List<MeetupStruct>>> mergeCombiner =
            (Function2<Tuple2<MeetupSchemaType, List<MeetupStruct>>, Tuple2<MeetupSchemaType, List<MeetupStruct>>, Tuple2<MeetupSchemaType, List<MeetupStruct>>>) (meetupSchemaTypeListTuple2One, meetupSchemaTypeListTuple2Two) -> {
                List<MeetupStruct> meetupStructs = new ArrayList<>(meetupSchemaTypeListTuple2One._2);
                meetupStructs.addAll(meetupSchemaTypeListTuple2Two._2);

                return new Tuple2<>(meetupSchemaTypeListTuple2One._1, meetupStructs);
            };
}

package com.thash.streaming.util;

import com.google.common.collect.Sets;
import com.thash.streaming.model.MeetupSchemaType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.logging.Logger;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * @author tarhashm
 */
public class SchemaUtils {
    private static final Logger LOGGER = Logger.getLogger(SchemaUtils.class.getName());
    private static Set<String> MEETUP_PHOTO_ENDPOINT_KEYS = Sets.newHashSet("photo_album", "highres_link", "photo_id", "visibility", "member", "ctime", "photo_link", "mtime", "thumb_link");
    private static Set<String> MEETUP_EVENT_COMMENT_KEYS = Sets.newHashSet("visibility", "member", "comment", "id", "mtime", "event", "table_name", "group", "in_reply_to", "status");
    private static Set<String> MEETUP_EVENT_RSVP_KEYS = Sets.newHashSet("venue", "visibility", "response", "guests", "member", "rsvp_id", "mtime", "event", "group");

    public static final StructType RSVP_SCHEMA = new StructType()
            .add("venue",
                    new StructType()
                            .add("venue_name", StringType, true)
                            .add("lon", DoubleType, true)
                            .add("lat", DoubleType, true)
                            .add("venue_id", LongType, true))
            .add("visibility", StringType, true)
            .add("response", StringType, true)
            .add("guests", LongType, true)
            .add("member",
                    new StructType()
                            .add("member_id", LongType, true)
                            .add("photo", StringType, true)
                            .add("member_name", StringType, true))
            .add("rsvp_id", LongType, true)
            .add("mtime", LongType, true)
            .add("event",
                    new StructType()
                            .add("event_name", StringType, true)
                            .add("event_id", StringType, true)
                            .add("time", LongType, true)
                            .add("event_url", StringType, true))
            .add("group",
                    new StructType()
                            .add("group_city", StringType, true)
                            .add("group_country", StringType, true)
                            .add("group_id", LongType, true)
                            .add("group_lat", DoubleType, true)
                            .add("group_long", DoubleType, true)
                            .add("group_name", StringType, true)
                            .add("group_state", StringType, true)
                            .add("group_topics", DataTypes.createArrayType(
                                    new StructType()
                                            .add("topicName", StringType, true)
                                            .add("urlkey", StringType, true)), true)
                            .add("group_urlname", StringType, true));

    public static MeetupSchemaType getSchemaType(Set<String> keys) {
        Collection<String> missingFromPhotoEndPoint = Sets.difference(keys, MEETUP_PHOTO_ENDPOINT_KEYS);
        Collection<String> missingFromCommentEndPoint = Sets.difference(keys, MEETUP_EVENT_COMMENT_KEYS);
        Collection<String> missingFromRsvpEndPoint = Sets.difference(keys, MEETUP_EVENT_RSVP_KEYS);

        EnumMap<MeetupSchemaType, Double> schemasWithPercentage = new EnumMap<>(MeetupSchemaType.class);
        double photoPercentage = Math.abs((missingFromPhotoEndPoint.size() * 100.0 / keys.size()));
        double commentPercentage = Math.abs((missingFromCommentEndPoint.size() * 100.0 / keys.size()));
        double rsvpPercentage = Math.abs((missingFromRsvpEndPoint.size() * 100.0 / keys.size()));

        schemasWithPercentage.put(MeetupSchemaType.PHOTO, photoPercentage);
        schemasWithPercentage.put(MeetupSchemaType.COMMENT, commentPercentage);
        schemasWithPercentage.put(MeetupSchemaType.RSVP, rsvpPercentage);
        Optional<MeetupSchemaType> meetupSchemaType = schemasWithPercentage.entrySet().stream().min(Map.Entry.comparingByValue()).map(Map.Entry::getKey);
        LOGGER.info("KeySize: " + keys.size() + " Photo: " + missingFromPhotoEndPoint.size() + " Comment: " + missingFromCommentEndPoint.size() + " Rsvp: " + missingFromRsvpEndPoint.size());
        LOGGER.info("Photo: " + photoPercentage + " Comment: " + commentPercentage + " Rsvp: " + rsvpPercentage);
        return meetupSchemaType.get();
    }
}

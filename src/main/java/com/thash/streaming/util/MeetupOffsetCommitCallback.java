package com.thash.streaming.util;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MeetupOffsetCommitCallback implements OffsetCommitCallback {

    private static final Logger log = Logger.getLogger(MeetupOffsetCommitCallback.class.getName());

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        log.info("---------------------------------------------------");
        log.log(Level.INFO, "{0} | {1}", new Object[]{offsets, exception});
        log.info("---------------------------------------------------");
    }
}

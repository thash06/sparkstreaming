package com.thash.streaming.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author tarhashm
 */
public class MeetupStruct implements Serializable {
    private MeetupSchemaType schemaType;
    private String json;

    public MeetupStruct(MeetupSchemaType schemaType, String json) {
        this.schemaType = schemaType;
        this.json = json;
    }

    public MeetupSchemaType getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(MeetupSchemaType schemaType) {
        this.schemaType = schemaType;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MeetupStruct that = (MeetupStruct) o;
        return schemaType == that.schemaType &&
                Objects.equals(json, that.json);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaType, json);
    }

    @Override
    public String toString() {
        return "MeetupStruct{" +
                "schemaType=" + schemaType +
                ", json='" + json + '\'' +
                '}';
    }
}

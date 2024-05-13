package com.github.spring.batch.model;

import java.io.Serializable;

/**
 * POJO to hold the ids.
 */
public class MessageKey implements Serializable {

    private final String id;
    private final String avroKey;

    public MessageKey(String id, String avroKey) {
        this.id = id;
        this.avroKey = avroKey;
    }

    public String getId() {
        return id;
    }

    public String getAvroKey() {
        return avroKey;
    }

}

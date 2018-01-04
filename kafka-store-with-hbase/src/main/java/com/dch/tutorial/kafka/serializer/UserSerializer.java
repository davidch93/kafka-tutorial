package com.dch.tutorial.kafka.serializer;

import com.dch.tutorial.kafka.model.User;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Serializer class for converting {@link User} to bytes.
 *
 * @author David.Christianto
 */
public class UserSerializer implements Serializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, User data) {
        try {
            return new ObjectMapper().writeValueAsBytes(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {
    }

}

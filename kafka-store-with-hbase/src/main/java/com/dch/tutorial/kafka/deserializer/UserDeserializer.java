package com.dch.tutorial.kafka.deserializer;

import com.dch.tutorial.kafka.model.User;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Deserializer class for converting {@link User} to bytes.
 *
 * @author David.Christianto
 */
public class UserDeserializer implements Deserializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public User deserialize(String topic, byte[] data) {
        try {
            return new ObjectMapper().readValue(data, User.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {
    }

}

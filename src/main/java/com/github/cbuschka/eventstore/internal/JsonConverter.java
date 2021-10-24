package com.github.cbuschka.eventstore.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

import java.io.IOException;

public class JsonConverter {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows(IOException.class)
    public <T> T fromJson(String json, Class<T> type) {
        return objectMapper.readerFor(type).readValue(json);
    }

    @SneakyThrows(IOException.class)
    public <T> T fromJson(JsonNode json, Class<T> type) {
        return objectMapper.readerFor(type).readValue(json);
    }

    @SneakyThrows(IOException.class)
    public String toJson(Object object) {
        return objectMapper.writeValueAsString(object);
    }
}

package com.github.cbuschka.eventstore;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.UUID;

@AllArgsConstructor
@Getter
public class PublishableEvent {

    private UUID uuid;

    private String type;

    private JsonNode data;
}

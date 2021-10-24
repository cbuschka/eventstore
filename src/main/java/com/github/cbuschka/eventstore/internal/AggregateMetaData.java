package com.github.cbuschka.eventstore.internal;

import com.github.cbuschka.eventstore.PublishableEvent;
import com.github.cbuschka.eventstore.annotations.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

public class AggregateMetaData {

    public boolean snaphotEnabled;
    public final Class<?> aggregateClass;
    public final Field aggregateUuidField;
    public final Map<String, EventMetaData> eventMap;
    public final Field aggregateVersionField;
    public final Field aggregateCorrelationUuidField;
    public final Field eventListField;
    public final String type;

    public static class EventMetaData {
        public final String type;
        public final Class<?> eventClass;
        public final Method handlerMethod;
        public final Field eventUuidField;

        public EventMetaData(Class<?> eventClass, String type, Method handlerMethod, Field eventUuidField) {
            this.eventClass = eventClass;
            this.type = type;
            this.handlerMethod = handlerMethod;
            this.eventUuidField = eventUuidField;
        }
    }

    public AggregateMetaData(Class<?> aggregateClass) {
        try {
            Aggregate anno = aggregateClass.getAnnotation(Aggregate.class);
            if (anno == null) {
                throw new NoSuchElementException("Aggregate has no annotation.");
            }

            this.type = !anno.type().isEmpty() ? anno.type() : aggregateClass.getName().substring(aggregateClass.getName().lastIndexOf('.') + 1);
            this.snaphotEnabled = anno.snapshotsEnabled();
            this.aggregateClass = aggregateClass;
            this.eventListField = getAnnotatedField(aggregateClass, EventList.class, List.class);
            this.aggregateUuidField = getAnnotatedField(aggregateClass, AggregateId.class, UUID.class);
            this.aggregateCorrelationUuidField = getOptionalAnnotatedField(aggregateClass, AggregateCorrelationId.class, UUID.class).orElse(null);
            this.aggregateVersionField = getAnnotatedField(aggregateClass, AggregateVersion.class, Integer.TYPE);
            this.eventMap = getEventHandlerMapFrom(aggregateClass);
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Map<String, EventMetaData> getEventHandlerMapFrom(Class<?> aggregateClass) throws NoSuchFieldException {
        Map<String, EventMetaData> eventClassWithMethodMap = new HashMap<>();
        for (Method method : aggregateClass.getDeclaredMethods()) {
            EventHandler anno = method.getAnnotation(EventHandler.class);
            if (anno != null) {
                Class<?> eventClass = method.getParameterTypes()[0];
                Event eventAnno = eventClass.getAnnotation(Event.class);
                if (eventAnno != null) {
                    String type = getEventType(eventClass);
                    Field eventUuidField = getAnnotatedField(eventClass, EventId.class, UUID.class);
                    eventUuidField.setAccessible(true);
                    method.setAccessible(true);
                    eventClassWithMethodMap.put(type, new EventMetaData(eventClass, type, method, eventUuidField));
                }
            }
        }

        return eventClassWithMethodMap;
    }

    public String getEventType(Class<?> eventClass) {
        Event eventAnno = eventClass.getAnnotation(Event.class);
        String name = eventAnno.type();
        if (name.isBlank()) {
            name = eventClass.getName().substring(eventClass.getName().lastIndexOf('.') + 1);
            if (name.endsWith("Event")) {
                name = name.substring(0, name.length() - "Event".length());
            }
        }

        return name;
    }

    private Optional<Field> getOptionalAnnotatedField(Class<?> aggregateClass, Class<? extends Annotation> annotationType, Class<?> expectedType) throws NoSuchFieldException {

        List<Field> fields = getAnnotatedFields(aggregateClass, annotationType, expectedType);
        if (fields.isEmpty()) {
            return Optional.empty();
        } else if (fields.size() > 1) {
            throw new NoSuchFieldException("Ambigious field annotated with " + annotationType.getName() + " in " + aggregateClass.getName() + ".");
        }

        return Optional.of(fields.get(0));
    }

    private Field getAnnotatedField(Class<?> aggregateClass, Class<? extends Annotation> annotationType, Class<?> expectedType) throws NoSuchFieldException {

        List<Field> fields = getAnnotatedFields(aggregateClass, annotationType, expectedType);
        if (fields.isEmpty()) {
            throw new NoSuchFieldException(aggregateClass.getName() + " has no field annotated with " + annotationType.getName());
        } else if (fields.size() > 1) {
            throw new NoSuchFieldException("Ambigious field annotated with " + annotationType.getName() + " in " + aggregateClass.getName() + ".");
        }

        return fields.get(0);
    }

    private List<Field> getAnnotatedFields(Class<?> aggregateClass, Class<? extends Annotation> annotationType, Class<?> expectedType) throws NoSuchFieldException {
        List<Field> fields = new ArrayList<>();
        for (Field field : aggregateClass.getDeclaredFields()) {
            Annotation anno = field.getAnnotation(annotationType);
            if (anno != null) {
                if (expectedType != null && !field.getType().isAssignableFrom(expectedType)) {
                    throw new NoSuchFieldException("field " + field.getName() + " in + " + aggregateClass.getName() + " is not assignable by " + expectedType.getName());
                }
                field.setAccessible(true);
                fields.add(field);
            }
        }

        return fields;
    }
}

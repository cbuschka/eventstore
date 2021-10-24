package com.github.cbuschka.eventstore.annotations;

import java.lang.annotation.*;

@Target(ElementType.FIELD)
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface EventList {
}

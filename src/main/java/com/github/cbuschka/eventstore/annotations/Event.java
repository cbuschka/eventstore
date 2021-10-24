package com.github.cbuschka.eventstore.annotations;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Event {
    String type() default "";
}

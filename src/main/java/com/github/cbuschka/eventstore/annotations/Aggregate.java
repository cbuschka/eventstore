package com.github.cbuschka.eventstore.annotations;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(ElementType.TYPE)
public @interface Aggregate {
    boolean snapshotsEnabled() default false;

    String type() default "";
}

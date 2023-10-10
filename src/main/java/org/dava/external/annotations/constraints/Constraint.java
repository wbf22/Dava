package org.dava.external.annotations.constraints;


import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Constraint {
    String sql() default "";
}

//    ALTER TABLE attractions.attraction
//    ADD CONSTRAINT attraction_check_attraction_name
//    CHECK ( (attraction_status = 'ACTIVE' AND attraction_name IS NOT NULL) OR (attraction_status != 'ACTIVE') );


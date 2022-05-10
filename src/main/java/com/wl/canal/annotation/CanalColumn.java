package com.wl.canal.annotation;

import java.lang.annotation.*;

/***
 * 数据库表实体字段映射
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface CanalColumn {

    String value() default "";
}

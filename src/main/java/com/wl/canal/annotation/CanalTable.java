package com.wl.canal.annotation;

import javax.swing.table.DefaultTableCellRenderer;
import java.lang.annotation.*;

/***
 * 监听表信息
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface CanalTable {

    /***
     * 数据库名称
     * @return
     */
    String databaseName() default "";

    /***
     * 表名称
     * @return
     */
    String tableName() default "";

}

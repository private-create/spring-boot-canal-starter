package com.wl.canal.init;

import com.google.common.collect.Maps;
import com.wl.canal.annotation.CanalTable;
import com.wl.canal.listener.CanalListener;
import com.wl.canal.load.LoadCanalData;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;

@Component
public class InitCanalListenerMap implements CommandLineRunner {

    @Resource
    private ApplicationContext applicationContext;

    @Resource
    private LoadCanalData loadCanalData;
    @Override
    public void run(String... args) throws Exception {
        Map<String, CanalListener> stringCanalListenerMap = scannerCanalListener();
        loadCanalData.setLoadCanalData(stringCanalListenerMap);

    }

    private Map<String, CanalListener> scannerCanalListener(){

        Map<String, CanalListener> beansOfType = applicationContext.getBeansOfType(CanalListener.class);

        Map<String,CanalListener> result = Maps.newHashMap();
        beansOfType.forEach((key,value)->{
            //判断是否带有CanalTable注解
             Class<?> aClass = value.getClass();

             CanalTable canalTable = aClass.isAnnotationPresent(CanalTable.class) ? aClass.getAnnotation(CanalTable.class) : null;

             if(Objects.isNull(canalTable)){return;}

             String canalKey = canalTable.databaseName() + canalTable.tableName();

             result.put(canalKey,value);
        });
        return result;
    }
}

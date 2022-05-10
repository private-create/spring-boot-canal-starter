package com.wl.canal.load;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wl.canal.annotation.CanalColumn;
import com.wl.canal.dict.RowDataType;
import com.wl.canal.listener.CanalListener;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/***
 * 监听canal数据解析
 */
@ConfigurationProperties(prefix = "canal.server")
@Data
@Component
public class LoadCanalData {

    private String host;

    private Integer port;

    private String destination;

    private String userName;

    private String password;

    private static final Integer MESSAGE_NUM=100;

    private static final ConcurrentHashMap<String, CanalListener> canalListenerMap = new ConcurrentHashMap<>();

    public void setLoadCanalData(Map<String,CanalListener> map){
        if(!CollectionUtils.isEmpty(map)){
            canalListenerMap.putAll(map);
            loadData();
        }
    }

    public void loadData(){

         List<String> strings = Arrays.asList(destination.split(","));
        createConnect(strings);
    }

    public void createConnect(List<String> destinations){
        destinations.forEach(d ->{
            new Thread(()->{
                //创建连接
                CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(host, port), d, userName, password);

                try {
                    //打开连接
                    connector.connect();

                    //订阅数据库表,全部表
                    connector.subscribe(".*\\..*");

                    //回滚到未进行ack的地方，下次fetch的时候，可以从最后一个没有ack的地方开始拿
                    connector.rollback();

                    while (true){
                        //获取消息
                        Message message = connector.get(MESSAGE_NUM);

                        //获取批量id
                        long id = message.getId();

                        //获取消息的数量
                        List<CanalEntry.Entry> entries = message.getEntries();

                        int size =  entries.size();

                        if(id==-1 || size<1){
                            Thread.sleep(2000);
                        }else {
                            handMessage(entries);
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    connector.disconnect();
                }
            }).start();
        });
    }


    private void handMessage(List<CanalEntry.Entry> entries){

        entries.forEach(entry -> {
            //如果是事务就跳过
            if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND){
                return;
            }
            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());

                loadCanalListener(rowChange,entry);

            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }

        });
    }

    private void loadCanalListener( CanalEntry.RowChange rowChange,CanalEntry.Entry entry){
        String canalKey = entry.getHeader().getSchemaName() + entry.getHeader().getTableName();
        CanalEntry.EventType eventType = rowChange.getEventType();
        handler(rowChange,canalKey,eventType);

    }


    private void handler(CanalEntry.RowChange rowChange,String canalKey, CanalEntry.EventType eventType){
        if(canalListenerMap.containsKey(canalKey)){
            CanalListener canalListener = canalListenerMap.get(canalKey);

            ParameterizedType genericInterface = (ParameterizedType) canalListener.getClass().getGenericInterfaces()[0];

            Class clazz = (Class) genericInterface.getActualTypeArguments()[0];

            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();


            rowDatasList.forEach(rowData -> {

                switch (eventType){
                    case DELETE:
                        canalListener.delete(getEntity(rowData, clazz, RowDataType.before));
                        break;
                    case UPDATE:
                        canalListener.update(getEntity(rowData, clazz, RowDataType.before),getEntity(rowData, clazz, RowDataType.after));
                        break;
                    case INSERT:
                        canalListener.insert(getEntity(rowData, clazz, RowDataType.after));
                        break;
                    default:
                        break;
                }
            });

        }
    }

    private <T> T getEntity(CanalEntry.RowData rowData, Class clazz, RowDataType flag){
        List<CanalEntry.Column> columnsList = null;
        switch (flag){
            case before:
                columnsList = rowData.getBeforeColumnsList();
                break;
            case after:
                columnsList = rowData.getAfterColumnsList();
                break;
            default:
                break;
        }
        if(CollectionUtils.isEmpty(columnsList)){ return null;}
        final Map<String, List<CanalEntry.Column>> map = columnsList.stream().collect(Collectors.groupingBy(CanalEntry.Column::getName));
        try {
           Object o = clazz.newInstance();
           Field[] fields = clazz.getDeclaredFields();
           for (Field field : fields) {
                String name = field.getName();
               if(field.isAnnotationPresent(CanalColumn.class)){
                   CanalColumn annotation = field.getAnnotation(CanalColumn.class);
                   name = StringUtils.isEmpty(annotation.value())?name:annotation.value();
               }
               if(map.containsKey(name)){
                   field.setAccessible(true);
                   field.set(o,map.get(name).get(0).getValue());
               }


           }
           return (T) o;
       }catch (Exception e){
           e.printStackTrace();
       }
        return null;
    }

}

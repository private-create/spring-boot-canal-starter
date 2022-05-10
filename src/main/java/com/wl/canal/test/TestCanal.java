package com.wl.canal.test;

import com.wl.canal.annotation.CanalTable;
import com.wl.canal.listener.CanalListener;
import com.wl.canal.test.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@CanalTable(databaseName = "kylwsp",tableName = "user")
@Slf4j
public class TestCanal implements CanalListener<User> {
    @Override
    public void insert(User after) {
        log.info("监听到新增事件after:{}",after.toString());

    }

    @Override
    public void update(User before, User after) {
        log.info("监听到更新事件before:{}",before.toString());

        log.info("监听到更新事件after:{}",after.toString());
    }

    @Override
    public void delete(User before) {
        log.info("监听到删除事件before:{}",before.toString());
    }
}

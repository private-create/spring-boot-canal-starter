package com.wl.canal.test.entity;

import com.wl.canal.annotation.CanalColumn;
import lombok.Data;

@Data
public class User {


    @CanalColumn("user_seq")
    private String userSeq;

    @CanalColumn("user_id")
    private String userId;

    @CanalColumn("user_name")
    private String userName;

    @CanalColumn("user_spell")
    private String userSpell;

    @CanalColumn("user_password")
    private String userPassword;
}

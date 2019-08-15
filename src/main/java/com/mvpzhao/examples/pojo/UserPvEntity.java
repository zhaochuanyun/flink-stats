package com.mvpzhao.examples.pojo;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class UserPvEntity {

    Timestamp time;

    String userId;

    Long pvcount;

    public UserPvEntity(Timestamp time, String userId, Long pvcount) {
        this.userId = userId;
        this.time = time;
        this.pvcount = pvcount;
    }
}

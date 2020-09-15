package com.donaldy.serializer;

/**
 * @author donald
 * @date 2020/09/15
 */
public class User {

    private Integer userId;
    private String username;

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Integer getUserId() {
        return userId;
    }

    public String getUsername() {
        return username;
    }
}

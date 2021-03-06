package com.aliyun.tablestore.kafka.connect.model;

/**
 * @Author lihn
 * @Date 2021/10/13 10:11
 */
public class StsUserBo {
    private String ak;
    private String sk;
    private String token;
    private String ownId;

    public StsUserBo() {
    }

    public String getAk() {
        return ak;
    }

    public void setAk(String ak) {
        this.ak = ak;
    }

    public String getSk() {
        return sk;
    }

    public void setSk(String sk) {
        this.sk = sk;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getOwnId() {
        return ownId;
    }

    public void setOwnId(String ownId) {
        this.ownId = ownId;
    }
}

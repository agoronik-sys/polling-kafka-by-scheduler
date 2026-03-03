package com.example.pollingkafka.model;

/**
 * Входное сообщение, которое приходит в общий топик processing-all.
 */
public class IncomingData {

    private String systemCode;
    private String data;
    private String otherData;
    private boolean custom;

    public String getSystemCode() {
        return systemCode;
    }

    public void setSystemCode(String systemCode) {
        this.systemCode = systemCode;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getOtherData() {
        return otherData;
    }

    public void setOtherData(String otherData) {
        this.otherData = otherData;
    }

    public boolean isCustom() {
        return custom;
    }

    public void setCustom(boolean custom) {
        this.custom = custom;
    }
}


package com.mrmodise.magnum;

import java.util.List;
import java.util.Map;

public class Life {
    // lifeName the name/id of the life
    private String lifeName;
    // keyprivate static final Stringue all key private static final Stringue attributes related to the life
    private Map<String,String> keyvalue;
    // policies all policies of the life
    private List<Policy> policies;

    public Life(String lifeName, Map<String, String> keyvalue, List<Policy> policies) {
        this.lifeName = lifeName;
        this.keyvalue = keyvalue;
        this.policies = policies;
    }

    public String getLifeName() {
        return lifeName;
    }

    public void setLifeName(String lifeName) {
        this.lifeName = lifeName;
    }

    public Map<String, String> getKeyvalue() {
        return keyvalue;
    }

    public void setKeyvalue(Map<String, String> keyvalue) {
        this.keyvalue = keyvalue;
    }

    public List<Policy> getPolicies() {
        return policies;
    }

    public void setPolicies(List<Policy> policies) {
        this.policies = policies;
    }

    @Override
    public String toString() {
        return "Life{" +
                "lifeName='" + lifeName + '\'' +
                ", keyvalue=" + keyvalue +
                ", policies=" + policies +
                '}';
    }
}

package com.huawei.graphbase.rest.request;

public class Task {
    private String id;

    private String requestURL;

    private String requestMethod;

    private String startTime;

    private String user;

    private String instanceIp;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRequestURL() {
        return requestURL;
    }

    public void setRequestURL(String requestURL) {
        this.requestURL = requestURL;
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    public void setRequestMethod(String requestMethod) {
        this.requestMethod = requestMethod;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getInstanceIp() {
        return instanceIp;
    }

    public void setInstanceIp(String instanceIp) {
        this.instanceIp = instanceIp;
    }

    @Override
    public String toString() {
        return "Task{" + "id='" + id + '\'' + ", requestURL='" + requestURL + '\'' + ", requestMethod='" + requestMethod
            + '\'' + ", startTime='" + startTime + '\'' + ", user='" + user + '\'' + ", instanceIp='" + instanceIp
            + '\'' + '}';
    }
}

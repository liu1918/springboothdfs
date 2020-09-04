package com.util;


public class Result {
    private String resCode;
    private String resDes;
    private Object data;
    public static final String FAILURE="sys-00-01";
    public static final String SUCCESS = "SUCCESS";

    public Result(String resCode,String resDes,Object data){
        this.resCode = resCode;
        this.resDes = resDes;
        this.data=data;
    }

    public Result(String resCode,String resDes){
        this.resCode = resCode;
        this.resDes = resDes;
    }

    public String getResCode() {
        return resCode;
    }

    public void setResCode(String resCode) {
        this.resCode = resCode;
    }

    public String getResDes() {
        return resDes;
    }

    public void setResDes(String resDes) {
        this.resDes = resDes;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}

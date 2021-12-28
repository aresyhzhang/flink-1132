package com.netease.neop.flink.formats.base.mgdc.entity;

/** @ Author ：aresyhzhang @ Date ：Created in 13:59 2021/12/24 @ Description：mgdc函数解析实体 */
public class BaseMgdcEntity {
    private Long rowtime;
    private String pid;
    private String logType;
    private String logTime;
    private String logDate;
    private String source;

    public Long getRowtime() {
        return this.rowtime;
    }

    public void setRowtime(Long rowtime) {
        this.rowtime = rowtime;
    }

    public String getPid() {
        return this.pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getLogType() {
        return this.logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getLogTime() {
        return this.logTime;
    }

    public void setLogTime(String logTime) {
        this.logTime = logTime;
    }

    public String getLogDate() {
        return this.logDate;
    }

    public void setLogDate(String logDate) {
        this.logDate = logDate;
    }

    public String getSource() {
        return this.source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return String.format("%s[%s][%s],%s", this.pid, this.logTime, this.logType, this.source);
    }
}

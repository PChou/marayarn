package com.eoi.marayarn.web.enums;

/**
 * Created by wenbo.gong on 2020/6/1
 */
public enum ApplicationStatus {

    READY("Ready", "就绪"),
    SUSPEND("Suspend", "挂起"),

    RUNNING("Running", "运行中"),
    SCALING("Scaling", "重新分配中"),
    DESTROYING("Destroying", "删除中"),
    ;

    public final String value;
    public final String description;

    ApplicationStatus(String value, String description) {
        this.value = value;
        this.description = description;
    }


}

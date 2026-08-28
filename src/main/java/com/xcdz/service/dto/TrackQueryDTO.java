package com.xcdz.service.dto;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 软件单元:JAVA-DTO-TRACK_QUERY-001
 * 功能:批次数据条件查询入参对象（前端→后端，REST /target/track/query 与 WebSocket 订阅共用的筛选契约）
 * 说明:时间字段用 String 传输（格式 yyyy-MM-dd HH:mm:ss），由入口层（Controller/Processor）解析校验
 */
@Data
@Accessors(chain = true)
public class TrackQueryDTO implements Serializable {
    //目标名称模糊关键字（LIKE 匹配 target.target_name）；null 或空白表示不限制（全部目标）
    private String target;

    //采集时间下界 yyyy-MM-dd HH:mm:ss（含）；空表示不限制
    private String startTime;

    //采集时间上界 yyyy-MM-dd HH:mm:ss（含）；空表示不限制
    private String endTime;

    //返回条数上限；空由服务端取默认值（500），最大 1000
    private Integer limit;
}

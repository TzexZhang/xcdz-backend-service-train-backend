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
    private String targetName;

    //采集时间下界 yyyy-MM-dd HH:mm:ss（含）；空表示不限制
    private String startTime;

    //采集时间上界 yyyy-MM-dd HH:mm:ss（含）；空表示不限制
    private String endTime;

    //返回条数上限；空由服务端取默认值（500），最大 1000
    private Integer limit;

    //id 游标（只返回 id > afterId 的行）；空/缺省 = 首页（不限制）。翻页时传入上一页最大 id，
    //与服务端流式快照的游标分页语义一致（REST 定位为联调验证工具，非前端必经链路）
    private String afterId;
}

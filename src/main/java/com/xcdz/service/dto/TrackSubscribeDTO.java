package com.xcdz.service.dto;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 软件单元:JAVA-DTO-TRACK_SUBSCRIBE-001
 * 功能:WebSocket 订阅业务入参对象（Processor 解析 TrackWsMessage 并校验后组装，传给推送任务）
 * 与 TrackQueryDTO 的区别:本 DTO 携带已解析的 LocalDateTime，属服务层内部契约，
 * 避免推送任务重复做字符串解析；纯数据载体，归一化等处理由入口层（Processor）完成
 */
@Data
@Accessors(chain = true)
public class TrackSubscribeDTO implements Serializable {
    //目标名称模糊关键字（LIKE 匹配 target.target_name）；null 表示不限制（全部目标），
    //空白关键字由入口层归一化为 null 后再传入
    private String target;

    //采集时间下界（含）；null 表示不限制（已由入口层按 yyyy-MM-dd HH:mm:ss 解析）
    private LocalDateTime startTime;

    //采集时间上界（含）；null 表示不限制
    private LocalDateTime endTime;
}

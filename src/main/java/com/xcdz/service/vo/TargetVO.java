package com.xcdz.service.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 软件单元:JAVA-VO-TARGET-001
 * 功能:目标出参对象（后端→前端，仅 REST 接口使用，走 Jackson 序列化）
 */
@Data
@Accessors(chain = true)
public class TargetVO implements Serializable {
    //目标id
    private String id;

    //目标名称
    private String targetName;

    //创建时间
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTime;
}

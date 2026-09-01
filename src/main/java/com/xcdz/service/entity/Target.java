package com.xcdz.service.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 软件单元:JAVA-ENTITY-TARGET-001
 * 功能:目标实体（树的一级，固定节点，映射前端树形结构）
 */
@Data
@Accessors(chain = true)
@TableName("target")
public class Target implements Serializable {
    //目标id（String 雪花id，显式声明 @TableId 锁定 assign_id 策略，
    //不依赖全局配置；SQL 脚本预置数据用 TG-DEMO-xxx 可读前缀）
    @TableId(type = IdType.ASSIGN_ID)
    private String id;

    //目标名称
    private String targetName;

    //创建时间
    private LocalDateTime createTime;
}

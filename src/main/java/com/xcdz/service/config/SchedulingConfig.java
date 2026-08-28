package com.xcdz.service.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 软件单元:JAVA-CFG-SCHEDULING-001
 * 功能:开启 Spring @Scheduled 定时任务能力（项目此前未开启）
 * 线程安全说明:沿用默认单线程调度器——推送任务(TrackPushTask)与模拟器(DataSimulator)
 * 串行执行且耗时均为毫秒级（1~2s 周期），单线程既满足需求又天然避免任务间并发竞争；
 * 若未来任务增多再引入自定义线程池
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}

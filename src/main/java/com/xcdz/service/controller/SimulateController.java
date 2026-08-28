package com.xcdz.service.controller;

import com.xcdz.service.simulate.DataSimulator;
import com.xcdz.service.utils.ResponseUtils;
import com.xcdz.service.vo.R;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 软件单元:JAVA-CTRL-SIMULATE-001
 * 功能:数据模拟器控制接口——动态控制模拟写入的启停与频率
 */
@Tag(name = "数据模拟器", description = "控制模拟数据写入 track 表的启停与频率")
@Slf4j
@RestController
@RequestMapping("/simulate")
public class SimulateController {

    @Autowired
    private DataSimulator dataSimulator;

    /**
     * 开启模拟写入（幂等）
     */
    @Operation(summary = "开启模拟写入", description = "恢复向 track 表周期写入模拟数据")
    @PostMapping("/start")
    public R<?> start() {
        dataSimulator.start();
        log.info("模拟器已开启");
        return ResponseUtils.ok(status());
    }

    /**
     * 停止模拟写入（幂等，不影响存量数据与推送链路）
     */
    @Operation(summary = "停止模拟写入", description = "暂停写入，已入库数据与 WebSocket 推送不受影响")
    @PostMapping("/stop")
    public R<?> stop() {
        dataSimulator.stop();
        log.info("模拟器已停止");
        return ResponseUtils.ok(status());
    }

    /**
     * 动态调整写入间隔（最小 500ms）
     */
    @Operation(summary = "调整写入间隔", description = "动态修改模拟写入周期，最小 500ms")
    @Parameter(name = "millis", description = "写入间隔毫秒数", example = "2000")
    @PostMapping("/interval")
    public R<?> interval(@RequestParam long millis) {
        dataSimulator.setInterval(millis);
        log.info("模拟器写入间隔调整为 {}ms", dataSimulator.getIntervalMillis());
        return ResponseUtils.ok(status());
    }

    /**
     * 查询模拟器当前状态
     */
    @Operation(summary = "查询模拟器状态", description = "返回运行开关与当前写入间隔")
    @GetMapping("/status")
    public R<?> status() {
        return ResponseUtils.ok(buildStatus());
    }

    /**
     * 组装状态数据（running=是否写入中，intervalMillis=当前间隔）
     */
    private Map<String, Object> buildStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("running", dataSimulator.isRunning());
        status.put("intervalMillis", dataSimulator.getIntervalMillis());
        return status;
    }
}

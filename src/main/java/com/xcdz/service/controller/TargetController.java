package com.xcdz.service.controller;

import com.xcdz.service.dto.TrackDetailDTO;
import com.xcdz.service.dto.TrackQueryDTO;
import com.xcdz.service.entity.Target;
import com.xcdz.service.service.TargetService;
import com.xcdz.service.service.TrackService;
import com.xcdz.service.utils.TargetConvert;
import com.xcdz.service.utils.TrackConvert;
import com.xcdz.service.utils.ResponseUtils;
import com.xcdz.service.vo.R;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * 软件单元:JAVA-CTRL-TARGET-001
 * 功能:目标控制器——目标列表（前端树/筛选项）+ 批次数据条件查询（推送对账）
 * 分层契约:入参以 DTO 承接、出参组装为 VO、业务委托 Service，本层无数据访问与业务规则
 */
@Tag(name = "数据源目标管理", description = "目标列表查询与批次数据条件查询")
@Slf4j
@RestController
@RequestMapping("/target")
public class TargetController {

    //批次数据查询默认与最大条数上限（与 WebSocket INIT 快照上限一致，防全量拉取）
    private static final int QUERY_DEFAULT_LIMIT = 500;
    private static final int QUERY_MAX_LIMIT = 1000;

    @Autowired
    private TargetService targetService;

    @Autowired
    private TrackService trackService;

    /**
     * 查询全部目标（前端构建树形结构一级节点/筛选下拉使用）
     */
    @Operation(summary = "查询目标列表", description = "返回全部目标，用于前端树形结构与筛选条件")
    @GetMapping("/list")
    public R<?> list() {
        log.info("查询目标列表");
        List<Target> targets = targetService.listAll();
        return ResponseUtils.ok(TargetConvert.toVOList(targets));
    }

    /**
     * 批次数据条件查询:与 WebSocket 订阅过滤条件完全一致（目标名称模糊关键字 + 采集时间范围），
     * 用于对账验证——"订阅期间累计收到的推送条数"应等于本接口同条件查询总数
     */
    @Operation(summary = "批次数据条件查询（对账用）",
            description = "按目标名称模糊关键字与采集时间范围查询 track（LEFT JOIN target 组合目标名称），"
                    + "条件语义与 WebSocket 订阅一致；target 为名称模糊关键字（LIKE），时间格式 yyyy-MM-dd HH:mm:ss")
    @GetMapping("/track/query")
    public R<?> queryTracks(@ParameterObject TrackQueryDTO dto) {
        log.info("批次数据条件查询: {}", dto);
        //入口层校验:limit 空→默认，越界收敛到 [1,1000]；时间格式/区间非法→失败响应（项目无全局异常处理器，不抛裸 500）
        int limit = (dto.getLimit() == null) ? QUERY_DEFAULT_LIMIT : Math.min(Math.max(dto.getLimit(), 1), QUERY_MAX_LIMIT);
        LocalDateTime start;
        LocalDateTime end;
        try {
            start = parseTime(dto.getStartTime());
            end = parseTime(dto.getEndTime());
        } catch (DateTimeParseException e) {
            return ResponseUtils.fail(e.getMessage());
        }
        if (start != null && end != null && start.isAfter(end)) {
            return ResponseUtils.fail("startTime 不能晚于 endTime");
        }

        List<TrackDetailDTO> tracks = trackService.findByCondition(dto.getTarget(), start, end, limit);
        return ResponseUtils.ok(TrackConvert.toVOList(tracks));
    }

    /**
     * 时间入参解析:空白返回 null（=不限制）；格式非法抛 DateTimeParseException 由调用方统一回执
     * 格式契约与 WebSocket 订阅报文一致（见 TrackConvert.DATE_TIME_FORMATTER）
     */
    private LocalDateTime parseTime(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return LocalDateTime.parse(value.trim(), TrackConvert.DATE_TIME_FORMATTER);
    }
}

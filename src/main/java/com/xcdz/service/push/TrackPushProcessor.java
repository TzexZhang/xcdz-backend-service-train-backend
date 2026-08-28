package com.xcdz.service.push;

import com.alibaba.fastjson.JSON;
import com.xcdz.service.dto.TrackSubscribeDTO;
import com.xcdz.service.utils.TrackConvert;
import com.xcdz.service.websocket.protocol.WsEnvelope;
import com.xcdz.service.websocket.protocol.WsMessageProcessor;
import com.xcdz.service.websocket.session.WsSessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * 软件单元:JAVA-WS-TRACK-PUSH_PROCESSOR-001
 * 功能:目标/批次推送业务消息处理器——承接通用路由层分发的 SUBSCRIBE / UNSUBSCRIBE
 * 分层契约:本类只做"业务报文解析与入口校验"，业务入参组装为 TrackSubscribeDTO 后
 * 委托 TrackPushTask；本类不包含任何业务规则与数据访问
 */
@Slf4j
@Component
public class TrackPushProcessor implements WsMessageProcessor {

    @Autowired
    private TrackPushTask pushTask;

    @Autowired
    private WsSessionManager sessionManager;

    /**
     * 声明处理的消息类型:订阅与退订
     */
    @Override
    public Set<String> supportTypes() {
        return Set.of(TrackWsMessage.TYPE_SUBSCRIBE, TrackWsMessage.TYPE_UNSUBSCRIBE);
    }

    /**
     * 消息入口:SUBSCRIBE → 解析校验后订阅；UNSUBSCRIBE → 退订
     */
    @Override
    public void process(WebSocketSession session, String type, String rawPayload) {
        if (TrackWsMessage.TYPE_UNSUBSCRIBE.equals(type)) {
            pushTask.unsubscribe(session);
            return;
        }
        final TrackWsMessage req;
        try {
            //fastjson 反序列化，未知字段忽略，缺失字段为 null
            req = JSON.parseObject(rawPayload, TrackWsMessage.class);
        } catch (Exception e) {
            sendError(session, "报文解析失败: " + e.getMessage());
            return;
        }
        handleSubscribe(session, req);
    }

    /**
     * 连接关闭:幂等移除该连接的订阅，订阅表不残留
     */
    @Override
    public void onConnectionClosed(WebSocketSession session) {
        pushTask.unsubscribe(session);
    }

    /**
     * 处理订阅:入口层校验（时间格式/区间合法性）+ 组装业务 DTO（空 target 归一化为 null=全部目标），
     * 业务流程（快照/水位/登记）委托推送任务
     */
    private void handleSubscribe(WebSocketSession session, TrackWsMessage req) {
        /**
         * 开始、结束时间校验
         */
        LocalDateTime startTime = parseTime(session, req.getStartTime(), "startTime");
        if (startTime == null && req.getStartTime() != null && !req.getStartTime().trim().isEmpty()) {
            return; //parseTime 已回执 ERROR
        }
        LocalDateTime endTime = parseTime(session, req.getEndTime(), "endTime");
        if (endTime == null && req.getEndTime() != null && !req.getEndTime().trim().isEmpty()) {
            return;
        }
        if (startTime != null && endTime != null && startTime.isAfter(endTime)) {
            sendError(session, "startTime 不能晚于 endTime");
            return;
        }
        //归一化:空白关键字 → null（全部目标），与契约"target 空串=全部"一致
        String target = req.getTarget();
        if (target != null && target.trim().isEmpty()) {
            target = null;
        }
        TrackSubscribeDTO dto = new TrackSubscribeDTO()
                .setTarget(target)
                .setStartTime(startTime)
                .setEndTime(endTime);
        pushTask.subscribe(session, dto);
    }

    /**
     * 时间字段解析:空白视为"不限制"返回 null；格式非法回执 ERROR 并返回标记
     * 返回值语义:null + 原值为空 → 不限制；null + 原值非空 → 已回执错误；非 null → 解析成功
     */
    private LocalDateTime parseTime(WebSocketSession session, String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return LocalDateTime.parse(value.trim(), TrackConvert.DATE_TIME_FORMATTER);
        } catch (DateTimeParseException e) {
            sendError(session, fieldName + " 格式非法，要求 yyyy-MM-dd HH:mm:ss: " + value);
            return null;
        }
    }

    /**
     * 向单个连接回执错误信息（不中断连接，客户端可修正报文后重试）
     */
    private void sendError(WebSocketSession session, String message) {
        sessionManager.send(session, new WsEnvelope().setType(WsEnvelope.TYPE_ERROR).setMessage(message));
    }
}



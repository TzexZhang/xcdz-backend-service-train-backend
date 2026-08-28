package com.xcdz.service.websocket.auth.impl;

import com.xcdz.service.websocket.auth.WsHandshakeAuthorizer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import java.util.Map;

/**
 * 软件单元:JAVA-WS-AUTH-HANDSHAKE_INTERCEPTOR-001
 * 功能:WebSocket 握手鉴权拦截器——Spring WS 握手链路与 WsHandshakeAuthorizer 扩展点之间的适配器
 * 说明:MVC 的 HandlerInterceptor 拦不到 WebSocket 握手（WS 端点走自己的 handler mapping，
 * 不经过 @RequestMapping 与 MVC 拦截器链），握手期校验必须用 HandshakeInterceptor，
 * 本类即该挂点；鉴权策略本体在 WsHandshakeAuthorizer 实现中，本类不含任何鉴权规则
 */
@Slf4j
@Component
public class WsHandshakeAuthInterceptor implements HandshakeInterceptor {

    //鉴权策略（单构造器自动装配，容器注入 WsHandshakeAuthorizer 实现）
    private final WsHandshakeAuthorizer authorizer;

    public WsHandshakeAuthInterceptor(WsHandshakeAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    /**
     * 握手前鉴权:拒绝时以 401 结束握手（连接不建立，客户端收到非 101 响应）
     */
    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler wsHandler, Map<String, Object> attributes) {
        boolean allowed = authorizer.authorize(request, attributes);
        if (!allowed) {
            response.setStatusCode(HttpStatus.UNAUTHORIZED);
            log.warn("WebSocket 握手被拒绝: {}", request.getRemoteAddress());
        }
        return allowed;
    }

    /**
     * 握手后:无清理事项
     */
    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                               WebSocketHandler wsHandler, Exception exception) {
    }
}

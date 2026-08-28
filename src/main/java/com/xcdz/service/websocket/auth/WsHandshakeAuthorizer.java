package com.xcdz.service.websocket.auth;

import org.springframework.http.server.ServerHttpRequest;

import java.util.Map;

/**
 * 软件单元:JAVA-WS-AUTH-HANDSHAKE_AUTHORIZER-001
 * 功能:WebSocket 握手鉴权扩展点——由 WsHandshakeAuthInterceptor 在握手前调用
 * 预留口子:当前项目无认证体系，默认实现 impl.WsHandshakeAuthorizerImpl 全量放行；
 * 未来接入认证时提供新实现（注册为 @Primary 或删除放行实现）即可，握手链路零改动
 */
public interface WsHandshakeAuthorizer {

    /**
     * 握手鉴权:校验握手请求（可读取 header/query 中携带的凭证）
     *
     * @param request    握手 HTTP 请求（HTTP GET + Upgrade）
     * @param attributes 会话属性表（实现方可写入用户身份等数据，连接建立后经
     *                   WebSocketSession.getHandshakeAttributes() 读取）
     * @return true=放行握手；false=拒绝（拦截器以 401 结束握手，连接不建立）
     */
    boolean authorize(ServerHttpRequest request, Map<String, Object> attributes);
}

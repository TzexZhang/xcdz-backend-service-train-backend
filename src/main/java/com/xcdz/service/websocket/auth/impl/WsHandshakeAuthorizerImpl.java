package com.xcdz.service.websocket.auth.impl;

import com.xcdz.service.websocket.auth.WsHandshakeAuthorizer;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 软件单元:JAVA-WS-AUTH-AUTHORIZER_IMPL-001
 * 功能:握手鉴权默认实现——全量放行（当前项目无认证体系，有无凭证一律允许连接）
 * 替换方式:未来接入认证时，提供 WsHandshakeAuthorizer 新实现并注册为
 * {@code @Primary}（或直接删除本类），WsHandshakeAuthInterceptor 与 WebSocketConfig 均无需改动
 */
@Component
public class WsHandshakeAuthorizerImpl implements WsHandshakeAuthorizer {

    /**
     * 无认证阶段:一律放行
     */
    @Override
    public boolean authorize(ServerHttpRequest request, Map<String, Object> attributes) {
        return true;
    }
}

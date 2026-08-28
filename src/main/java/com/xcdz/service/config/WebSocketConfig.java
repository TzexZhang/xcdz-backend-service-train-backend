package com.xcdz.service.config;

import com.xcdz.service.websocket.auth.impl.WsHandshakeAuthInterceptor;
import com.xcdz.service.websocket.router.RoutingWebSocketHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * 软件单元:JAVA-CFG-WEB_SOCKET-001
 * 功能:WebSocket 配置——注册通用路由处理器与握手鉴权拦截器到 /ws/data 端点
 * 说明:
 *   1. setAllowedOrigins("*") 放开握手跨域限制，与 WebConfig 中已放开的 CORS 策略保持一致；
 *   2. 通用路由处理器以 @Component 注册（构造注入会话管理器与容器内全部业务处理器），
 *      本配置只做端点注册与鉴权挂点装配，不感知具体业务；
 *   3. 握手鉴权走 WS 专用 HandshakeInterceptor 挂点（MVC 拦截器对 WS 握手不生效），
 *      当前注入的 Authorizer 实现为全量放行，未来接入认证时替换实现即可
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    // 业务处理
    @Autowired
    private RoutingWebSocketHandler routingWebSocketHandler;

    //握手鉴权拦截器（容器 Bean，内部装配 WsHandshakeAuthorizer 实现，当前为全量放行）
    @Autowired
    private WsHandshakeAuthInterceptor handshakeAuthInterceptor;

    /**
     * 注册处理器:前端连接地址 ws://host:9700/ws/data
     */
    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        // addInterceptors 作用：添加握手拦截器
        // setAllowedOrigins 作用：设置允许跨域的源
        registry.addHandler(routingWebSocketHandler, "/ws/data")
                .addInterceptors(handshakeAuthInterceptor)
                .setAllowedOrigins("*");

    }
}

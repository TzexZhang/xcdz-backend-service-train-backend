package com.xcdz.service.contants;

/**
 * 响应状态码
 * author: 程鑫
 * date: 2025年05月27日 17:26
 */
public final class ResponseCode {
    //请求成功
    public static final Integer SUCCESS = 200;
    //请求失败
    public static final Integer FAIL = 500;
    //未授权
    public static final Integer UNAUTHORIZED = 401;
    //权限不足
    public static final Integer ACCESS_DENIED = 403;
    //未找到
    public static final Integer NOT_FOUND = 404;
    //请求方法错误
    public static final Integer METHOD_NOT_ALLOWED = 405;
    //请求参数错误
    public static final Integer BAD_REQUEST = 400;
    //网关
    public static final Integer GATEWAY_TIMEOUT = 503;
    //用户名不存在
    public static final Integer USER_NOT_EXIST = 101;
    //密码错误
    public static final Integer PASSWORD_ERROR = 102;
    //令牌失效
    public static final Integer TOKEN_EXPIRED = 103;
    //用户被禁用
    public static final Integer USER_DISABLED = 104;
    //用户被锁
    public static final Integer USER_LOCKED = 105;
    //账号已过期
    public static final Integer ACCOUNT_EXPIRED = 106;
}

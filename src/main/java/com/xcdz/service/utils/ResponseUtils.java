package com.xcdz.service.utils;


import com.xcdz.service.contants.ResponseCode;
import com.xcdz.service.vo.R;

/**
 * 响应工具类
 * @author  chengxin
 * @date 2024/10/22 17:22
 */
public class ResponseUtils {

    private final static  String SUCCESS = "请求成功!";
    private final static String FAIL = "请求失败!";

    public static R<?> ok(Object data){
        return  ok(SUCCESS,data);
    }

    public static R<?> ok(String message,Object data){
        return new R<>().setCode(ResponseCode.SUCCESS).setMessage(message).setResult(data);
    }

    public static R<?> fail(Integer code,String message,Object data){
        return  new R<>().setCode(code).setMessage(message).setResult(data);
    }

    public static R<?> fail(String message){
        return fail(ResponseCode.FAIL,message,null);
    }

    public static R<?> fail(Integer code,String message){
        return fail(code,message,null);
    }
}

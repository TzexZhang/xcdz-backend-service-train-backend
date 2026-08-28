package com.xcdz.service.vo;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 响应类
 * author: 程鑫
 * date: 2024年10月22日 13:37
 *  统一响应包装（code/message/result）
 */
@Data
@Accessors(chain = true)
public class R<T> implements Serializable {
     /**
     * 消息
     */
    private String message;
    /**
     * 请求状态码
     */
    private Integer code;
    /**
     * 请求时间
     */
    private long timestamp = System.currentTimeMillis();
    /**
     * 返回结果
     */
    private T result;

    private boolean success = true;
}

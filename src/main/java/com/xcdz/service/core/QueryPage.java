package com.xcdz.service.core;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.List;

/**
 * QueryPage
 * author: 程鑫
 * date: 2024年10月23日 15:08
 * 通用分页响应包装
 */
@Data
@Accessors(chain =true)
public class QueryPage<T> implements Serializable {
    protected List<T> records;
    protected long total;
    protected long size;
    protected long current;
    protected long pages;
}

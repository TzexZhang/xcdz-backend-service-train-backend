package com.xcdz.service.utils;

import org.springframework.beans.BeanUtils;

/**
 * 实例复制工具类
 * author: 程鑫
 * date: 2024年10月22日 15:19
 */
public class InstanceCopyUtils {

    /**
     * 实例复制
     * @param targetClass
     * @param source
     * @return
     * @param <T>
     */
    public static<T> T instanceCopy(Class<T> targetClass,Object source){
        if(source == null){
            return null;
        }
        T t = null;
        try {
            t = targetClass.newInstance();
            BeanUtils.copyProperties(source, t);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return t;
    }
}

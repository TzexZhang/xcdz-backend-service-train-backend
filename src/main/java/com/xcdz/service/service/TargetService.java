package com.xcdz.service.service;

import com.xcdz.service.entity.Target;

import java.util.List;

/**
 * 软件单元:JAVA-SERV-TARGET-001
 * 功能:目标服务接口（定义"能做什么"）
 * 分层契约:只做业务编排，数据访问委托 TargetMapper，不继承 MP 的 IService
 */
public interface TargetService {

    /**
     * 查询全部目标（前端树形结构一级节点/筛选项数据源）
     *
     * @return 全部目标，按创建时间升序
     */
    List<Target> listAll();
}

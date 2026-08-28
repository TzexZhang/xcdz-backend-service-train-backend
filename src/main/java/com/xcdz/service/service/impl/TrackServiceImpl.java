package com.xcdz.service.service.impl;

import com.xcdz.service.dto.TrackDetailDTO;
import com.xcdz.service.entity.Track;
import com.xcdz.service.mapper.TrackMapper;
import com.xcdz.service.service.TrackService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

/**
 * 软件单元:JAVA-SERV-TRACK-001
 * 功能:批次数据服务实现（业务编排层——校验/规则/事务，数据访问全部委托 Mapper）
 * 有意不继承 MP 的 ServiceImpl:防止 lambdaQuery/saveBatch 等内置数据访问能力
 * 泄漏进业务层，保证"Service 只做业务、SQL 归 Mapper"的分层铁律可被执行
 */
@Service
public class TrackServiceImpl implements TrackService {

    @Autowired
    private TrackMapper trackMapper;

    /**
     * 条件查询编排:limit 非正数属非法业务参数直接返回空（不发起无效查库），
     * 其余过滤语义（空白关键字=全部目标、时间为空=不限制）由 Mapper 的 XML 动态条件兜底
     */
    @Override
    public List<TrackDetailDTO> findByCondition(String target, LocalDateTime startTime, LocalDateTime endTime, int limit) {
        if (limit <= 0) {
            return Collections.emptyList();
        }
        return trackMapper.selectByCondition(target, startTime, endTime, limit);
    }

    /**
     * 水位增量查询编排:limit 非正数防御，同上
     */
    @Override
    public List<TrackDetailDTO> findIncremental(long afterId, int limit) {
        if (limit <= 0) {
            return Collections.emptyList();
        }
        return trackMapper.selectIncremental(afterId, limit);
    }

    /**
     * 最大 id 查询编排:直接委托 Mapper（MP 3.5.7 BaseMapper 无 insertBatch/聚合，
     * 该"取最大id"语义已在 Mapper 以 selectOne+order by 实现）
     */
    @Override
    public long maxId() {
        return trackMapper.selectMaxId();
    }

    /**
     * 批量写入编排:事务边界在此层（单批 1~6 条，逐条 insert 的性能开销可忽略；
     * MP 3.5.7 BaseMapper 无批量插入方法，循环调用 Mapper 单条插入保证原子性）
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void saveRecords(List<Track> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        for (Track record : records) {
            trackMapper.insert(record);
        }
    }
}

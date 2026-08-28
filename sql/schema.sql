-- =====================================================================
-- 软件单元:SQL-SCHEMA-DATA_SOURCE_PUSH-001
-- 功能:WebSocket 增量推送演示建表脚本（在 train 库手动执行）
-- 设计说明:
--   1. 两表设计（已确认契约）:
--      - target  目标表（树的一级，固定，对应前端树形结构的目标节点）
--      - track   批次数据表（每行 = 一条采集数据，由"第三方"持续写入）
--   2. track.id 使用 BIGINT 自增而非项目惯例的雪花 String:
--      自增 id 单调递增，是推送侧"水位轮询"感知增量的依据；
--      target 保持项目惯例（String 雪花 id，由 MP assign_id 生成）
--   3. idx_target_time 联合索引支撑最高频查询:按目标 + 采集时间范围过滤
--   4. 存量库迁移（原 parent_node/child_record 表，如需保留旧数据，先手动执行
--      下方迁移语句，无需再执行本脚本的建表部分；旧表演示数据可弃时直接 DROP）:
--      RENAME TABLE parent_node TO `target`;
--      ALTER TABLE `target` CHANGE node_name target_name VARCHAR(100) NOT NULL COMMENT '目标名称';
--      RENAME TABLE child_record TO track;
--      ALTER TABLE track CHANGE parent_id target_id VARCHAR(32) NOT NULL COMMENT '所属目标id → target.id';
--      ALTER TABLE track DROP INDEX idx_parent_time, ADD INDEX idx_target_time (target_id, collect_time);
-- =====================================================================

-- ---------------------------------------------------------------------
-- 目标表（固定树节点）
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS target (
    id          VARCHAR(32)  NOT NULL                COMMENT '目标id（雪花id，String）',
    target_name VARCHAR(100) NOT NULL                COMMENT '目标名称',
    create_time DATETIME                             COMMENT '创建时间',
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT '数据源目标表（树的一级节点）';

-- ---------------------------------------------------------------------
-- 批次数据表（第三方持续写入，增量水位来源）
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS track (
    id           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '自增主键（单调递增，推送水位依据）',
    target_id    VARCHAR(32)  NOT NULL                COMMENT '所属目标id → target.id',
    metric_value DECIMAL(12, 2)                       COMMENT '采集指标值（演示用随机数）',
    collect_time DATETIME     NOT NULL                COMMENT '数据自身采集时间（前端时间筛选依据，可能晚于入库时间=迟到补录数据）',
    create_time  DATETIME     NOT NULL                COMMENT '入库时间',
    PRIMARY KEY (id),
    KEY idx_target_time (target_id, collect_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT '批次数据表（每行一条采集数据，持续插入）';

-- ---------------------------------------------------------------------
-- 演示目标数据（可按需删改；id 使用可读前缀便于人工辨认，
-- 后续通过业务代码新增的目标将由 MP assign_id 生成雪花 id，互不冲突）
-- 幂等设计:使用 INSERT IGNORE，脚本重复执行时已存在的演示目标自动跳过，
-- 避免 MySQL 1062 主键冲突（CREATE TABLE 已带 IF NOT EXISTS，整脚本可重复执行）
-- ---------------------------------------------------------------------
INSERT IGNORE INTO target (id, target_name, create_time) VALUES
('TG-DEMO-001', '华东电网', NOW()),
('TG-DEMO-002', '华北电网', NOW()),
('TG-DEMO-003', '华南电网', NOW()),
('TG-DEMO-004', '西北电网', NOW());

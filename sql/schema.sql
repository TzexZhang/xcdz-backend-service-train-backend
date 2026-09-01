-- =====================================================================
-- 软件单元:SQL-SCHEMA-DATA_SOURCE_PUSH-001
-- 功能:WebSocket 增量推送演示建表脚本（在 train 库手动执行）
-- 设计说明:
--   1. 两表设计（已确认契约）:
--      - target  目标表（树的一级，固定，对应前端树形结构的目标节点）
--      - track   批次数据表（每行 = 一条采集数据，由"第三方"持续写入）
--   2. track.id 与 target.id 统一为项目惯例的 String 雪花 id（MP assign_id 生成）:
--      雪花 id 为 19 位定长数字字符串，趋势单调递增且字典序与数值序一致，
--      推送侧水位游标（id > afterId）与按 id 升序游标翻页的语义保持不变
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
    id           VARCHAR(32)  NOT NULL                COMMENT '数据id（String 雪花id，MP assign_id 生成）',
    target_id    VARCHAR(32)  NOT NULL                COMMENT '所属目标id → target.id',
    metric_value DECIMAL(12, 2)                       COMMENT '采集指标值（演示用随机数）',
    collect_time DATETIME     NOT NULL                COMMENT '数据自身采集时间（前端时间筛选依据，可能晚于入库时间=迟到补录数据）',
    create_time  DATETIME     NOT NULL                COMMENT '入库时间',
    PRIMARY KEY (id),
    KEY idx_target_time (target_id, collect_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT '批次数据表（每行一条采集数据，持续插入）';

-- ---------------------------------------------------------------------
-- 演示目标数据（可按需删改；id 使用可读前缀便于人工辨认，
-- 后续通过业务代码新增的目标/批次数据 id 均由 MP assign_id 生成，互不冲突）
-- 注意:本脚本幂等建表（CREATE TABLE IF NOT EXISTS），
-- 存量 track 表若为旧 BIGINT 自增结构，需先手动迁移列类型:
--      ALTER TABLE track MODIFY COLUMN id VARCHAR(32) NOT NULL COMMENT '数据id（String 雪花id，MP assign_id 生成）';
--      （存量自增 id 会以数字字符串形式保留，与新雪花 id 在字典序下可比）
-- 幂等设计:使用 INSERT IGNORE，脚本重复执行时已存在的演示目标自动跳过，
-- 避免 MySQL 1062 主键冲突（CREATE TABLE 已带 IF NOT EXISTS，整脚本可重复执行）
-- ---------------------------------------------------------------------
INSERT IGNORE INTO target (id, target_name, create_time) VALUES
('TG-DEMO-001', '华东电网', NOW()),
('TG-DEMO-002', '华北电网', NOW()),
('TG-DEMO-003', '华南电网', NOW()),
('TG-DEMO-004', '西北电网', NOW());

-- ---------------------------------------------------------------------
-- 演示批次数据（10 条，可按需删改）:
--   1. id 模拟 19 位定长数字雪花 id 且趋势递增，与推送侧水位游标
--      （id > afterId）及按 id 升序翻页的字典序比较语义兼容；
--   2. target_id 关联上方 TG-DEMO-001~004；
--   3. collect_time 略早于 create_time，模拟"采集 → 入库"延迟；
--   4. 同样使用 INSERT IGNORE，脚本重复执行时自动跳过
-- ---------------------------------------------------------------------
INSERT IGNORE INTO track (id, target_id, metric_value, collect_time, create_time) VALUES
('2095000000000000101', 'TG-DEMO-001', 3250.50, '2026-08-29 09:00:00', '2026-08-29 09:00:01'),
('2095000000000000102', 'TG-DEMO-002', 2817.35, '2026-08-29 09:05:00', '2026-08-29 09:05:01'),
('2095000000000000103', 'TG-DEMO-001', 3312.80, '2026-08-29 09:10:00', '2026-08-29 09:10:02'),
('2095000000000000104', 'TG-DEMO-003', 1976.42, '2026-08-29 09:15:00', '2026-08-29 09:15:01'),
('2095000000000000105', 'TG-DEMO-004', 1245.90, '2026-08-29 09:20:00', '2026-08-29 09:20:01'),
('2095000000000000106', 'TG-DEMO-002', 2864.17, '2026-08-29 09:25:00', '2026-08-29 09:25:02'),
('2095000000000000107', 'TG-DEMO-003', 2031.66, '2026-08-29 09:30:00', '2026-08-29 09:30:01'),
('2095000000000000108', 'TG-DEMO-001', 3198.04, '2026-08-29 09:35:00', '2026-08-29 09:35:01'),
('2095000000000000109', 'TG-DEMO-004', 1302.58, '2026-08-29 09:40:00', '2026-08-29 09:40:02'),
('2095000000000000110', 'TG-DEMO-002', 2790.23, '2026-08-29 09:45:00', '2026-08-29 09:45:01');

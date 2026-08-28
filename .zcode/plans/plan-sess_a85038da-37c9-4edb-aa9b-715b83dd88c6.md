# 最终计划：Target/Track 重命名 + WebSocket 通用层解耦（已并入 ws-test.html 删除）

## 变更总览
1. 领域重命名：ParentNode→Target（目标）、ChildRecord→Track（批次）、协议字段 `parentIds`→`target`（List\<String\>，空数组=全部目标）、表 `parent_node`→`target`、`child_record`→`track`、列 `parent_id`→`target_id`、`node_name`→`target_name`、库名注释改 train（application.yml 本就指向 train，不改）
2. WebSocket 基础设施通用化：路由/会话/基础报文下沉 `websocket.core`（零业务概念可复用），业务通过 `WsMessageProcessor` SPI 接入；水位/快照/窗口关闭/线程安全等行为逻辑全部不变
3. **删除 `src/main/resources/static/ws-test.html`**（已有对应前端项目，不再维护内嵌测试页）
4. REST 路由：`/parent/list`→`/target/list`、`/parent/record/query`→`/target/track/query`；预置数据 id `PN-DEMO-*`→`TG-DEMO-*`

## 一、WebSocket 解耦结构

**新增 `websocket/core`（通用层 4 文件，仅依赖 spring-websocket/fastjson）**
- `WsSessionManager`：全部连接登记 + 以 session 为锁的线程安全 send(Object) + 失败清理
- `WsMessageProcessor`（SPI）：`supportTypes()` / `process(session, type, rawPayload)` / `default onConnectionClosed(session)`
- `RoutingWebSocketHandler`：JSON 解析→type 提取→PING 内建应答→按路由表委托业务处理器；缺 type/非法 JSON/未知类型回 ERROR（不中断连接）；连接关闭时通知全部处理器清理
- `WsEnvelope`：通用最小报文 `{type, message, data}`，承载 PONG/ERROR

**业务侧 `websocket` 包（4 文件）**
- `TrackPushProcessor implements WsMessageProcessor`：SUBSCRIBE/UNSUBSCRIBE 业务报文解析校验（原 DatasourcePushHandler 业务部分）
- `TrackSubscriptionManager`：订阅状态表（targetFilter/时间窗/水位/windowClosedNotified），发送职责移交 core
- `TrackPushTask`（原 DatasourcePushTask）：水位轮询+过滤+推送逻辑不变，发送走 core，ADR 注释补解耦决策
- `TrackWsMessage`（原 WsMessage，扁平独立类避免 Lombok 继承链断裂）：target/startTime/endTime/data(List\<TrackVO\>)/ids + 业务 TYPE 常量

**删除**：DatasourcePushHandler、PushSessionManager、WsMessage、DatasourcePushTask（职责已拆分迁移）；`WebSocketConfig` 改注册 RoutingWebSocketHandler（@Bean 构造注入全部 Processor），端点 /ws/data 不变

## 二、领域重命名（15 个文件改名+内容替换）
entity/Target+Track、mapper/TargetMapper+TrackMapper、service/TargetService+TrackService 及 Impl、utils/TargetConvert+TrackConvert（DATE_TIME_FORMATTER 随类迁移）、vo/TargetVO+TrackVO、dto/TrackQueryDTO+TrackSubscribeDTO、controller/TargetController（路由与 Swagger 文案"目标列表/批次数据条件查询"）
- `TargetController.queryTracks()` 原名 queryRecords；`matchParent()`→`matchTarget()`
- DataSimulator：Target/Track/targetId/“target 表为空”日志/注释；SimulateController 注释 child_record→track
- 旧 15 个原文件删除（非 git 仓库，无版本回滚点）

## 三、SQL（schema.sql 重写）
- 建 `target`（id, target_name, create_time）与 `track`（id 自增, target_id, metric_value, collect_time, create_time，索引 idx_target_time）；INSERT IGNORE 预置 TG-DEMO-001..004（名称保留）；头部注释"train 库"+注释掉的存量迁移语句（RENAME TABLE + CHANGE COLUMN，可选执行）
- target/track 均非 MySQL 保留字，可直接作表名

## 四、验证
- `mvn clean compile`（clean 清除旧类 class）
- grep 零残留：ParentNode|ChildRecord|parentIds|parentId|parent_node|child_record|PN-DEMO|parentFilter|matchParent|DatasourcePushHandler|PushSessionManager（含访问器形式）
- 解耦达标判据：core 包内无 Target/Track 等业务符号

## 五、风险
- 对外契约破坏性变更：WS 字段与 REST 路由改名（对应前端项目需同步）
- 存量 train 库旧表默认不迁移（演示数据可弃则 DROP；需保留则执行脚本内注释的迁移语句）
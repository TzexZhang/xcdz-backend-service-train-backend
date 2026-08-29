# CDC 实时推送架构方案（v2）

> 定位：实时推送链路的**现状架构文档 + 部署运维手册**。
> 技术路线：binlog CDC（Debezium）+ Kafka 单通道感知，服务端流式完整快照，前端零对账。

---

## 1. 架构概述

### 1.1 目标与特性

| 特性 | 实现手段 |
|---|---|
| 前端只管 WS 接收（append + 按 id 去重） | 服务端流式完整快照，零截断、零对账 |
| 毫秒级感知、可感知 DML 全集 | Debezium binlog CDC → Kafka |
| 单一感知通道，业务库零轮询压力 | CDC 直读 binlog，应用不回库扫增量 |
| 慢消费者不拖垮推送线程 | 会话级有界发送缓冲 + 超时断连 |
| 延迟可观测 | Micrometer 埋点（binlog 提交 → 应用处理） |
| 订阅规模可扩展 | 事件按 targetId 倒排路由，替代全订阅扫描 |

### 1.2 总体架构

```
写入方(模拟器/业务代码/外部SQL)
   │ INSERT/UPDATE/DELETE
   ▼
MySQL train.track / train.target            ← 零侵入（ROW 格式 binlog）
   │ binlog
   ▼
Debezium Connect（docker :8083）             ← 唯一感知通道
   │ 变更事件(JSON, schema_only 快照模式)
   ▼
Kafka: xcdz.train.track / xcdz.train.target  ← 单分区保证表内有序
   │ @KafkaListener (group: xcdz-push, at-least-once)
   ▼
Spring Boot(9700)
   ├─ TrackCdcConsumer   ← 事件解析(UTC/decimal) → 路由分发 → 指标埋点
   ├─ TargetNameCache    ← target_id→target_name 维度缓存(启动全量 + target CDC 维护)
   └─ TrackPushTask      ← 订阅时流式完整快照 + CDC 增量分发(pushLock 串行)
   ▼
WS 网关层(WsSessionManager, decorator 背压)
   ▼
前端:纯 WS 接收(SUBSCRIBED → DATA/SNAPSHOT… → READY → DATA/LIVE…)
```

---

## 2. WS 协议 v2

### 2.1 客户端 → 服务端（`TrackWsRequest`，独立类）

```jsonc
{ "type": "SUBSCRIBE",   "target": "华东", "startTime": null, "endTime": null }
{ "type": "UNSUBSCRIBE" }
```

- `target`：目标名称模糊关键字，空/缺省 = 全部目标
- 时间格式 `yyyy-MM-dd HH:mm:ss`，闭区间，按数据自身 `collect_time` 过滤（迟到补录语义）

### 2.2 服务端 → 客户端（`TrackWsMessage`）

| type | 载荷字段 | 语义 |
|---|---|---|
| `SUBSCRIBED` | target/startTime/endTime 回显 | 订阅确认，前端重置本地列表 |
| `DATA` | `phase:"SNAPSHOT"|"LIVE"`、`data:[TrackVO]`、`seq`、`part` | 统一数据消息（快照与实时同流异相） |
| `READY` | `count` | 快照完整结束，此后进入 LIVE |
| `CLOSED` | endTime | 订阅窗口结束（迟到补录仍可能来 DATA/LIVE） |
| `ERROR` | message | 业务错误（连接不断，可修正重试） |

### 2.3 前端全部逻辑

```js
SUBSCRIBED → list = []
DATA/SNAPSHOT → list.push(...data)
DATA/LIVE     → data.forEach(r => !seen.has(r.id) && (seen.add(r.id), list.push(r)))
READY  → render()
CLOSED/ERROR → toast
```

### 2.4 设计说明

- 快照与实时统一为 `DATA + phase`：同一条流的两个阶段，边界由独立信号 `READY` 表达
- `seq` 会话级单调递增，前端本期可忽略；未来断线补齐/丢包检测只需客户端上报 lastSeq，协议零改动
- ERROR 统一进业务报文，消除与通用层 `WsEnvelope` 的双轨混乱（通用信封仅保留协议级用途）
- DELETE 撤回不 overload 到 DATA：CDC 感知到 track DELETE 本期仅记日志，将来新增独立 type（如 `REVOKE {ids}`）

---

## 3. 核心设计

### 3.1 流式完整快照（零对账的根基）

`subscribe` 持 `pushLock`，游标分页流式下发**全量**符合条件的存量：

```
cursor = 0
loop:
  page = findByCondition(条件, afterId=cursor, LIMIT=500)   // id > cursor,走主键索引
  发送 DATA(phase=SNAPSHOT, part++)
  cursor = page 内最大 id
  直到 page < 500 条 → 发送 READY(count=累计条数)
sub.lastPushedId = max(cdcSeenMaxId, 快照最大id)             // 登记会话水位
```

- 每轮内存只持 500 条；`afterId` 游标走主键，不惧大存量
- 快照零截断，前端无对账合并逻辑

### 3.2 CDC 增量消费（`TrackCdcConsumer`）

- 消费 `xcdz.train.track`，解析 Debezium envelope：`op=c/u/r` 取 `after` 组装 `TrackDetailDTO`；`op=d` 记日志（REVOKE 预留）
- 时间字段：Debezium 将 DATETIME 序列化为 **UTC epoch millis**，按 `ZoneOffset.UTC` 转 `LocalDateTime`（用系统时区会差 8 小时）
- 数值字段：connector 配 `decimal.handling.mode=string`，消费端 `new BigDecimal(str)`
- `target_name` 由 `TargetNameCache` 补齐，零回库
- 持 `pushLock` 调用分发逻辑并推进 `cdcSeenMaxId`
- Kafka at-least-once 的重复消费 → 会话 `lastPushedId` 幂等消化

### 3.3 维度缓存（`TargetNameCache`）

- 启动全量加载 target 表 → `Map<targetId, targetName>`
- 消费 `xcdz.train.target` 的 c/u/d 事件增量维护（改名/新增实时生效）
- 联动职责：target 变更时通知订阅管理器**重算倒排路由分组**（见 3.4）

### 3.4 事件路由（倒排索引，替代全订阅扫描）

登记时展开、分发时路由：

```
订阅登记:
  keyword=null   → wildcard 全域集合
  keyword="华东" → 反查 TargetNameCache 命中 targetId 集合 → 注册 Map<targetId, Set<Subscription>>

事件分发:
  rows 按 targetId 分组 → 候选 = wildcard ∪ routeIndex.get(targetId)
  → 候选集内做 时间窗过滤 + 水位去重（每订阅独立窗口，谓词不可共享，保留）
```

- 复杂度 O(事件 × 命中订阅)，取代 O(事件 × 全订阅)
- target 表 CDC 事件驱动重算受影响 keyword 的分组
- 水位推进为"分发轮末按订阅统一收集后推进"，语义不变（只增不减；未匹配也算已考虑）

### 3.5 背压保护（`WsSessionManager`）

Spring 原生 `ConcurrentWebSocketSessionDecorator`：

- `register` 时包装：`sendTimeLimit=5000ms`、`bufferSizeLimit=512KB`（阈值入 yml）
- `send()` = 消息入有界缓冲 + 后台串行 flush，推送线程不再被慢客户端阻塞
- 溢出策略 = **断连**：缓冲超限/发送超时自动关闭该连接；客户端重连即走完整快照自愈
- 收益：快照分片下发期间慢客户端无法拖住 `pushLock`

### 3.6 可观测性（Micrometer，零新增依赖）

```
cdc.lag      DistributionSummary   now - source.ts_ms   （binlog事务提交→应用处理）
cdc.events   Counter{op=create|update|delete}
```

- 通过 `/actuator/metrics/cdc.lag` 即时可查；接 Prometheus 只需加 registry 依赖
- 低频汇总日志兜底（每分钟一条 max/avg）

---

## 4. 正确性保证

### 4.1 快照与增量衔接（原子性）

`subscribe`（WS 线程）与 `onCdcEvents`（Kafka 线程）同持 `pushLock` 串行：

```
subscribe(pushLock)                     onCdcEvents(pushLock)
  流式快照查询+下发                        事件路由分发
  sub.lastPushedId =                      cdcSeenMaxId 推进
    max(cdcSeenMaxId, snapshotMaxId)
```

- 快照查询期间到达的 CDC 事件在锁外排队 → 事后以 LIVE 事件重放，重复由 `lastPushedId` 去重
- 快照之后的新行 → CDC 正常分发
- `cdcSeenMaxId` 作为空快照时的水位底线，防 Kafka rebalance 重消费旧事件重推

### 4.2 投递语义

**at-least-once（Kafka）+ 幂等去重（会话 lastPushedId / 前端按 id）+ 完整快照边界（READY）**。
任何一环的重复都被下一环安全消化；缺口的可能来源（截断/漏扫）已在设计上消灭。

---

## 5. 基础设施部署（docker + kafka + connect）

### 5.0 部署拓扑与核心概念

```
Windows 宿主机                                    WSL2 Ubuntu (docker-compose ~/kafka/)
┌──────────────────────┐   binlog   ┌──────────────────────────────────┐
│ MySQL(train)  :3306  │◄───────────│ Debezium Connect  :8083(REST)    │
│                      │            │ Kafka             :9092/:29092   │
│ Spring Boot   :9700  │◄───WS──────│ kafka-ui          :8080          │
└──────────────────────┘  localhost └──────────────────────────────────┘
```

- **Connector 配置没有"配置目录"**：Kafka Connect（分布式模式）不读取本地配置文件，注册唯一入口是 REST API（`:8083/connectors`）。注册 JSON 只是 curl 提交的请求体，提交成功后配置持久化在 Kafka 内部 topic `connect-configs`，Connect 容器重启自动加载，**无需重复注册**
- 采集位点持久化在 `connect-offsets`，容器重启不丢、不重复采集

| 场景 | 需要重新注册 connector 吗 |
|---|---|
| 重启 Spring Boot 后端 | 不需要（后端只是 Kafka consumer） |
| 重启 connect / kafka 容器 | 不需要（配置与位点存于 Kafka 内部 topic） |
| 删除了连接器（`DELETE /connectors/xcdz-mysql`） | 需要 |
| Kafka 无持久卷重建、内部 topic 丢失 | 需要 |
| 修改连接参数（加表、换账号等） | 需要（PUT 更新） |

### 5.1 docker-compose（WSL2 内，完整版）

实际部署于 WSL2 `~/kafka/docker-compose.yml`（KRaft 单节点 + kafka-ui + Debezium Connect）：

```yaml
services:
  kafka:
    image: apache/kafka:latest               # KRaft 模式，无需 ZooKeeper
    container_name: kafka
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # 双监听器：PLAINTEXT 供宿主机（localhost:9092）接入，DOCKER 供容器网络（kafka:29092）互通
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,DOCKER://:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,DOCKER://kafka:29092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,DOCKER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on:
      - kafka

  connect:
    image: quay.io/debezium/connect:3.1      # Debezium 3.x 只发布到 quay.io，docker hub 的 debezium/connect:3.1 会 not found
    container_name: debezium-connect
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:29092
      GROUP_ID: connect-cdc
      CONFIG_STORAGE_TOPIC: connect-configs
      OFFSET_STORAGE_TOPIC: connect-offsets      # 采集位点持久化在 Kafka，重启不丢
      STATUS_STORAGE_TOPIC: connect-status
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
    depends_on:
      - kafka
```

要点说明：

- **双监听器是关键**：宿主机应用（application.yml 的 `localhost:9092`）与容器（connect / kafka-ui 的 `kafka:29092`）走不同 listener，advertised 地址配错任一侧，对应客户端都会连不上
- `apache/kafka` 镜像默认面向单节点（内部 topic RF=1），无需显式配置；若换用 `confluentinc/cp-kafka` 镜像则**必须**显式加以下三项，否则 `__consumer_offsets` 自动创建失败（RF=3 vs 1 broker），Connect worker 永远 join 不上 group，`POST /connectors` 报 "ensuring membership" 90s 超时：
  ```yaml
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
  ```
- 端口总览：kafka `9092`（宿主接入）/ `29092`（容器网络）、kafka-ui `8080`、connect REST `8083`

### 5.2 MySQL 侧准备（Windows 宿主机）

```sql
-- 前提确认
SHOW VARIABLES LIKE 'log_bin';         -- 需为 ON
SHOW VARIABLES LIKE 'binlog_format';   -- 需为 ROW（MySQL 8.x 默认满足）

-- CDC 专用账号
CREATE USER 'dbzuser'@'%' IDENTIFIED BY '<密码>';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dbzuser'@'%';
FLUSH PRIVILEGES;
```

### 5.3 容器启动与就绪检查

```bash
cd ~/kafka && docker compose up -d

# 轮询直到返回 []（约 30~60s）
curl http://localhost:8083/connectors
```

kafka 容器曾**无持久卷重建**时的坑：旧 connect 进程的 consumer 会用 broker 默认配置
（cleanup.policy=delete）抢先自动重建 connect-offsets/connect-configs/connect-status，
新 worker 报 `required cleanup.policy=compact, found delete` 拒绝启动。处理：

```bash
docker exec <kafka容器> kafka-topics.sh --bootstrap-server localhost:29092 --delete --topic connect-configs
# connect-offsets、connect-status 同理，删完重启 connect
```

### 5.4 Connector 注册

注册 JSON 已随项目入库：`deploy/xcdz-mysql-connector.json`。在 **Windows PowerShell** 提交：

```powershell
curl.exe -X POST -H "Content-Type:application/json" `
  -d "@D:\project\java\xcdz-backend-service-train-backend\deploy\xcdz-mysql-connector.json" `
  http://localhost:8083/connectors
```

坑位说明：
- 头值 `Content-Type:application/json` 冒号后**不能有空格**（PowerShell 会剥离引号导致头解析错误）
- 大 JSON 用 `curl.exe -d @file`，不要用 `Invoke-WebRequest -InFile`（chunked 传输会挂死 Jetty）

关键参数与易错点：

| 配置项 | 值 | 说明 |
|---|---|---|
| `database.hostname` | WSL 默认网关 IP（WSL 内 `ip route show default` 查看下一跳，如 172.20.80.1） | 容器 → Windows 宿主 MySQL 必须走 WSL 网关；`host.docker.internal` 是 Docker Desktop 专属，WSL2 原生 docker 不支持 |
| `database.server.id` | 5401（显式指定） | Debezium 3.x 不再默认随机，必须显式；勿与 MySQL 现有从库 ID 冲突 |
| `database.connectionTimeZone` | Asia/Shanghai | Windows MySQL 系统时区为中文（GBK）时必须配置，否则 JDBC 报 unrecognized time zone |
| `topic.prefix` + `database/table.include.list` | `xcdz` / `train.track,train.target` | 事件 topic 名 = `{prefix}.{db}.{table}`，须与 application.yml 的 `push.cdc.*-topic` 一致 |
| `snapshot.mode` | schema_only | 存量只走服务端订阅快照，CDC 只管增量（快照-流衔接前提） |
| `decimal.handling.mode` | string | 避免 DECIMAL 变 Base64，消费端 `new BigDecimal(str)` |
| `tombstones.on.delete` | false | 删除事件无需墓碑（消费端本期仅记日志） |
| `key/value.converter.schemas.enable` | false | 去除 envelope 内 Avro schema 冗余，减小消息体积 |

### 5.5 后端应用配置（application.yml 关键项）

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092   # WSL2 容器端口自动转发到 Windows localhost
    consumer:
      group-id: xcdz-push
      auto-offset-reset: earliest       # 无位点时从最早开始（topic 内均为 CDC 增量事件，不含存量）
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    listener:
      type: batch                       # 批量消费：一批事件一次持锁分发（水位/seq 批内连续）
      ack-mode: batch

push:
  ws:
    send-time-limit-ms: 5000            # WS 单次发送超时（背压，超限断连）
    buffer-size-limit-bytes: 524288     # 单连接发送缓冲上限（512KB）
  cdc:
    track-topic: xcdz.train.track       # 默认命名 {topic.prefix}.{db}.{table}
    target-topic: xcdz.train.target
```

- 依赖：`spring-kafka`、`spring-boot-starter-actuator`（Micrometer），无额外组件

---

## 6. 应用侧模块职责

| 模块 | 职责 |
|---|---|
| `push/TrackWsRequest` / `TrackWsMessage` | v2 协议双方向报文（SUBSCRIBE/UNSUBSCRIBE；SUBSCRIBED/DATA/READY/CLOSED/ERROR + phase/seq/part） |
| `push/TrackPushProcessor` | WS 业务入口：请求解析、订阅生命周期管理 |
| `push/TrackPushTask` | 订阅时流式完整快照 + CDC 增量分发（pushLock 串行、cdcSeenMaxId 水位） |
| `push/TrackSubscriptionManager` | 倒排路由索引（wildcard + keyword 分组、增删联动、轮末统一水位推进） |
| `push/cdc/TrackCdcConsumer` | `@KafkaListener` 批量消费 CDC 事件（UTC/decimal 解析）、路由分发、Micrometer 埋点 |
| `push/cdc/TargetNameCache` | target_id→target_name 维度缓存（启动全量 + target CDC 维护）、路由联动重算 |
| `websocket/session/WsSessionManager` | 会话管理 + decorator 背压（有界缓冲、超时断连） |
| `websocket/protocol`、`websocket/router`、`websocket/auth` | WS 通用层：协议信封、消息路由、握手鉴权（与业务解耦） |
| `service` + `mapper`（Track/Target） | 游标分页查询 `findByCondition(afterId)`；track/target 业务 CRUD |
| `simulate/DataSimulator` | 测试数据模拟写入（触发 binlog 变更） |
| `controller`（Target/Simulate） | Target 管理 REST、模拟器控制接口 |

---

## 7. 端到端验证

1. **采集端**：`curl http://localhost:8083/connectors/xcdz-mysql/status` → connect 与 task 均 RUNNING
2. **触发 binlog**：
   ```sql
   INSERT INTO train.track (target_id, metric_value, collect_time, create_time)
   VALUES ('TG-DEMO-002', 777.77, NOW(), NOW());
   ```
3. **Kafka**：kafka-ui（localhost:8080）查看 `xcdz.train.track` 数秒内出现变更事件
4. **WS 联调**：SUBSCRIBE → DATA/SNAPSHOT 分片完整（游标分页）→ READY（count=存量数）→ 写库 ~2s 内 DATA/LIVE
5. **幂等**：重启应用制造 Kafka 重复消费，确认前端按 id 不重不丢
6. **背压**：模拟慢消费者，验证缓冲超限断连 + 重连完整快照自愈
7. **可观测**：`/actuator/metrics/cdc.lag` 查看延迟分布，稳态 MAX ≈ 1s

---

## 8. 企业级定位与演进路径（本期不做）

| 维度 | 本期形态 | 生产演进 |
|---|---|---|
| 推送服务 | 单实例 | consumer group 集群 + WS 网关集群 + 会话状态外置 Redis |
| Connect | 单 worker | Connect 集群 HA |
| MySQL 采集 | 直连唯一实例 | 从库采集（改 connector hostname 即可，应用零改动） |
| 监控 | Micrometer 指标 | Prometheus + Grafana + 告警 |
| 过滤路由 | 应用内倒排 | 下沉 Kafka 分区 / Flink 物化 |

正确性语义（快照-流衔接、at-least-once + 幂等、路由、背压、协议）与生产同构，
单机验证结论可无损带到集群形态。

---

## 9. 关键设计决策记录

1. 感知层：Debezium CDC + Kafka 单通道（无降级开关、无兜底轮询）
2. 存量：服务端流式完整快照（无截断），前端零对账
3. 协议：v2（DATA/phase/seq/READY），双方向报文拆分独立类
4. DELETE：track 表删除事件仅记日志，协议预留 REVOKE
5. 背压：Spring decorator，溢出断连 + 重连快照自愈
6. 集群/监控体系：本期不做，列为演进项

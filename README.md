# kafka2db

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Java 8+](https://img.shields.io/badge/Java-8%2B-orange.svg)

本服务同时连接最多 10 个 Kafka 集群。每个集群可以订阅多个 Topic，并按 Topic 的外置映射配置，将一条 Kafka 消息中的全部明细批量写入 OceanBase、TDengine 或两个数据库。

[快速开始](#快速开始) · [配置说明](#外置配置) · [贡献指南](CONTRIBUTING.md) · [许可证](LICENSE)

## 快速开始

需要 JDK 8 或更高版本，以及 Maven 3.6.3 或更高版本。运行前准备 Kafka，并按目标数据库配置创建 OceanBase 表或 TDengine 超级表。

构建并运行测试，生成包含运行时依赖的 JAR：

```sh
mvn --batch-mode --no-transfer-progress verify
```

复制配置模板。Linux / macOS：

```sh
cp config/application.example.yaml application.yaml
```

Windows PowerShell：

```powershell
Copy-Item config/application.example.yaml application.yaml
```

编辑 `application.yaml`，填写 Kafka、数据库连接和字段映射，然后启动：

```sh
java -jar target/kafka2db-1.0.0.jar application.yaml
```

也可通过系统属性 `kafka2db.config` 或环境变量 `KAFKA2DB_CONFIG` 指定配置路径。优先级为命令行第一个参数、系统属性、环境变量、当前目录 `application.yaml`。

正常日志同时写入控制台和 `logs/kafka2db.log`，按天滚动并保留 30 天。本地配置、日志和失败消息目录已加入 `.gitignore`。

## 项目结构

```text
.github/                         GitHub Actions、Issue 和 PR 模板
config/application.example.yaml  外置配置模板
src/main/java/                   服务源码
src/main/resources/              日志配置
src/test/java/                   自动化测试
CONTRIBUTING.md                  贡献指南与提交规范
LICENSE                          MIT 许可证
pom.xml                          Maven 构建配置
```

## 处理语义

消息格式：

```text
消息头~记录1~记录2~||记录3~记录4~||...记录N~||
```

- 第一个 `~` 之前是唯一消息头。
- 后续内容按 `||` 分组，每个组内按 `~` 分割明细记录。
- 所有数据组共享消息头。
- 每个字段使用 `;` 分隔，空字段保留并最终写成 SQL `NULL`。
- 消息必须以 `||` 结束，每条明细必须以 `~` 结束。
- 默认删除报文中的 `\r`、`\n`，其他空格和 UTF-8 字符原样保留。
- 消息头字段暴露为 `headerField1..N`，明细字段暴露为 `boreholeField1..N`。
- 另外可以映射 `groupIndex` 和 `recordIndex`，两者均从 1 开始。

示例数据对应的源字段为：

```text
headerField1    煤矿编码
headerField2    煤矿名称
headerField3    数据上传时间

boreholeField1  测点编码
boreholeField2  传感器类型
boreholeField3  传感器位置
boreholeField4  测量值
boreholeField5  状态
boreholeField6  数据时间
```

## 数据库路由

每个 Topic 映射可以配置：

- 仅 `oceanbase`。
- 仅 `tdengine`。
- 同时配置 `oceanbase` 和 `tdengine`。

两个目标拥有各自的 `fields`，字段数量、顺序和类型互不相关。解析器先生成原始字段模型，再分别投影为 OceanBase 和 TDengine 数据。

### OceanBase

- 使用 MySQL JDBC 驱动。
- 一条 Kafka 消息生成一条多值 `INSERT ... ON DUPLICATE KEY UPDATE ...`。
- `uniqueKeyField` 必须包含在 `fields` 中，真实表上必须存在对应主键或唯一索引。
- 一条 SQL 在本地事务中提交。

### TDengine

- 使用 WebSocket JDBC 驱动。
- 一条 Kafka 消息生成一条多子表 `INSERT`。
- 使用 `USING 超级表 TAGS (...)` 自动创建不存在的子表，不自动创建超级表。
- `subtableSuffix` 指向一个目标列，该列的值与 `subtablePrefix` 拼接成子表名。
- 子表名只允许字母、数字和下划线，且必须以字母或下划线开头。
- `timestampField` 必须位于 `fields` 中，写入时固定排在普通列首位。
- `tags` 和普通字段顺序必须与 TDengine 超级表定义一致。

双库写入没有分布式事务。一个数据库成功、另一个数据库失败时，成功写入的结果不会回滚；失败文件会记录两个目标各自的状态，便于只修复失败目标。

## 重试与 offset

- Kafka 自动提交已强制关闭。
- 每条 Kafka record 处理完成后，只提交该 partition 的 `offset + 1`。
- 数据库首次写入失败后再重试 3 次，最多写入 4 次。
- 默认等待时间为 1 秒、2 秒、4 秒。
- 双库分别重试，已经成功的目标不会因另一个目标失败而重复执行。
- 字段数量、日期转换等确定性错误不进行数据库重试。
- 所有目标成功后提交 offset。
- 重试耗尽时，先将完整原始消息写入本地失败文件，落盘成功后提交 offset并继续下一条消息。
- 如果失败消息无法落盘，则不提交 offset，消费者重建后会再次获取该消息。
- offset 提交本身失败时消息可能重放。OceanBase 通过 upsert 幂等；TDengine 需要保证相同子表和时间戳重复写入符合业务预期。

失败文件位置：

```text
runError/yyyy-MM-dd/failed-messages.jsonl
```

每条 JSON 包含 Kafka 集群 ID、Topic、partition、offset、成功目标、失败目标、尝试次数、异常原因和完整原始消息。不发送死信 Topic。

## 外置配置

完整配置模板见 [`config/application.example.yaml`](config/application.example.yaml)。主要结构：

```yaml
runtime: {}
databases:
  oceanbase: {}
  tdengine: {}
kafkaSources:
  - id: kafka-a
    bootstrapServers: [127.0.0.1:9092]
    groupId: kafka2db-a
    concurrency: 1
    topics:
      - name: sensor-topic
        mapping: sensor-v1
topicMappings:
  sensor-v1:
    source: {}
    oceanbase: {}
    tdengine: {}
```

约束：

- `kafkaSources` 最多配置 10 个。
- 同一 Kafka 集群内 Topic 名不能重复。
- 每个已订阅 Topic 必须引用存在的 `topicMappings`。
- 配置修改后需要重启进程，当前版本不支持热更新。
- Kafka 和数据库账号密码允许明文配置。
- 启动阶段验证配置但不连接数据库，数据库连接按需创建。
- `max.poll.interval.ms` 必须大于单消息双库写入、SQL 超时和全部重试的最长耗时。
- 建议保留 `max.poll.records: 1`，一条 Kafka 消息本身已经是数据库批次。

支持的字段类型：

```text
string, int, long, decimal, date, datetime
```

日期支持 `yyyy-MM-dd`、`yyyyMMdd`；时间支持 `yyyy-MM-dd HH:mm:ss`、`yyyyMMddHHmmss` 和 `yyyy-MM-ddTHH:mm:ss`。

## 当前测试范围

- 共享头和多个 `||` 数据组解析。
- 头字段数、体字段数和结束符严格校验。
- 换行清理、空字段和中文内容处理。
- 数据类型转换与非法日期拒绝。
- OceanBase 单条多值 upsert SQL。
- TDengine 单条多子表 SQL。
- 首次写入加 3 次重试。
- 双库只重试失败目标。
- 完整失败消息 UTF-8 JSONL 落盘。
- 外置配置模板加载和启动期校验。

GitHub Actions 会在推送到 `main` 和提交 PR 时，使用 Java 8 与 Java 17 执行构建与测试，配置见 [CI 工作流](.github/workflows/ci.yml)。

## 参与贡献

欢迎提交 Issue 或 Pull Request。提交信息和 PR 标题采用 [Conventional Commits](https://www.conventionalcommits.org/zh-hans/v1.0.0/)，例如 `fix(parser): preserve empty message fields`；PR 标题由 GitHub Actions 自动检查。开发步骤与完整规范见 [贡献指南](CONTRIBUTING.md)。

## 许可证

本项目原创代码采用 [MIT 许可证](LICENSE)，允许在保留版权和许可声明的前提下使用、修改和分发，包括商业用途。第三方依赖继续遵循各自的许可证。

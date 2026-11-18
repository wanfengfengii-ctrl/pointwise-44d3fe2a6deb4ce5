# 河口监测实验室 · MonitorLake V1

仅使用 Python 3.11 标准库与 SQLite。V1 为接入员、质控员和研究员保存仪器分段采样的固定证据，完成单遍接纳、完整列块编码、不可变对象发布、固定快照读取、精确逐行汇总和 JSON 行导出。所有命令从项目根目录运行。

```sh
python3.11 -B -m unittest discover -s tests -v
python3.11 -B -m tools.scenarios verify
python3.11 -B -m tools.scenarios demo --samples 20000
python3.11 -B -m tools.scenarios init ./lab-lake
```

verify 运行完整回归，再用独立列表遍历核账，并追加分页业务、页长枚举与结构损坏核验，任何断言或测试失败均返回非零退出码。demo 在独立临时目录实际发布样例和一条长记录，汇总、关闭重开并核对旧成果，然后清理临时目录；分页演示再发布一条跨页长记录并逐行汇总。V1 demo 使用完整列块，允许完整候选、整列和完整行物化，不作长记录工作内存保证。init 只初始化指定仓库，不装载样例；重复执行保留用户数据。运行所需临时文件仅位于各测试自己的临时目录。run_manifest.json 列出辅助命令，.pointwise-runtime.json 是机器运行入口。

## 一天的业务

1. 接入员打开仓库，首次取得公开空快照 0；再次打开恢复当前发布指针。
2. 接入员提供批次 ID、来源文本和单遍记录源，创建未发布候选。
3. 每条记录通过资格检查并复制为独立规范值，按仪器出现顺序取得从 0 连续的批内序号。
4. 完整候选编码为一个 V1 段，保留有效性、行到段和段到采样的偏移。
5. 完整对象暂存、校验并封存，再在短事务中共同登记批次、固定成员和当前指针。
6. 质控员选定快照，读取带来源坐标的原始记录，逐条核对仪器原序。
7. 质控员在同一快照核对结构计数、有效读数笔数和精确整数和。
8. 研究员导出同一行的证据与摘要，关闭重开后复核；后来新增批次保留旧成果。

## 数据与口径

记录是含 label 和 samples 的映射，其他输入字段不参与本产品身份。label 是可重复、可为空的字符串，不是主键。samples 为 None 或列表，列表元素为 None 或列表，内层值为 None 或非布尔有符号 64 位整数。元组、浮点、第三层列表、布尔及越界整数拒绝。保留每一层 None 与空列表以及全部原序。规范化逐层复制列表；修改调用方输入和读取返回列表均不会改变已发布证据。

记录身份是 (batch_id, ordinal)。批次 ID 是非空字符串，按原值识别；来源描述是保留原值的字符串。零行批次也是完整批次。身份材料包含来源描述与有序规范化记录，采用确定性 JSON 保存完整身份并另存 SHA-256；冲突判定比较完整材料，不能仅凭摘要放行。完全相同的重放返回首次发布回执，即使当前指针已前移；异内容或异来源的相同 ID 拒绝。重放仍完整消费、检查此次源。

采样单位是仪器整数计数，不换算单位；计数无单位。摘要字段：
- outer_is_null 区分外层缺测与空外层；
- segment_count 是外层位置数，null_segment_count 统计 None 段，empty_segment_count 只统计 [] 段；
- non_null_sample_count 只计非 None 读数，sum 忽略 None，以 Python 任意精度整数累加；
- 没有有效读数时 sum 为 None，有读数抵消时 sum 为 0。

examples.json 的五条固定输入涵盖全部层级。B 的 [None, [], [None, 2, -2]] 对应段数 3、缺测段 1、空段 1、有效读数 2、和 0。C 的两个最大合法读数之和为 18446744073709551614。两个 A 是不同记录。tools.scenarios.reference_summary 使用独立朴素遍历与内置 sum 核账；long_rows 使用固定算术序列，不依赖随机数。

## Python API

```python
from io import StringIO
from monitorlake.service.lake import Lake
from monitorlake.catalog.database import EMPTY_SNAPSHOT_ID
from monitorlake.schema.errors import StorageError, CorruptSegment

with Lake("./lab-lake") as lake:
    receipt = lake.write("instrument-day-001", "instrument-export",
                         iter([{"label": "A", "samples": [None, [], [2, -2]]}]))
    rows = list(lake.read_rows(receipt.snapshot_id))
    summaries = list(lake.summaries(receipt.snapshot_id))
    sink = StringIO()
    delivered = lake.export(receipt.snapshot_id, sink)
```

write 返回不可变 WriteResult，字段为 batch_id、snapshot_id、row_count，并有 as_dict()。current_snapshot() 返回整数。read_rows(snapshot_id) 返回 RowRecord 迭代器；字段为 snapshot_id、batch_id、ordinal、source_description、observation，另有 label、samples 属性和 as_dict()。observation.as_dict() 返回 label/samples。summaries 返回 RowSummary 迭代器，包含相同坐标、label 和上述七个摘要字段，提供 as_dict()。快照参数为非布尔非负 SQLite 整数，未知 ID 拒绝。

export(snapshot_id, sink) 返回成功写入的 JSON 行数。每行顶层含 snapshot_id、batch_id、ordinal、source_description，record 为 label/samples，summary 为完整 RowSummary 字典。原始值和摘要来自同一行，不按标签关联两次扫描。文本 JSON 使用确定性字段次序，保留精确整数。sink.write 应写入完整文本并返回字符数或 None；短写视为错误。

## 存储与失败

catalog.sqlite3 保存批次完整身份、首次回执、不可变快照成员及当前指针；objects/<SHA-256>.v1 保存完整不可变段；staging 只保存本次候选临时文件。普通业务不覆盖已封存对象；同摘要复用前验证完整内容。一个批次一个完整段：魔数、版本、头长度、JSON 列目录和各列 JSON 数组。目录包含相对列内容区的字节偏移、字节长度、元素数和列摘要。六个列为 labels、outer_validity、row_segment_offsets、segment_validity、segment_value_offsets、sample_values。完整对象摘要和独立结构检查共同保护读取。

非法业务输入、批次冲突和未知快照抛 ValueError。源迭代异常原样传播，保留异常对象。对象缺失、字节损坏或目录/层级偏移不合法抛 CorruptSegment；元数据损坏抛 StorageError，不重置为空仓。close 可重复；关闭后公开业务入口及已创建迭代器的继续消费抛 RuntimeError。上下文管理器自动关闭。

源异常和尾部非法输入均整批不发布。编码、暂存和封存在元数据事务外完成，批次、回执、新快照和当前指针在同一事务提交。Lake(path, before_commit=callback) 提供确定性的提交前运维演练钩子：callback 接收 PublicationRecord，触发位置是全部元数据已写入、尚未提交；异常原样传播且事务回滚。可将 lake.before_commit 设回 None 后重试。封存但未引用对象允许留存，不是成功回执，不做历史回收。钩子不得重入业务接口或修改仓库。

读取在入口固定指定快照的有序引用；坏段可能在较早健康记录已输出后抛错，不能视为完整成功。导出端口失败原样传播，已写前缀不撤回，仓库不变。初始化仅接受全新空位置或完整既有目录，不接受以空状态替代损坏元数据。不承诺并发写入、进程崩溃恢复、压实或垃圾回收。

## 分页采样段（V2）

PAGING.md 定义分页入口的完整契约；这里给出交付格式与边界说明。

`Lake.write_paged(batch_id, source_description, rows, page_bytes)` 返回原 WriteResult；`page_bytes` 为 4096～65536 的非布尔整数，非法时不消费 `rows`；label 须能编码为 UTF-8 且至多 1024 字节。同一记录和内层段可跨任意多页，页边界不产生新记录或新内层段。

持久对象 `objects/<SHA-256>.v1` 中：`MLAKEPG2` 魔数、4 字节大端头长度、规范 JSON 头（version=2、row_count、body_length、page_bytes、page_count），随后是逐页帧：4 字节页内容长度 + 32 字节页 SHA-256 + 页内容。所有页内容按顺序拼接成一个逻辑令牌流：

- 记录：varint(label 字节数) + label 原始 UTF-8 字节 + 外层标签；
- 外层：`0x00` 表示 None，`0x01` + varint(段数)；
- 内层段：`0x00` 表示 None，`0x01` + varint(值数)；
- 值：`0x00` 表示 None，`0x01` + varint(zigzag 64 位整数)。

页内容严格不超过 `page_bytes`；除最后一页外每页恰为 `page_bytes` 字节，`page_count = ceil(body_length / page_bytes)`。长度与校验字段属结构性元数据，不计入页内容；标签、嵌套边界、续接字节和采样正文全部位于页内容中。仅由头、帧字段和 `page_count` 即可枚举全部页长，`monitorlake.segments.paged_format.verify_page_lengths` 与 `tools.paged_checks.page_envelope_checks` 对真实对象做该核验。

身份仍为 `{"rows":[{"label","samples"}...],"source_description"}` 的确定性 JSON 文本，与 V1 逐字节一致。分页路径从不整体构造该文本：接纳时按令牌流增量散列（不落盘全文），重放比较时从已发布对象逐页重新导出并与候选流分块比对；V1 已存身份经 SQLite 增量 BLOB I/O 分块读取。因此 `write`/`write_paged` 双向重放、改变 `page_bytes` 重放均返回首次回执，物理分页不进入业务身份。目录中分页批次只存对象摘要标记，完整身份由不可变对象精确承载；摘要相等仍会做全文比较。

工作内存：新增写入、对象校验安装、重放比较、分页汇总和重开时读取分页身份，额外载荷均以单页为界（读取保留一页，写入缓冲一页，散列与比较各保留固定分块），不缓存整条长记录、整列、完整身份文本、页目录或整批。`read_rows` 允许为正在返回的一行分配完整结果。输入自身、调用方保留的返回值、固定解释器开销、SQLite 有界页缓存、批次元数据和不随页数增长的整数状态不计。V1 段沿用原约定；export 无新增内存承诺。

## 模块与回归

schema 的 types/validation/normalization/errors 定义资格、独立规范值和错误。intake 的 source/ordinal/batch/staging 管理单遍游标、连续坐标、完整身份和暂存；intake 的 identity/paged_batch 增量生成语义身份并单遍编码分页候选。segments 的 layout/encode/decode/checksum 产生并验证完整列块；segments 的 paged_format 定义页帧、令牌与有界读写。catalog 的 database/repository/objects/publication 共同实现持久目录、不可变对象、原子发布与流式重放比较。consumption 的 scan/rows/summary/export 固定引用、保真还原、精确计算和证据交付（V1 与分页批次共用入口）。service 的 requests/results/lifecycle/lake 提供请求、回执、资源生命周期及公开入口。

六份原回归分别核对接纳与重放、嵌套结构及偏移、精确汇总、固定导出与重开、源中断与事务失败、完整性门禁。tests/test_paging.py 增加分页往返、跨页边界、跨入口/跨预算重放、源复用、发布失败重试、页长枚举与结构损坏。结构损坏夹具会重新计算页校验和与整体摘要，验证失败来自结构检查而非仅摘要比对。必要段损坏时原始读取和摘要均明确失败，较早健康快照仍可读。测试先断言实际结果，再输出部分 POINTWISE_SCENARIO JSON 供核账；这些输出不能代替业务断言。

`tools.scenarios verify` 在全部回归和原业务核账之后，再运行 `paged_examples.verify`（独立列表参照与失败演练）和 `tools.paged_checks.verify_checks`（联合快照、页长枚举、结构损坏）。`demo` 同样追加 `paged_examples.demo` 与 `paged_checks.demo_checks`，实际展示长行跨页、关闭重开和逐行汇总。`paged_examples.py memory` 分开测量写入、改变预算重放、重开与汇总峰值。

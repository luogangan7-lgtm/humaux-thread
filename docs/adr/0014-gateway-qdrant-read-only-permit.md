# ADR-0014: Gateway 的 Qdrant intra-cell 访问必须只读（`CellAccessMode::QdrantReadOnly`）

日期：2026-09-02 · 状态：Accepted · 影响面：`crates/infra-cell`（resource.rs / permit.rs / transport.rs）/ `bins/gateway`（bootstrap.rs / recall.rs）/ `xtask/src/architecture_check.rs`

## 背景

2026-08-30 裁决："no Gateway Qdrant access before a closed operation/method/path write denial
exists"。0141 之前 gateway 从未真正拨 Qdrant——`recall.search` 一直走
`DependencyUnavailable` 兜底（`SemanticRecallRuntime`/`with_semantic_recall` 只在测试里出现）。
本卡首次把 gateway 接进真实语义召回（0142），这条裁决因此第一次可执行：`crates/infra-cell` 此前
只有 `IntraCellMethod::{Get,Put,Post,Delete}` 与 `authorize_cell_access(registry, resource, ttl)`，
没有任何按调用者收窄写权限的机制——一个能读的 permit 天然也能写，纯靠"gateway 代码里不写 PUT"这
种约定，不是闭合裁决。

## 决定

1. `crates/infra-cell::resource` 新增 `CellAccessMode { ReadWrite, QdrantReadOnly }`，作为
   `ResourceEntry` 的一个字段，默认 `ReadWrite`（`ResourceEntry::new` 签名不变，现有全部构造点不受
   影响）；`ResourceEntry::with_access_mode(mode)` 是唯一收紧入口。
2. `authorize_cell_access` 铸造的 `CellAccessPermit` 把 `entry.access_mode()` 原样带上（mint 时快
   照，不是每次 `execute` 现查 registry）。
3. `HttpIntraCellTransport::execute` 在做任何 DNS 解析/拨号之前，检查
   `permit.access_mode() == QdrantReadOnly` 时的方法/路径白名单：`GET` 任意路径放行；`POST` 只放行
   `/points/search`、`/points/search/batch`、`/points/query`、`/points/scroll`、
   `/points/count` 这五个后缀；`PUT`/`DELETE`（任意路径）与其余 `POST`（`/points`、
   `/collections/*`、`/snapshots/*` 等写入端点）一律 `IntraCellError::WriteDenied`，不发出请求。
4. `bins/gateway/src/bootstrap.rs` 是唯一给 `IntraCellResource::QDRANT_REST` 建 registry entry 的
   文件，且必须 `.with_access_mode(CellAccessMode::QdrantReadOnly)`；`xtask
   architecture-check` 扫描该目录源码，见到 `QDRANT_REST` 附近没有
   `CellAccessMode::QdrantReadOnly`，或见到 `CellAccessMode::ReadWrite` 字面量，即判红。

## 依据

- §83.4 六条判据里的 判据5（destination identity/权限）此前只覆盖 TLS/API-key，没覆盖"这个 Cell 内
  资源允许哪些操作"——本裁决是对同一判据的补强，不是新开一条判据。
- Gateway 是唯一直接暴露给外部 MCP 调用方的进程；`recall.search` 是它对 Qdrant 唯一的合法用途，且
  语义上只需要只读检索（`/points/query`、`/points/search`）。给它 `ReadWrite` 权限是在没有任何调用
  方需要写的情况下，把攻击面留给"gateway 代码以后会不会不小心加一行写调用"这种未来时态。
- 收紧点选在 transport 层（每次 `execute` 强制检查），不是只在 bootstrap 层"不注册写权限就够了"：
  `HttpIntraCellTransport` 是唯一发出 HTTP 请求的地方，checked-in-one-place 比"信任调用方从不构造写
  请求"更强——同一个防线也保护了将来任何新增调用点。

## 否决的替代方案

| 方案 | 否决理由 |
|---|---|
| 只在 bootstrap.rs 里"不写构造 PUT/DELETE 请求的代码"（纯约定） | 没有可执行的失败模式；下一个 PR 加一行 `client.put(...)` 不会被任何东西拦下来 |
| 给 `IntraCellResource` 拆成 `QDRANT_REST_READ` / `QDRANT_REST_WRITE` 两个变体 | 变体数量膨胀，且 `IntraCellResourceRegistry`/`authorize_cell_access` 的判据2/6（同 Cell、caller 白名单）要为两个变体各注册一遍，本质是同一个物理资源硬拆成两个逻辑资源 |
| 在 `bins/gateway/src/recall.rs` 里手写请求方法白名单检查 | 只覆盖这一个调用点；下一个直接用 `IntraCellHttpTransport` 的 gateway 模块不会自动继承这条检查 |
| 把方法白名单做成 `ResourceEntry` 的运行时可变字段（可在 bootstrap 之后再收紧/放宽） | `ResourceEntry`/`IntraCellResourceRegistry` 现有设计是"部署时构造一次，之后不变"（§78.1），运行时可变字段是给自己开一个绕过口子 |

## 后果

- `crates/adapters` 的 worker 侧（`bins/retrieval-worker`、`bins/public-worker`）继续用
  `ReadWrite`（默认值不变）——它们确实需要 upsert/scroll-index 写路径，这条裁决只收紧 gateway 一侧。
- 注错：把 `bootstrap.rs` 里的 `.with_access_mode(CellAccessMode::QdrantReadOnly)` 删掉或换成
  `ReadWrite` ⇒ `xtask architecture-check` 红；把 `HttpIntraCellTransport::execute` 里的白名单检查
  删掉 ⇒ `cargo test -p humaux-infra-cell` 的只读 permit 单测（GET 放行、`/points/search` 放行、
  PUT/DELETE/其余 POST 拒绝）红。
- 升级路径：若未来某个 gateway 功能确实需要对 Qdrant 写（目前没有任何已知需求），走
  `ResourceEntry::with_access_mode(CellAccessMode::ReadWrite)` 显式声明，并更新本 ADR 与
  architecture-check 的白名单逻辑——不是绕过它。

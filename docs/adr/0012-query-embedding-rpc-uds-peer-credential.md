# ADR-0012: Gateway→retrieval-worker 查询嵌入 RPC 走 Unix domain socket + 内核 peer credential；SPIFFE/mTLS 留给 Production 档

日期：2026-09-02 · 状态：Accepted · 影响面：§4.2（进程凭证边界）/ §9159（Internal Workload Identity）/ §67.2（Single-Node 冻结档）/ `IntraCellResource` 闭集 / `bins/gateway` `bins/retrieval-worker`

## 背景

`recall.search` 查询侧需要为 query 做嵌入。§4.2 规定 gateway 只持认证 secret 与 `role_gateway` 池，
PLATFORM_RETRIEVAL provider credential 归 `humaux-retrieval-worker` 独有；2026-08-30 外部调研
（记忆 2eb0f470）裁决：gateway 经窄 async `RetrievalEmbeddingPort` 调 worker，请求只带
`{call_id, tenant_hint, raw_query}`，auth = workload-identity，禁止静态共享 secret。

调研 artifact 的核心代码假设存在「内部 TLS/workload-identity listener」并以 SPIFFE ID 校验对端。
实测仓库：`spiffe|mTLS|UnixListener|peer_cred` 命中 0；gateway 是裸 `TcpListener`；无 SPIRE、无证书发放。

## 决定

1. **传输**：worker 在 Unix domain socket 上 `axum::serve`（axum 0.8 原生 `impl Listener for UnixListener`）。
   gateway 用 tokio `UnixStream` 连接。不引入新 crate。
2. **身份**：worker 在读 body 之前取 `UnixStream::peer_cred()`，要求 `uid == 部署登记的 gateway uid`
   （由 worker 启动参数/env 给出，不硬编码）。不匹配 ⇒ 关闭连接，不进 handler。
3. **授权模型不新增**：沿用 `authorize_cell_access(registry, resource)` 与 `CellAccessPermit`，只给
   `IntraCellResource` 闭集加一个变体 `RETRIEVAL_EMBEDDING_RPC`（同步更新 `xtask architecture_check`
   的注册表扫描）。不引入 artifact 自造的 `IntraCellOperation` / `WorkloadIdentity`。
4. **升级路径**：`RetrievalEmbeddingPort` trait 是唯一跨进程边界；换成 mTLS/SPIFFE 时只动 worker 的
   listener 绑定与 gateway 的 client 连接两处，call 登记/幂等/预算/账本语义不变。

## 依据

- §9159「Phase Strategy」逐字：Single-node / OSS developer mode = `local trust + loopback/network isolation`；
  Production = `SPIFFE/SPIRE OR equivalent workload identity` + mTLS；「SPIFFE 是 reference，不写死 Domain」。
- §67.2 当前冻结档是 Single-Node Production。
- §9159「IP 不是内部服务身份」：peer credential 绑定 OS uid，严格强于 loopback。

## 否决的替代方案

| 方案 | 否决理由 |
|---|---|
| 现在上 SPIFFE/SPIRE + mTLS | 零基座，需从零建 agent/证书轮换/内部 TLS listener；spec 把它排在 Production 档，不阻塞单节点 |
| 静态共享 secret header | 2026-08-30 裁决明令禁止；破坏 gateway secret 边界 |
| loopback TCP 无身份 | §9159「IP 不是内部服务身份」；同机任意进程可连 127.0.0.1 |
| artifact 的 `IntraCellOperation`/`WorkloadIdentity` 第二套授权模型 | permit.rs 自述身份在 bootstrap 烙进 registry 是防自证的有意设计；一个资源变体已足够 |

## 后果

- 部署必须让 gateway 与 retrieval-worker 以**不同 OS 用户**运行（否则 uid 校验退化）。写进 §67.2 部署清单与 compose/systemd 模板。
- macOS 开发机 `peer_cred()` 同样可用（tokio 封装 `getpeereid`）。
- 注错：把 worker 期望 uid 改成别的值 ⇒ gateway 调用必须被拒；用 TCP 替换 UDS ⇒ architecture_check 红。

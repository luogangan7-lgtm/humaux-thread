# ADR-0003: 协议 choke point 与 egress choke point 拆分（Layer 0/1A/1B）

日期：2026-08-26 · 状态：Accepted · 触发：把 Qdrant 接上真实 HTTP 线路时发现 §83.4 把两个不同的 choke point 意外合成了一个——本 ADR 按裁决拆开，`docs/adr/0002-*` 已被 email deliverability schema 决策占用，故本决策落在 0003（文件名与 §83.4 编辑同批交付）。

## 背景：两个被合并的 choke point

2.3 冻结的 §83.4 只有一层：`crates/infra-egress/src/http.rs` 是全 workspace 唯一允许构造 `reqwest::Client`/`hyper::Client` 的地方，业务 Adapter 只能经 `OutboundPurpose`/`EgressPermit` 调用它。这把两个不同性质的问题合成了一个：

1. **协议问题**（与目的地无关）：全 workspace 只应有一处知道怎么造一个 HTTP client——超时、TLS 后端、连接池。这条约束对任何目的地都成立，无论内外。
2. **信任边界问题**（只对外部目的地成立）：跨出 Cell/tenant 边界的每一次数据披露都必须有 `EgressPermit` 且记入 `ops.data_disclosures`（§7.4）。这条约束只对**离开 Cell** 的调用有意义。

把 Qdrant 接线时，这两条挤在同一层立刻冲突：Qdrant 是 Cell 内部基础设施（§7.0/§7.2 明文："Sparse/BM25 是本地 Retrieval lane"），不是外部披露对象，但唯一的 client 构造点在 `infra-egress` 里，逼着 Qdrant 调用要么伪装成一次 `OutboundPurpose` 外部披露（写一条它没有资格写的账），要么绕开 `infra-egress` 另起一个 `reqwest::Client` 构造点（G80-3 判据1 立刻由绿转红）。三个选项（甲：套一个新 `OutboundPurpose`；乙：开一层新的 Cell-internal 概念；丙：直接沿用 `infra-egress` 现成 client）在 `crates/adapters/src/qdrant.rs` 模块头留了三年没有决断——本 ADR 是那个决断。

## 裁决

拆成三层：

```text
Layer 0  crates/infra-network/  唯一 raw client 构造点（协议中立）
Layer 1A crates/infra-egress/   外部出境：OutboundPurpose + EgressPermit + ExternalCall（§7.3 不变）
Layer 1B crates/infra-cell/     Cell 内访问：IntraCellResource + CellAccessPermit（不是 EgressPermit）
```

> Humaux Thread maintains one mandatory raw network choke point for all HTTP/gRPC communication. Network protocol or private address space does not determine whether a request constitutes data egress. Intra-cell resources and external recipients use distinct, non-convertible capability types. Access to an intra-cell resource requires explicit resource registration, workload identity, caller/operation authorization, validated destination identity, validated A/AAAA endpoint resolution, disabled automatic redirects, and network-layer enforcement. Intra-cell access does not create an ops.data_disclosures entry. ops.data_disclosures records processing or disclosure to an external legal/organizational recipient or processor, regardless of whether the network route uses the public Internet, VPN, PrivateLink, or another private transport. Therefore a self-hosted Qdrant instance operated inside the same Humaux Data Cell is an IntraCellResource, while a managed third-party service reached through a private endpoint remains an ExternalProcessor when the provider is a separate processing entity.

三层各自的实现要点：

- **Layer 0**：`infra-network::http::build_client`/`build_client_with_resolver` 是全 workspace 唯一构造 `reqwest::Client`（未来若引入 `hyper::Client`/`tonic::Channel` 同此）的地方。不含任何 `OutboundPurpose`/`EgressPermit`/`IntraCellResource` 语义——它对"这次调用要不要记账"没有任何意见，只对"全仓库只准一处会造这个东西"有意见。Layer 1A/1B 都只经它的 `pub use reqwest;` 拿类型，自己的 `Cargo.toml` 不直接依赖 `reqwest`，G80-3 判据1 的 manifest 集合因此仍是严格的单文件相等。
- **Layer 1A**（`humaux-infra-egress`，既有 `HttpExternalCall`）：语义完全不变——`OutboundPurpose` + `EgressPermit`，私人数据必须由 `EgressPermit` 派生，写 `ops.data_disclosures`。唯一改动是它的 `reqwest::Client` 现在由 Layer 0 构造，自己不再调用 `Client::builder()`。
- **Layer 1B**（`humaux-infra-cell`，新）：`IntraCellResource`（闭集枚举，第一版仅 `QDRANT_REST`）+ `CellAccessPermit`（不是 `EgressPermit`）。**不写 `ops.data_disclosures`，不进 `OutboundPurpose` registry**。独立的 `intra-cell-resource-registry`（与 `external-egress-registry` 并列，见 §83.4）。

## 第二轮调研修正（2026-08-26，带可核查来源）

第一轮把 Layer 0/1A/1B 的**代码拓扑**判对了，但裁决段落原文容易被读成「私有 IP ⇒ 不算披露」——这句话本身错。第二轮联网调研带回三处判据修正：

**修正一：判据必须是法律/组织实体边界，不是私有 IP。** EDPB *Guidelines 07/2020* 明文：成为 processor 的首要条件是 "being a separate entity in relation to the controller"；同一实体内部门之间不构成 processor 关系——"process data itself, using its own resources within its organisation … this is not a processor situation"（对应 GDPR Art.4(8)/Art.28）。**反向陷阱**：走 AWS PrivateLink 访问 Bedrock，网络路径完全 private——但这条路径按 `crates/domain/src/boundary.rs`（`NetworkRouteClass::ExternalNetwork` 的文档："The public Internet, a VPN, PrivateLink, or any other route leaving Humaux-operated infrastructure"）本身就分类为 `ExternalNetwork`，枚举里没有、也不需要再开一个"私网但外部"的变体——`ExternalNetwork` 本身就不含"私网 ⇒ 不算外部"的推断，这正是陷阱所在：路径私有与否对这条判据从头到尾没有意见。AWS 是独立法人 ⇒ **仍要写 `ops.data_disclosures`**。正向：自部署同 Cell 的 Qdrant 不是独立实体 ⇒ 不写。为把这条钉死为拓扑而不是纪律，`domain::boundary` 新增两个**正交、不可互转**的枚举——`NetworkRouteClass`（包去哪了）与 `RecipientClass`（谁在处理数据）——写账本的唯一充要条件是 `domain::boundary::requires_disclosure_record(RecipientClass) == true`，即 `RecipientClass ∈ {ExternalProcessor, ExternalIndependentRecipient}`；不是 `route == IntraCell`，不是私有 IP，不是协议是不是 HTTP。§7.4 已同步冻结这条判据。

**修正二：本 ADR"业界依据"一条诚实更正。** 第一轮列举 Google VPC Service Controls、Istio egress gateway、AWS PrivateLink、Qdrant 官方自托管建议为"按安全边界划线、不按协议划线"的四个同类先例——**这四者不能一概而论**。前三者（VPC Service Controls、Istio egress gateway、PrivateLink）确实是在组织/安全边界上做文章；但如果拿 **Kubernetes NetworkPolicy** 类比进这组先例，它其实是**反例**：NetworkPolicy 是纯 L3/L4 网络分段工具，只按 IP/port/namespace selector 生效，完全不区分"对方是不是独立法律实体"——把它当作"按安全边界（而非协议）划线"的佐证，会把"网络能不能连通"和"谁在处理数据"这两个不同维度的问题悄悄合并，正是修正一要拆开的那个错误。§83.4 的 Layer 0/1A/1B 拓扑本身没错，错的是把它当成"因为学了 NetworkPolicy 的隔离方式，所以隔离开了就代表判据成立"的证据链。

**修正三：DNS check/use 间隙必须真闭合，不是文档留白。** OWASP SSRF Cheat Sheet 点名 DNS rebinding / DNS pinning bypass：先校验 hostname、之后 HTTP client 自己再解析一次，两次解析可能不是同一批地址。`crates/infra-cell/src/transport.rs` 曾在模块头诚实登记这个缺口（ponytail 注）；本轮已关闭——`humaux-infra-network::http::build_client_with_resolver`（`reqwest::ClientBuilder::dns_resolver`，稳定于 0.12.28）让 infra-cell 的 `ValidatingResolver` 成为客户端唯一的 DNS 解析器，校验用的解析器与拨号用的解析器结构上是同一个调用，不再是纪律。闭合过程中额外发现：`reqwest` 默认遵循 `HTTP_PROXY`/`HTTPS_PROXY` 环境变量，若不显式关闭，Layer 1B 的自定义解析器会被系统代理绕过（代理自己解析目的地，`dns_resolver` 从未被咨询）——`ClientConfig` 新增 `trust_env_proxy` 字段，Layer 1B 显式关闭（同 Cell 流量绕代理本身也是 §83.4 判据4 的违反），Layer 1A 保持原有默认行为不变。

**修正四：registry 声明不能自证运行时事实。** 对应 Humaux 全局"不要拿声明校验声明"原则（旧系统坑5）：`humaux-admin q cell.resources` 新增为 live probe（§4.4），逐资源交叉核对 registry 声明（`configured_host`/`configured_port`）与本次运行独立取得的运行时事实（`resolved_ips`/`same_cell`/`private_route`/`identity_verified`/`reachable`）——两者必须来自两处代码路径，一处失效不能被另一处掩盖。

**未推翻的部分（NIST SP 800-207 / CISA 已核）**：Zero Trust 反对"内网 = 可信"，但不反对本次的 Layer 0/1A/1B 代码拓扑拆分本身；内部访问同样需要 workload identity + 授权 + 审计，只是审计目的地是 trace/metrics/security event，不是披露账本——这正是六条 AND 判据（下节）已经在做的事，Zero Trust 原则与本 ADR 的判据3/5/6 一致，不冲突。

## 为什么 Qdrant 绝不能写 `ops.data_disclosures`

1. **§7.4 冻结它是唯一权威出境账本**，删除传播（§37）据此判断外部 processor 是否需要收到删除请求。写进去后，一次正常的 Qdrant projection 写入会让删除传播把 Qdrant 误判为需要通知的 External recipient——它根本不是。
2. **投影关系不是披露关系**：PG（`private.memory_records` 等）是 Authority，Qdrant 是从 PG 可重建的 Projection（§17 自己的措辞）。把一次投影写入记成一次"披露"，混淆了"这份数据存在于两个地方"和"这份数据被交给了第三方"两件本质不同的事。
3. **`data_disclosures_finalized_total`（§53.5 INV-2 分母）与 `data_disclosures_reserved_unfinalized`（INV-3）是这两条不变式的真实指标**。Qdrant 的 upsert/search 是高频路径（每次 memory 写入、每次检索都会命中），一旦混入这两个计数器，它们就不再度量"披露有没有完成/有没有卡住"，而是被 Projection/Search 流量的体量稀释到失去信号——不是变慢，是**从这一刻起测的不是那两个东西了**。

`CellAccessPermit` 因此在类型上就不携带 `payload_sha256`/`data_class`，也没有到 `EgressPermit` 的任何转换函数——minting 一个 `CellAccessPermit` 结构上不可能顺手预留一条 `ops.data_disclosures` 行，不是靠调用者自觉不写。

## IntraCellResource 六条 AND 判据（缺一 fail-closed）

不做这六条会让"不是外部就什么都能请求"变成一个新后门：

1. **registry membership**：调用方只能传 `IntraCellResource` 枚举值，不能传任意 URL——`IntraCellResource` 没有 `From<&str>`/`FromStr` 到自身的路径，`crates/infra-cell/tests/intra_cell_topology_ui.rs` 用 `trybuild` 编译期证明这一点。endpoint 只由 `IntraCellResourceRegistry`（部署时灌入，非字面量）解析。
2. **same cell**：`source_cell_id == target.cell_id`，`authorize_cell_access` mint 时检查。
3. **resolved address ∈ Cell CIDR/Service IP 集**：DNS 解析后逐个地址核对；public IP / metadata IP（`169.254.169.254`）/ link-local / 别的 cell CIDR 一律拒——metadata/link-local 的拒绝是硬编码、不经过可配置的 CIDR 表，防止一次登记表误配置把它放回来。
4. **不允许 Internet/NAT route**：部署级判据，不伪装成 CI 闸——见 README "Intra-cell network deploy gate"。
5. **destination identity**：mTLS 或 TLS 证书 + 精确内部 DNS SAN + API key；同上，部署级。
6. **caller allowlist**：registry 写死哪些进程可访问该资源，`authorize_cell_access` mint 时检查。

本期代码级/类型级实现 1/2/3/6；4/5 写成 deploy gate 文档条目，不假装 CI 能测出网络拓扑和证书链。业界依据：Google VPC Service Controls、Istio egress gateway、AWS PrivateLink、Qdrant 官方自托管建议——都按安全边界划线，不按协议划线；本 ADR 的 Layer 0/1A/1B 拆分是同一原则在这个 workspace 里的落地。**这四者是六条 AND 判据（尤其判据3/4/5）的先例，不是「披露账本要不要记」那条判据的先例**——后者判据见下方「第二轮调研修正」的修正一/修正二，二者不要混用同一份先例列表。

## 已否决的选项

- **甲：套一个新 `OutboundPurpose::QDRANT_REST(EgressPermit)`**——会强迫每一次 Qdrant 调用都写 `ops.data_disclosures`，直接违反上面三条理由；也会让 `external-egress-registry`（一份"这个 workspace 会跟哪些外部世界通信"的清单）混入一个根本不外部的条目，读者无法再用它回答"我们的数据去过哪些第三方"。
- **乙：直接沿用旧的单层 `infra-egress` client，不开新层**——这就是 T5.4/T5.6 交付时模块头留下的第三个未决选项（丙），会让 Qdrant 绕开真正的资源边界判据（六条 AND 判据无处安放），且 G80-3 的"raw client 构造点唯一"这条判据会因为 Qdrant 需要不同的语义（不同的 permit 类型、不同的 registry）而被迫在同一个 trait 里分叉，最终两种调用方式挤在一个文件里，比拆成两个文件更难审计。

## 验收 gate

`xtask architecture-check` 的 G80-3（§83.4）：Layer 0 raw-client 构造点集合恰为 `{crates/infra-network/src/http.rs}`；Layer 1A `OutboundPurpose` 集合与 `external-egress-registry` 逐字相等；Layer 1B `IntraCellResource` 集合与 `intra-cell-resource-registry` 逐字相等；两个 registry 互不相交（反向哨兵）。六条注错（0a/a/b/b2/c/d/e/f，见 §83.4）覆盖：删除任一 Layer 0/1A/1B crate、adapters 里直接构造 `reqwest::Client`、`OutboundPurpose`/`IntraCellResource` registry 漂移、Qdrant 被同时塞进两个 registry、`IntraCellResource` 长出裸 URL 构造路径。

**第二轮新增验收**（对应上面四处修正）：

- 修正一：`crates/domain/src/boundary.rs` 的 `requires_disclosure_record` 只接受 `RecipientClass`，编译期就不存在把 `NetworkRouteClass` 传进去的路径；`external_processor_over_an_intra_cell_shaped_route_still_requires_disclosure` / `same_entity_resource_over_an_external_network_shaped_route_still_skips_disclosure` 两个测试钉死判据不随路由改变。`crates/adapters/src/disclosure.rs::reserve_in_txn` 的写入前置现在经由这个函数判定，不是 IntraCell/私有 IP/协议判断。
- 修正三：`crates/infra-cell/src/transport.rs` 的 `rebinding_between_successive_calls_is_rejected_by_the_same_resolver`（注错：同一 resolver 前后两次返回不同地址，第二次落在 metadata 段 ⇒ 必须失败）与 `stable_resolution_to_a_cell_address_succeeds_on_every_call`（正对照）——**这两个测试只覆盖 hostname 授权；IP 字面量授权（registry `host` 直接是地址，本仓库今天所有真实调用点的实际形态）是独立路径**，`ValidatingResolver` 对它从不被 `reqwest`/`hyper` 的 connector 调用，`execute()` 因此在 `send()` 之前另跑一遍相同的 metadata/link-local→CIDR 判据，对应注错：`ip_literal_host_pointing_at_metadata_address_is_rejected_without_a_connection`、`ip_literal_host_outside_registered_cidr_is_rejected_before_any_connection`（真实 `TcpListener` + panic-if-called 的 resolver，双重证明既没连接也没经过 resolver）。
- 修正四：`bins/admin/src/cell_resources.rs` 的 `empty_resolution_is_a_hard_error_not_a_health_reading`（注错：resolver 返回零地址 ⇒ 必须是 `Err`/非零退出，不能读成 `value = 0` 的成功探测）。

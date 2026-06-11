<div align="center">
<h1>sailing</h1>
</div>
<div align="center">

一个 Sans-I/O 的 [Raft](https://raft.github.io/) 共识算法库 — `no_std` + `alloc`、确定性、经过模糊测试加固。

[<img alt="github" src="https://img.shields.io/badge/github-al8n/sailing-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/sailing/ci.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/sailing?style=for-the-badge&logo=codecov" height="22">][codecov-url]
<img alt="license" src="https://img.shields.io/badge/License-Apache%202.0/MIT-blue.svg?style=for-the-badge" height="22">

[English][en-url] | 简体中文

</div>

`sailing-proto` 是共识内核：一个纯状态机，**没有 I/O、时钟、线程或运行时** —
应用提供存储、单调时间 `now` 与消息传递，并通过精简的 `handle_*` / `poll_*` 接口驱动它。
同一个状态机可以在 tokio、compio、embassy、裸机或确定性模拟器上以完全相同的方式运行。

## 状态

**预发布。** 共识内核与 TCP/TLS 流式传输层已完成并经过大量测试（VOPR 模糊测试 +
安全不变量断言 + etcd 交互测试集 + 多轮对抗性审查）；QUIC 传输层与参考异步驱动尚在开发中。
0.1 之前线缆格式与公开 API 仍可能变化。

## 特性

- **完整 Raft**：领导者选举（PreVote / CheckQuorum）、带流控与字节上限分批的日志复制、
  joint-consensus 成员变更（etcd 风格 `ConfChangeV2`）、领导权转移、快照安装与恢复。
- **线性一致读**：ReadIndex（领导者及跟随者转发）与可选的自校验租约快速路径。
- **构造级崩溃安全**：全程 persist-before-ack 持久化顺序；不可恢复的故障会 fail-stop
  （poison）而非破坏状态。
- **Sans-I/O 传输层**（按 feature 启用）：TCP（`tcp`，可用于 `no_std`）/ TLS（`tls`，rustls），
  含集群与节点身份握手、按节点路由、全程有界缓冲。
- **`no_std` + `alloc`** 内核；仅 TLS/QUIC 传输需要 `std`。

MSRV：**1.85**。

#### License

`sailing` is under the terms of both the MIT license and the Apache License (Version 2.0).

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT) for details.

Copyright (c) 2026 Al Liu.

[Github-url]: https://github.com/al8n/sailing/
[CI-url]: https://github.com/al8n/sailing/actions/workflows/ci.yml
[codecov-url]: https://app.codecov.io/gh/al8n/sailing/
[en-url]: https://github.com/al8n/sailing/tree/main/README.md

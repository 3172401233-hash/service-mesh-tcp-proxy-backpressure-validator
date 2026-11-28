# service-mesh-tcp-proxy-backpressure-validator

验证 TCP 代理背压、缓冲和连接迁移。

## 模型说明

**本仓库中所有连接均为本地模型**：不建立任何真实 TCP
socket，上下游、代理、迁移与对端确认全部由 `src/proxy.js`
中的确定性状态机在同一进程内模拟。这保证了背压、重传和
迁移场景可以在 `node --test` 下可重复地验证（Node.js 24.16.0）。

## 使用

```js
const { runSimulation } = require('./src/proxy');

const report = runSimulation(
  {
    upstreamWindow: 65536,   // 上游读窗口
    downstreamWindow: 16384, // 下游写窗口（触发暂停的缓冲上限）
    resumeThreshold: 8192,   // 恢复阈值，默认 downstreamWindow / 2
    ackTimeoutMs: 1000,      // ACK 超时，触发重传
  },
  [
    { type: 'upstream-data', bytes: 20000 },
    { type: 'downstream-ack', bytes: 12000 },
    { type: 'migrate' },
    { type: 'late-data', proxy: 0, bytes: 4000 },
    { type: 'upstream-fin' },
    { type: 'tick', ms: 500 },
    { type: 'cancel' },
  ],
);
```

## 输入事件

- `upstream-data`：上游到达的字节；暂停或窗口满时记为 `defer-upstream`。
- `downstream-ack`：下游累计确认，驱动缓冲排空与恢复判断。
- `upstream-fin`：半关闭；缓冲排空后才 `close-downstream`。
- `migrate`：连接迁移；新代理继承序列号并重发未确认字节。
- `late-ack` / `late-data`：旧代理晚到的确认或数据，计为重复并丢弃。
- `tick`：推进本地时钟，检测 ACK 超时并重传在途字节。
- `cancel`：取消会话，丢弃缓冲。

## 输出

- `bufferPeak`：运行期间缓冲未确认字节的峰值。
- `duplicateBytes`：旧代理晚到造成的重复字节数。
- `retransmittedBytes`：超时或迁移触发的重传字节数。
- `deferredBytes`：因背压被延迟读取的上游字节数。
- `actions`：带时间戳的动作日志（`pause-upstream`、`resume-upstream`、
  `retransmit`、`migrate`、`discard-duplicate`、`half-close`、
  `close-downstream`、`cancel` 等）。
- `finalState`：暂停、半关闭、取消与剩余缓冲等终态。

## 暂停 / 恢复 / 迁移条件

- 暂停：`bufferedBytes >= downstreamWindow`。
- 恢复：`bufferedBytes <= resumeThreshold`。
- 迁移：旧代理标记为 stale，未确认字节由新代理立即重发；
  此后旧代理的任何晚到数据/确认都计为重复并丢弃。

## 测试

```sh
node --test
```

覆盖慢下游（暂停/恢复）、连接迁移（重发 + 晚到去重）、
半关闭（排空后关闭）、ACK 超时重传和取消。

# go-token-bucket-rate-limiter

一个纯 Go 实现的并发安全、按 key 独立的令牌桶速率限制器。仅包含后端库、CLI 演示和自动化测试，不依赖数据库、Docker、云服务或任何第三方模块。

## 功能

- 每个字符串 key 拥有独立的令牌桶，初始为满容量
- token 随时间连续补充，且不超过容量上限
- `Allow` / `AllowN`：原子判断并一次性消耗，不存在部分扣减
- `Reset`：重置指定 key，使其下次请求重新从满容量开始
- `CleanupIdle`：按空闲时间回收长期未使用的 key
- 统计信息：累计放行 token 数、累计拒绝次数、当前活跃 key 数、累计清理 key 数
- 可注入时钟（`Clock` 接口），测试无需真实 sleep
- 多 goroutine 并发安全，同一 key 不会超发

## 使用示例

```go
l, err := ratelimiter.NewLimiter(3, 1, 2*time.Second) // 容量 3，每秒补 1 个，空闲 2s 可回收
if err != nil {
    // 参数非法：capacity/rate 必须为正，idleTTL 不能为负
}

l.Allow("alice")         // 消耗 1 个 token
l.AllowN("alice", 2)     // 原子消耗 2 个；不足 2 个时一个都不扣
l.Reset("alice")         // 重置 alice 的桶
removed := l.CleanupIdle() // 清理空闲超过 2s 的 key，返回清理数量
stats := l.Stats()       // Stats{Allowed, Denied, ActiveKeys, Cleaned}
```

### 注入可控时钟（测试用）

```go
clock := ratelimiter.NewManualClock(time.Now())
l, _ := ratelimiter.NewLimiter(5, 2, time.Minute, ratelimiter.WithClock(clock))
l.AllowN("k", 5)
clock.Advance(1500 * time.Millisecond) // 补充 3 个 token，无需真实 sleep
```

## 行为约定

- **AllowN 原子性**：剩余 token 不足 n 时整体拒绝，不消耗任何 token。
- **非法参数**：`n < 1` 或 `n > capacity` 的 `AllowN` 直接返回 false，不改变任何状态和统计。
- **Reset 与统计**：`Reset` 只删除指定 key 的桶（使 `ActiveKeys` 减一），**不影响** `Allowed`、`Denied`、`Cleaned` 计数。
- **空闲判定**：key 最近一次被访问（无论放行或拒绝）距今超过 idleTTL 即视为空闲；`idleTTL = 0` 表示禁用清理，`CleanupIdle` 不做事。
- **清理隔离性**：`CleanupIdle` 只删除空闲 key，其他 key 的 token 状态完全不受影响。

## 运行测试

```powershell
# Windows 下若默认构建缓存目录不可写，可指定本地缓存：
$env:GOCACHE = "$PWD\.gocache"

go test ./... -count=1 -v
```

如本机装有 gcc（启用 cgo），可额外运行竞态检测：

```powershell
$env:CGO_ENABLED = "1"; go test ./... -count=1 -race
```

测试覆盖：首次请求满容量、token 耗尽、时间补充、容量上限、AllowN 原子性与非法参数、不同 key 隔离、Reset（含对统计无影响）、空闲清理（含禁用与隔离性）、并发不超发、统计信息。

## 运行 CLI 演示

```powershell
go run ./cmd/demo
```

演示内容：突发请求耗尽容量、等待补充、key 间隔离、AllowN 原子消耗、Reset、空闲清理与最终统计输出。

## 项目结构

- `limiter.go` — Limiter 核心实现（令牌桶、清理、统计）
- `clock.go` — `Clock` 接口、`RealClock` 与可手动推进的 `ManualClock`
- `limiter_test.go` — 全部自动化测试
- `cmd/demo/main.go` — 命令行演示程序

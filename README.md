# go-token-bucket-rate-limiter

一个纯 Go 实现的并发安全、按 key 独立的令牌桶（token bucket）速率限制器。
仅包含后端库、命令行演示和自动化测试：不依赖数据库、Docker、云服务或任何第三方包（只用标准库）。

## 功能特性

- 每个字符串 key 拥有独立的令牌桶，初始为满容量
- token 随时间连续补充，上限为容量（惰性补充，无后台 goroutine）
- `Allow` / `AllowN`：`AllowN` 原子地判断并一次性消耗，绝不部分扣减
- `Reset`：删除某个 key 的桶，下次请求重新从满容量开始
- `CleanupIdle`：显式清理空闲时间超过阈值的 key，不影响其他 key 的状态
- 可注入时钟（`Clock` 接口），测试无需真实长时间 sleep
- 统计信息：累计放行数、累计拒绝数、当前活跃 key 数、累计清理 key 数
- 全部方法可安全并发调用（单个互斥锁保护，保证 `AllowN` 的原子性）

## 快速开始

```go
package main

import (
	"fmt"
	"time"

	"ratelimiter"
)

func main() {
	// 容量 5，每秒补充 2 个 token，key 空闲 30 秒后可被清理
	limiter := ratelimiter.NewLimiter(5, 2, 30*time.Second)

	fmt.Println(limiter.Allow("user-1"))     // true，消耗 1 个 token
	fmt.Println(limiter.AllowN("user-1", 3)) // true，原子消耗 3 个 token
	limiter.Reset("user-1")                  // 重置为满容量
	limiter.CleanupIdle()                    // 清理空闲超时的 key
	fmt.Println(limiter.Stats())             // 查看统计快照
}
```

## API 说明

### `NewLimiter(capacity int64, refillPerSecond float64, idleTTL time.Duration, opts ...Option) *Limiter`

- `capacity`：每个桶的最大 token 数，必须为正数，否则 panic
- `refillPerSecond`：每秒补充的 token 数（可为小数），必须 >= 0；为 0 表示不补充
- `idleTTL`：key 空闲多久后可被 `CleanupIdle` 回收；<= 0 表示禁用空闲清理
- `opts`：可选配置，目前支持 `WithClock(c Clock)` 注入自定义时钟（默认使用系统时钟）

### `Allow(key string) bool`

为 `key` 消耗 1 个 token，返回是否放行。等价于 `AllowN(key, 1)`。

### `AllowN(key string, n int64) bool`

原子地为 `key` 消耗 `n` 个 token：要么全部扣减并返回 `true`，要么一个都不扣并返回 `false`，
不存在部分扣减。`n <= 0` 是不消耗、不计入统计的空操作（返回 `true`）；
`n` 大于容量时永远无法满足，始终返回 `false`。

### `Reset(key string)`

删除 `key` 的桶，该 key 的下一次请求从满容量重新开始；对不存在的 key 是空操作。

**Reset 对统计信息的影响**：`Reset` 不修改 `Allowed`、`Denied`、`CleanedKeys` 任何计数器；
仅当被重置的 key 存在时，`ActiveKeys` 会减少（该 key 再次被使用时会重新计入）。

### `CleanupIdle() int`

移除所有空闲时间（距最近一次 `Allow`/`AllowN` 访问）达到或超过 `idleTTL` 的桶，
返回本次移除的数量，并累加到 `CleanedKeys`。只移除空闲 key，其余 key 的 token 状态完全不受影响。
本库不启动后台 goroutine，空闲清理完全由调用方显式触发。

### `Stats() Stats`

返回一致性的统计快照：

| 字段          | 含义                                   |
| ------------- | -------------------------------------- |
| `Allowed`     | 累计放行的 `Allow`/`AllowN` 调用次数   |
| `Denied`      | 累计拒绝的 `Allow`/`AllowN` 调用次数   |
| `ActiveKeys`  | 当前持有桶的 key 数量                  |
| `CleanedKeys` | 累计被 `CleanupIdle` 清理掉的 key 数量 |

计数器单调递增：`Reset` 和 `CleanupIdle` 都不会减少 `Allowed`/`Denied`，
只有 `CleanupIdle` 会增加 `CleanedKeys`。

### 时钟注入

```go
clock := ratelimiter.NewFakeClock(time.Now()) // 手动推进的测试时钟
limiter := ratelimiter.NewLimiter(4, 2, time.Minute, ratelimiter.WithClock(clock))

limiter.AllowN("k", 4)        // 耗尽
clock.Advance(time.Second)    // 推进 1 秒，补充 2 个 token
limiter.AllowN("k", 2)        // true
```

## 运行 CLI 演示

```powershell
go run ./cmd/demo
```

演示内容：突发请求耗尽容量、不同 key 相互独立、`AllowN` 原子消耗、
真实 sleep 1 秒后的 token 补充、`Reset` 恢复满容量、空闲清理以及最终统计输出。

## 运行测试

```powershell
# 全部测试（不依赖真实长时间 sleep，时间逻辑由 FakeClock 驱动）
go test ./...

# 重复运行 + 多核调度，验证并发场景稳定性
go test ./... -count=20 -cpu=4

# 竞态检测（需要本机启用 cgo，即安装 gcc/mingw；本仓库代码不依赖 cgo）
go test ./... -race
```

测试覆盖：首次请求满容量、token 耗尽、时间补充、容量上限、`AllowN` 原子性（含大于容量、非正数入参）、
不同 key 隔离、`Reset`（含对统计无影响）、空闲清理（含边界与禁用）、并发竞争（精确不超发）、统计信息。

## 项目结构

```
clock.go          Clock 接口、RealClock、FakeClock（可注入时钟）
limiter.go        Limiter 核心：Allow / AllowN / Reset / CleanupIdle
stats.go          Stats 快照
limiter_test.go   功能与并发测试
clock_test.go     时钟测试
cmd/demo/main.go  命令行演示
```

## 设计说明

- **原子性**：整个 `Limiter` 由一把 `sync.Mutex` 保护，`AllowN` 的"检查余额 + 扣减"在同一临界区内完成，
  因此并发下同一 key 不会超发，也不会出现部分扣减。
- **惰性补充**：token 在每次访问时按流逝时间结算（`min(capacity, tokens + elapsed*rate)`），
  不依赖定时器或后台 goroutine。
- **显式清理**：空闲 key 只通过 `CleanupIdle` 回收，库本身不启动任何后台任务，生命周期完全由调用方控制。

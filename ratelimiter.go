package gqueue

import (
	"context"
)

// Token bucket, run atomically inside Redis.
//
// Three details matter:
//  1. The clock is Redis's own (TIME), not the caller's. With several worker
//     pods, each pod's clock can be off by a bit; one shared clock means the
//     refill math is consistent no matter which pod calls.
//  2. last_refill is always stored. (Bug fixed 2026-11-07: it used to be saved
//     only when refill > 0. The first call always has elapsed = 0, so it was
//     never saved, every later call started from elapsed = 0 again, and the
//     bucket never refilled after the initial burst.)
//  3. last_refill advances by exactly the time that was "spent" on whole tokens,
//     not to `now`, so the fractional remainder is kept instead of thrown away
//     (otherwise the real rate drifts below the configured rate).
const luaAllow = `
local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6

local tokens = tonumber(redis.call('HGET', key, 'tokens'))
local last_refill = tonumber(redis.call('HGET', key, 'last_refill'))
if not tokens then tokens = max_tokens end
if not last_refill then last_refill = now end

local refill = math.floor((now - last_refill) * refill_rate)
if refill > 0 then
    tokens = math.min(max_tokens, tokens + refill)
    last_refill = last_refill + refill / refill_rate
end
-- A full bucket must not "bank" idle time for a later burst.
if tokens >= max_tokens then
    last_refill = now
end

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end
redis.call('HSET', key, 'tokens', tokens, 'last_refill', last_refill)
return allowed
`

// Allow checks the token bucket for this queue. Returns true if a job can be processed.
// maxTokens = bucket size (largest burst), refillRate = tokens added per second.
//
// If Redis is unreachable it returns true (fail open): jobs keep flowing and the
// cap is temporarily not enforced. The alternative, fail closed, would stop all
// processing whenever Redis hiccups.
func (q *Queue) Allow(ctx context.Context, maxTokens int, refillRate float64) bool {
	key := "ratelimit:" + q.Name
	result, err := q.client.Eval(ctx, luaAllow, []string{key}, maxTokens, refillRate).Int()
	if err != nil {
		return true
	}
	return result == 1
}

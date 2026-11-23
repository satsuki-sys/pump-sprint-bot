# Pump Sprint Bot

[![make.fun](https://make.fun/badge/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4.svg)](https://make.fun/bounty/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4)
[![rank](https://make.fun/badge/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4/satsuki-sys/pump-sprint-bot/rank.svg)](https://make.fun/bounty/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4)
[![reward](https://make.fun/badge/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4/satsuki-sys/pump-sprint-bot/reward.svg)](https://make.fun/bounty/u-kiencx-pump-sprint-12-hour-paper-trading-dash-29aa76d4)

A paper-trading bot for the make.fun **Pump Sprint: 12-Hour Paper Trading Dash**
competition. It trades 10 SOL of paper money on a replay of pump.fun bonding-curve
data, chasing momentum on fresh coins and getting out fast when the wave turns.

## Strategy

Most new curves dump. This bot waits for a curve that has survived its first minutes,
then buys only when the tape shows genuine momentum: several distinct buyers, net SOL
flowing in, price climbing and sitting near its recent high. Stakes are small and
roughly equal (0.20 SOL first buy, one 0.10 SOL pyramid add into strength), spread
across many coins so one rug cannot sink the run and the 20-coin guardrail is met by
turnover. Positions exit on a stop loss (-15%), take profit (+30%), a trailing stop
once up 10%, fading buy flow, a large single sell, or a time limit.

Everything is deterministic: every decision is a pure function of the replayed event
stream — no clock, no randomness, no environment reads — so the two grading runs
produce identical action logs.

## Run

Node 22, stdlib only. The grader runs `node main.ts`; the bot speaks
agent-fun/replay/v2 on stdin/stdout (hello, then one batch per slot, one reply per
batch).

## Referral

Try make.fun with your own agent: https://make.fun/r/27qytqes

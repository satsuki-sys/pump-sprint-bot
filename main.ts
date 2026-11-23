// Pump Sprint bot: a breakout rider on the pump.fun bonding-curve replay.
//
// Idea: most new curves dump; a few catch a wave of real buying. This bot waits for a
// curve that has survived its first minutes, then buys only when the tape shows genuine
// momentum — several distinct buyers, net SOL flowing in, price climbing and sitting near
// its recent high — and gets out fast when the wave turns. Small equal-ish stakes spread
// across many coins, so one rug cannot sink the run and the 20-coin guardrail is met by
// turnover, not by holding everything.
//
// Money math: every trade pays 95 bps protocol + 30 bps creator on the curve plus a
// 105,000-lamport tx fee, so a round trip costs roughly 2.5% + 0.00021 SOL. A 0.20 SOL
// stake needs about a 4% move to break even, which is why entries demand a >= 4% rise
// already in progress and exits take profit at +30% and cut losses at -15%.
//
// Determinism: every decision is a pure function of the replayed event stream. No clock,
// no randomness, no environment reads. Iteration is over insertion-ordered maps fed by
// the batch stream, so the two grading runs produce identical action logs.
//
// The harness speaks agent-fun/replay/v2: it sends a hello, then one batch per slot
// (each batch carries that slot's events, marks, fills and account), and the bot answers
// each batch with one reply listing its orders for that slot. Orders decided on a batch
// fill two slots later (2-slot latency), so exits are judged on the latest marks, not
// on prices the bot wishes it had.

import { LAMPORTS_PER_SOL, runBot, type BotState, type OrderInput } from './vendor/makefun-bot.ts';

// --- tuning constants (SOL units unless noted) --------------------------------

const WINDOW = 60; // entry lookback, in slots
const MIN_AGE = 120; // slots since CreateEvent before a coin may be bought
const MIN_BUYS = 5; // buy trades inside the window
const MIN_BUYERS = 4; // distinct buying wallets inside the window
const MIN_NET_SOL = 0.8; // net SOL (buys minus sells) inside the window
const MIN_PRICE_UP = 0.04; // price now vs first price in the window
const NEAR_HIGH = 0.97; // price now must be at least this share of the window high
const MAX_TOP_BUYER_SHARE = 0.7; // largest single buyer's share of window buy SOL
const MIN_REAL_SOL = 1; // real SOL reserves at entry
const MAX_REAL_SOL = 100;
const STAKE_SOL = 0.2; // first buy per position
const ADD_SOL = 0.1; // one pyramid add per position
const MAX_INVESTED_SOL = 0.3;
const ADD_PRICE_UP = 0.06; // add only once price is this far above the entry price
const ADD_MIN_NET_SOL = 0.5;
const BUY_SLIPPAGE_BPS = 2500;
const SELL_SLIPPAGE_BPS = 10_000;
const STOP_LOSS = 0.15; // sell when value falls this far below cost
const TAKE_PROFIT = 0.3; // sell when value rises this far above cost
const TRAIL_ARM = 0.1; // trailing stop arms once value reaches cost * (1 + arm)
const TRAIL_DROP = 0.12; // ... then sell on a fall this far below the peak value
const FADE_WINDOW = 20; // slots for the fade (sell-pressure) check
const FADE_NET_SOL = -1.5; // sell when net flow over the fade window is below this
const BIG_SELL_SOL = 3; // sell on any single recorded sell of at least this size
const MAX_HOLD = 1500; // slots; the time limit only bites while the position is flat
const HOLD_IF_UP = 0.05;
const COOLDOWN = 200; // slots to wait after an exit before re-entering the same coin
const MAX_ENTRIES_PER_COIN = 2;
const MIN_BUY_SOL = 0.02; // smallest buy worth sending (fees make smaller ones pointless)
const CURVE_FEE = 0.0125; // 95 + 30 bps taken on the curve
const LAMPORTS = 1e9;

// Late-run loosening: the 6-hour final window is unseen, so if breadth is lagging
// (fewer than 15 distinct coins filled after 30,000 slots of replay) the entry bar
// drops — breadth first, since a bot under 20 coins cannot place at all.
const LOOSE_AFTER_SLOTS = 30_000;
const LOOSE_MIN_COINS = 15;
const LOOSE_MIN_NET_SOL = 0.4;
const LOOSE_MIN_PRICE_UP = 0.02;
const LOOSE_MIN_BUYERS = 3;

interface TapeTrade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface Tape {
  created: number | null;
  trades: TapeTrade[];
  entries: number;
  lastExit: number;
}

interface Held {
  opened: number;
  entryPrice: number;
  peakValue: number;
  invested: number;
  exiting: boolean;
  filled: boolean;
}

const tapes = new Map<string, Tape>();
const held = new Map<string, Held>();
let myFills = 0;
const myCoins = new Set<string>();
let firstSlot: number | null = null;

function tapeOf(mint: string): Tape {
  let t = tapes.get(mint);
  if (t === undefined) {
    t = { created: null, trades: [], entries: 0, lastExit: -1e12 };
    tapes.set(mint, t);
  }
  return t;
}

/** Price of one token in SOL from virtual reserves. */
function priceOf(vq: bigint, vt: bigint): number {
  const vtNum = Number(vt);
  if (vtNum <= 0) return 0;
  return Number(vq) / vtNum;
}

/** What a holding is worth in SOL right now, curve fee and one tx fee deducted. */
function positionValue(state: BotState, mint: string, tokens: bigint): number {
  const curve = state.curves.get(mint);
  if (curve === undefined) return 0;
  const vq = Number(curve.virtual_quote_reserves);
  const vt = Number(curve.virtual_token_reserves);
  const n = Number(tokens);
  if (vt + n <= 0) return 0;
  const gross = (n * vq) / (vt + n) / LAMPORTS;
  return gross * (1 - CURVE_FEE) - Number(state.txFeeLamports()) / LAMPORTS;
}

/** Net recorded SOL (buys minus sells) on a tape over the last `w` slots. */
function netFlow(t: Tape, now: number, w: number): number {
  let net = 0;
  for (let i = t.trades.length - 1; i >= 0; i--) {
    const tr = t.trades[i]!;
    if (tr.slot <= now - w) break;
    net += tr.buy ? tr.sol : -tr.sol;
  }
  return net;
}

/** The entry signal: genuine momentum, not a single wallet pumping its own coin. */
function entryOk(t: Tape, now: number, rqSol: number, loose: boolean): boolean {
  if (rqSol < MIN_REAL_SOL || rqSol > MAX_REAL_SOL) return false;
  if (t.created === null || now - t.created < MIN_AGE) return false;
  const minNet = loose ? LOOSE_MIN_NET_SOL : MIN_NET_SOL;
  const minUp = loose ? LOOSE_MIN_PRICE_UP : MIN_PRICE_UP;
  const minBuyers = loose ? LOOSE_MIN_BUYERS : MIN_BUYERS;
  const from = now - WINDOW;
  let buySol = 0;
  let sellSol = 0;
  let buys = 0;
  let startPrice = -1;
  let high = 0;
  let last = 0;
  const byUser = new Map<string, number>();
  for (const tr of t.trades) {
    if (tr.slot <= from) {
      startPrice = tr.price;
      continue;
    }
    if (startPrice < 0) startPrice = tr.price;
    if (tr.buy) {
      buySol += tr.sol;
      buys++;
      byUser.set(tr.user, (byUser.get(tr.user) ?? 0) + tr.sol);
    } else {
      sellSol += tr.sol;
    }
    if (tr.price > high) high = tr.price;
    last = tr.price;
  }
  if (buys < MIN_BUYS || byUser.size < minBuyers) return false;
  if (buySol - sellSol < minNet) return false;
  if (startPrice <= 0 || last <= 0) return false;
  const up = last / startPrice - 1;
  if (up < minUp) return false;
  if (last < high * NEAR_HIGH) return false;
  let top = 0;
  for (const v of byUser.values()) if (v > top) top = v;
  if (buySol > 0 && top > MAX_TOP_BUYER_SHARE * buySol) return false;
  return true;
}

await runBot({
  onBatch(state, batch) {
    const now = batch.slot;
    if (firstSlot === null) firstSlot = now;
    const bigSell = new Set<string>();
    const traded = new Set<string>();

    for (const f of batch.fills) {
      if (f.status === 'filled' && !f.client_id.startsWith('_')) {
        myFills++;
        myCoins.add(f.mint);
      }
    }
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        tapeOf(e.mint).created = e.slot;
      } else if (e.event === 'TradeEvent') {
        const t = tapeOf(e.mint);
        t.trades.push({
          slot: e.slot,
          buy: e.is_buy,
          sol: Number(e.sol_amount) / LAMPORTS,
          user: e.user,
          price: priceOf(BigInt(e.virtual_quote_reserves), BigInt(e.virtual_token_reserves)),
        });
        traded.add(e.mint);
        if (!e.is_buy && Number(e.sol_amount) / LAMPORTS >= BIG_SELL_SOL) bigSell.add(e.mint);
      }
    }
    // Prune tapes to the longest window we ever query, keeping memory bounded.
    const keep = Math.max(WINDOW, FADE_WINDOW) + 5;
    for (const mint of traded) {
      const t = tapes.get(mint)!;
      let cut = 0;
      while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
      if (cut > 0) t.trades.splice(0, cut);
    }

    const orders: OrderInput[] = [];
    const cap = state.hello.caps.max_orders_per_batch;
    const txSol = Number(state.txFeeLamports()) / LAMPORTS;
    const pendingByMint = new Map<string, number>();
    for (const po of state.pending.values()) {
      pendingByMint.set(po.order.mint, (pendingByMint.get(po.order.mint) ?? 0) + 1);
    }
    const hasPendingSell = (mint: string): boolean => {
      for (const po of state.pending.values()) {
        if (po.order.mint === mint && po.order.side === 'sell') return true;
      }
      return false;
    };

    // Exits first: a position that must go outranks any new entry this batch.
    for (const [mint, h] of held) {
      const pos = state.positions.get(mint);
      const curve = state.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) {
          held.delete(mint);
          const t = tapeOf(mint);
          if (h.filled) t.lastExit = now;
          else t.entries--;
        }
        continue;
      }
      h.filled = true;
      if (curve === undefined || curve.complete) continue;
      const value = positionValue(state, mint, pos.token_amount);
      const cost = Number(pos.cost_lamports) / LAMPORTS;
      if (value > h.peakValue) h.peakValue = value;
      const t = tapeOf(mint);
      let exit = h.exiting;
      if (!exit && value < cost * (1 - STOP_LOSS)) exit = true;
      if (!exit && value >= cost * (1 + TAKE_PROFIT)) exit = true;
      if (!exit && h.peakValue >= cost * (1 + TRAIL_ARM) && value < h.peakValue * (1 - TRAIL_DROP)) exit = true;
      if (!exit && netFlow(t, now, FADE_WINDOW) < FADE_NET_SOL) exit = true;
      if (!exit && bigSell.has(mint)) exit = true;
      if (!exit && now - h.opened > MAX_HOLD && value < cost * (1 + HOLD_IF_UP)) exit = true;
      if (!exit) continue;
      h.exiting = true;
      if (hasPendingSell(mint)) continue;
      if (orders.length >= cap) break;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: SELL_SLIPPAGE_BPS });
    }

    // Entries and one pyramid add per position, in the order coins traded this batch.
    const loose = firstSlot !== null && now - firstSlot > LOOSE_AFTER_SLOTS && myCoins.size < LOOSE_MIN_COINS;
    let free = Number(state.freeCashLamports()) / LAMPORTS;
    let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
    for (const mint of traded) {
      if (orders.length >= cap) break;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      if (pendingByMint.has(mint)) continue;
      const t = tapes.get(mint)!;
      const h = held.get(mint);
      const price = priceOf(curve.virtual_quote_reserves, curve.virtual_token_reserves);
      const rqSol = Number(curve.real_quote_reserves) / LAMPORTS;
      let want: number;
      if (h === undefined) {
        if (slotsLeft <= 0) continue;
        if (t.entries >= MAX_ENTRIES_PER_COIN || now - t.lastExit < COOLDOWN) continue;
        if (!entryOk(t, now, rqSol, loose)) continue;
        want = STAKE_SOL;
      } else {
        // Pyramid into strength only: price stepped up from the entry and flow still in.
        if (h.exiting || !h.filled || h.invested >= MAX_INVESTED_SOL - MIN_BUY_SOL) continue;
        const pos = state.positions.get(mint);
        if (pos === undefined) continue;
        if (price < h.entryPrice * (1 + ADD_PRICE_UP)) continue;
        if (netFlow(t, now, WINDOW) < ADD_MIN_NET_SOL) continue;
        if (rqSol > MAX_REAL_SOL) continue;
        want = Math.min(ADD_SOL, MAX_INVESTED_SOL - h.invested);
      }
      const capSol = Number(state.maxBuyLamports(mint)) / LAMPORTS;
      let amount = Math.min(capSol, want);
      if (amount > free - txSol) amount = free - txSol;
      if (amount < MIN_BUY_SOL) continue;
      orders.push({
        side: 'buy',
        mint,
        quote_lamports: BigInt(Math.floor(amount * LAMPORTS)),
        max_slippage_bps: BUY_SLIPPAGE_BPS,
      });
      free -= amount + txSol;
      if (h === undefined) {
        held.set(mint, {
          opened: now,
          entryPrice: price,
          peakValue: 0,
          invested: amount,
          exiting: false,
          filled: false,
        });
        t.entries++;
        slotsLeft--;
      } else {
        h.invested += amount;
      }
    }
    return orders;
  },
});

// ============================================================
// THE TIPSTER EDGE — Engine v9.9 (Sofascore Edition)
// ============================================================
// Data source:  Sofascore via RapidAPI (single source of truth)
// Schedule:
//   Morning fetch  → 06:00 UK — full data pull (fixtures, odds, stats)
//   Midday refresh → 13:00 UK — odds-only refresh
//   Tip generation → every 15 minutes (reads from cache, zero API calls)
//   Results settle → every 60 minutes
//   Emails         → Pro 07:00, Free 08:30, Saturday 08:00 UK
//
// STRICT RULES ENGINE — target ROI: 30%+
//   Sports:  Ice Hockey (primary), Basketball (primary), Football (secondary)
//   Markets: H2H win markets + Overs (secondary, stricter rules)
//   Odds:    insight from INSIGHT_ODDS_MIN 1.05 | staked from BET_ODDS_MIN 1.35
//            | rejected above ODDS_ELITE_MAX 10.0.
//            (This line read "1.40–2.20 core | 2.21–2.50 elite only" for a long
//            time. The code has never enforced a 2.50 ceiling; whether it
//            should is a decision about which bets get published, so the line
//            has been corrected to describe the constants rather than the
//            constants changed to match the line.)
//   Edge:    no minimum is enforced. MIN_EDGE_PCT exists but is referenced
//            nowhere, and ELITE_H2H_EDGE / ELITE_OVERS_EDGE only pick a grade
//            that is never displayed — it is appended to tips.notes, and
//            nothing reads that column. See the note on applyStrictRules. What actually excludes a no-edge bet is Kelly:
//            the fraction is <= 0 whenever modelProb <= 1/odds, so it sizes at
//            0 and the tip publishes as insight. Measured over everything that
//            clears the confidence and odds filters, 1.6% of combinations have
//            non-positive edge against the price — the confidence buckets this
//            replaced staked about a unit on every one of them.
//            (This line read "H2H ≥ 8% | Overs ≥ 12%", which was never enforced.)
//   Grades:  A+ / A / B — edge bands only, for display. Grades do NOT set the
//            stake: that is quarter-Kelly (see KELLY_TIERS), which depends on
//            odds as well as edge, so the two correlate but do not map onto
//            each other. Measured across the publishable band, A+ averages
//            1.34u and A averages 0.75u, with both spanning 0.5u to 3u.
//            (This line previously read "A+ (2–2.5u) | A (1.5u)", which the
//            code has never done.)
//   Stakes:  0.5u–3.0u, quarter-Kelly. No cap on total daily exposure — each
//            tip is sized independently, so a heavy fixture list means a
//            proportionally heavier day.
//   Line:    Reject if odds moved ≥ 0.10 against; allow if improved
//
// v9.9 — DATA INTEGRITY & SECURITY PASS
//   [FIX] Settler no longer voids already-graded tips during history
//         backfill. Backfill is now a separate, API-free path that
//         writes the missing ledger row from the stored result.
//   [FIX] All results_history reads paginate. PostgREST caps at 1000
//         rows; past that the old code silently truncated, which made
//         every older tip look "missing" and mass-voided the ledger.
//   [FIX] Settlement now pays out at advised_odds (the price published
//         at tip time), not best_odds (the peak price ever observed).
//         best_odds is display-only from here on.
//   [FIX] Win/loss grading verifies the selection against BOTH teams.
//         A fuzzy-match miss used to silently grade the opposite side.
//   [FIX] updateStatsCache moved out of the settle loop.
//   [SEC] /stripe/portal and /verify-pro now require a valid JWT.
//   [ADD] /admin/resettle route (admin.html has always called it).
// ============================================================

const { createClient } = require('@supabase/supabase-js');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
const crypto = require('crypto');

// ─── CREDENTIALS ─────────────────────────────────────────────
const SUPABASE_URL         = process.env.SUPABASE_URL || 'https://eyhlzzaaxrwisrtwyoyh.supabase.co';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
if (!SUPABASE_SERVICE_KEY) throw new Error('FATAL: SUPABASE_SERVICE_KEY env var is not set');
const RAPIDAPI_KEY         = process.env.RAPIDAPI_KEY || '';
if (!RAPIDAPI_KEY) throw new Error('FATAL: RAPIDAPI_KEY env var is not set');
const ADMIN_KEY            = process.env.ADMIN_KEY || '';
if (!ADMIN_KEY) throw new Error('FATAL: ADMIN_KEY env var is not set');
// Unsubscribe links are signed so nobody can opt another subscriber out by
// guessing their UUID. Prefer a dedicated secret; fall back to the Stripe
// webhook secret so links already sitting in inboxes keep verifying. There is
// deliberately NO literal default — this repository is public, so a published
// fallback would make every unsubscribe token forgeable by anyone reading it.
const UNSUB_SECRET         = process.env.UNSUB_SECRET || process.env.STRIPE_WEBHOOK_SECRET || '';
if (!UNSUB_SECRET) throw new Error('FATAL: set UNSUB_SECRET (or STRIPE_WEBHOOK_SECRET) — unsubscribe links cannot be signed without it');
const RAPIDAPI_HOST        = 'sofascore.p.rapidapi.com';
const SOFASCORE_BASE       = `https://${RAPIDAPI_HOST}`;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// Constant-time string compare — avoids leaking secrets via response timing.
function safeEqual(a, b) {
  const ba = Buffer.from(String(a || ''), 'utf8');
  const bb = Buffer.from(String(b || ''), 'utf8');
  if (ba.length !== bb.length || ba.length === 0) return false;
  return crypto.timingSafeEqual(ba, bb);
}

// ─── PAGINATED SELECT ─────────────────────────────────────────
// PostgREST caps every response at 1000 rows. Any query whose result
// set can grow past that MUST paginate or it truncates silently — which
// is how the settler started treating a full ledger as "missing".
// An update, with both of the ways it can fail to change anything.
//
// Supabase reports a rejected write two different ways: an error object, or —
// when a row-level policy refuses the row — no error and no rows at all. Code
// that checks only the first treats the second as a success.
//
// Every caller below already carried a comment explaining why a failed write
// must not be treated as done. Each of those comments was true of one failure
// mode and silent about the other. This makes them true of both.
async function updateChecked(table, patch, applyFilter) {
  const { data, error } = await applyFilter(supabase.from(table).update(patch)).select('id');
  if (error) return { ok: false, why: error.message };
  if (!data || !data.length) return { ok: false, why: 'the write was refused — no rows changed' };
  return { ok: true, rows: data.length };
}

async function selectAll(table, columns, applyFilters = null) {
  const PAGE = 1000;
  let out = [], from = 0;
  for (;;) {
    let q = supabase.from(table).select(columns).order('id', { ascending: true }).range(from, from + PAGE - 1);
    if (applyFilters) q = applyFilters(q);
    const { data, error } = await q;
    if (error) throw new Error(`selectAll(${table}) failed at offset ${from}: ${error.message}`);
    if (!data || !data.length) break;
    out = out.concat(data);
    if (data.length < PAGE) break;
    from += PAGE;
  }
  return out;
}

// ─── ADVISED PRICE ────────────────────────────────────────────
// The single definition of "the price" for a tip: what we advised at
// publication. Display and settlement MUST read the same number — if the
// emails quote one price and the ledger settles at another, the published
// ROI describes bets nobody was actually told to place.
// Legacy rows (pre-v9.9) have no advised_odds and fall back to `odds`.
// Returns NaN for unusable input, matching bare parseFloat: callers already
// guard with Number.isFinite (settlement) or a falsy check (backfill).
// Is this a bet, or something published for information?
//
// applyStrictRules marks short-price selections `tier: 'insight'` with
// `stake: 0` — two facts about the same decision. The engine tested only the
// stake; updateStatsCache and the website test both. On the live table those
// two answers differ for real rows: two carry tier 'insight' with a stake of 1
// and 2, written by a build that set the tier without zeroing the stake.
//
// Which means the dispatchers could email one of those as a bet and tag it as
// a free tip, while the published record — correctly — excluded it. One
// definition now, and it is the stricter one, because a row that says
// "informational" in either field is not something to advise.
function isBet(tip) {
  return !!tip && tip.tier !== 'insight' && parseFloat(tip.stake ?? 1) > 0;
}

function advisedPrice(tip) {
  return parseFloat(tip?.advised_odds ?? tip?.odds);
}

// ─── SOFASCORE TOURNAMENT IDs ─────────────────────────────────
// These are Sofascore's internal tournament IDs
const SOFASCORE_TOURNAMENTS = {
  premier_league:   17,
  la_liga:          8,
  bundesliga:       35,
  serie_a:          23,
  ligue_1:          34,
  champions_league: 7,
  nba:              132,
  nhl:              234,
};

// ─── SPORTS CONFIG ────────────────────────────────────────────
const SPORTS = [
  { key: 'soccer_epl',               name: 'Football',   league: 'Premier League',   tournamentId: 17  },
  { key: 'soccer_spain_la_liga',      name: 'Football',   league: 'La Liga',          tournamentId: 8   },
  { key: 'soccer_germany_bundesliga', name: 'Football',   league: 'Bundesliga',       tournamentId: 35  },
  { key: 'soccer_italy_serie_a',      name: 'Football',   league: 'Serie A',          tournamentId: 23  },
  { key: 'soccer_france_ligue_one',   name: 'Football',   league: 'Ligue 1',          tournamentId: 34  },
  { key: 'soccer_uefa_champs_league', name: 'Football',   league: 'Champions League', tournamentId: 7   },
  { key: 'basketball_nba',            name: 'Basketball', league: 'NBA',              tournamentId: 132 },
  { key: 'icehockey_nhl',             name: 'Ice Hockey', league: 'NHL',              tournamentId: 234 },
];

// ─── STRICT RULES ENGINE CONSTANTS ───────────────────────────
const MIN_CONFIDENCE     = 52;  // Minimum win probability to publish
const NBA_MIN_CONFIDENCE = 52;
const NHL_MIN_CONFIDENCE = 52;
const MIN_EDGE_PCT      = 0;
// Vestigial. None of these is referenced anywhere, and setting them has no
// effect — same as MIN_EDGE_PCT above. What actually excludes a no-edge bet is
// Kelly: the fraction is <= 0 whenever modelProb <= 1/odds, so the tip sizes at
// 0 and publishes as insight rather than as a bet.
//
// Left in place rather than deleted because wiring them up is a real decision:
// an enforced `edge >= 0` filter would stop those tips being published at all,
// where today they still appear as informational. That changes the card.
const NHL_MIN_EDGE      = 0;   // unused
const NBA_MIN_EDGE      = 0;   // unused
const OVERS_MIN_EDGE    = 0;   // unused
const ELITE_H2H_EDGE    = 10;
const ELITE_OVERS_EDGE  = 14;

// Football over-2.5 was an unfinished integration: the market was fetched on
// every event and the model computed the probability, but no candidate was
// ever built from either, so the data was collected and thrown away.
//
// It is finished now, and off. Turning it on publishes a market subscribers
// are not currently advised to bet, which is a product decision rather than a
// fix — so it is one boolean rather than an open question. Everything else it
// needs already works: the totals line is de-vigged against its own Under, the
// settler grades any "Over X" selection, and 2.5 is a half line so it cannot
// push.
const PUBLISH_FOOTBALL_OVERS = false;

// Odds tiers:
//   INSIGHT_ODDS_MIN  — show as "Short Price Watch" (informational, stake 0)
//   BET_ODDS_MIN      — minimum odds for a real bet recommendation
//   ODDS_ELITE_MAX    — maximum odds published
const INSIGHT_ODDS_MIN  = 1.05; // Show from here — heavy favs shown as insight
const BET_ODDS_MIN      = 1.35; // Minimum odds for a real bet
const ODDS_CORE_MAX     = 5.00;  // unused — there is no core/elite split in the code
const ODDS_ELITE_MAX    = 10.0;

// How far a price may move against us before a refresh is refused, measured in
// implied-probability POINTS.
//
// This was 0.10 in decimal odds, and a fixed decimal step means something
// different at every price:
//
//     1.35 -> 1.25   5.93 points of implied probability
//     2.00 -> 1.90   2.63 points
//     5.00 -> 4.90   0.41 points
//    10.00 -> 9.90   0.10 points
//
// So it fired on a 1% relative drift at long odds — 10.00 to 9.90 is noise —
// while tolerating a genuine steam move at short ones. Long-priced tips
// therefore stopped taking refreshes almost immediately, freezing their live
// and best prices at publication, which is what the site shows next to the
// advised price.
//
// Everything else in this engine reasons in implied probability, and that is
// the natural unit for "the market moved against us". 2.5 points is what the
// old constant meant at 2.00, the middle of the published range.
const LINE_MOVE_REJECT_PP = 2.5;
// The score matrix is truncated at this many goals per side and then
// renormalised, so whatever Poisson mass sits above it is redistributed across
// the rest — which biases every probability drawn from it.
//
// At ordinary football lambdas the loss is immaterial (0.002 points at 1.5 vs
// 1.2). It is the clamp maximum that matters: at 4.0 against 4.0, a cap of 8
// discarded 2.1% per side and moved the draw by 0.61 points. 12 brings that to
// 0.03%, for 169 cells instead of 81.
const MATRIX_MAX_GOALS  = 12;
const FOOTBALL_EDGE_PREMIUM = 0;  // unused
const MIN_QUALITY_SCORE = 0.10;
// What counts as a short rest, per sport. These have to differ because the
// schedules do: an NHL side plays 82 games in about 185 days, one every 2.26,
// so two days off is a normal week rather than congestion. A football side
// plays 38 league games across roughly 270 days.
//
// The bands were a single pair, `<= 2` and `<= 3`, applied to both. In football
// that is genuine fixture congestion. In the NHL it described the ordinary
// state of every team, so the fatigue penalty fired on essentially every game —
// and with both sides carrying it, both lambdas came out about 2.3% low for no
// reason. The case that actually matters there is the back-to-back.
// How many players are on the field at once, used to turn a count of missing
// players into a fraction of the side. A flat "3% per absence" is a different
// statement in a sport that fields eleven and one that dresses eighteen
// skaters, and the same absence list cost both the same.
//
// The impact constants are chosen so football is unchanged: 0.33 over eleven
// is exactly the 0.03 per player it applied before.
const SQUAD_SIZE_FOOTBALL = 11;
const SQUAD_SIZE_NHL      = 18;   // skaters dressed; the goalie is modelled separately
const MISSING_ATTACK_IMPACT  = 0.33;   // if a whole side were missing
const MISSING_DEFENCE_IMPACT = 0.22;

const REST_BANDS_FOOTBALL = { heavy: 2, mild: 3 };
const REST_BANDS_NHL      = { heavy: 0, mild: 1 };   // heavy = a back-to-back

const NHL_HOME_ADVANTAGE  = 0.20;
const NHL_LEAGUE_AVG_GF   = 3.10;
// Same reasoning, and it mattered far more here: NHL lambdas run about twice
// football ones, and the published Over 5.5 line sits where the truncated tail
// actually is. Measured against an untruncated matrix, a cap of 7 understated
// P(Over 5.5) by 1.08 points at lambda 3.0 per side, 2.12 at 4.0 — a
// systematic bias against every over the engine could publish. The clamp
// permits 6.0, where a cap of 7 threw away a quarter of the distribution.
//
// 16 leaves under 0.001 points of error at the clamp maximum.
const NHL_MATRIX_MAX      = 16;
const NBA_HOME_ADVANTAGE  = 3.5;
const NBA_LEAGUE_AVG_PTS  = 113;
const NBA_SCORE_STD_DEV   = 12.0;
// The Dixon-Coles low-score correction. It was +0.10, and the sign was
// backwards.
//
// dixonColesTau below is the standard formulation, term for term:
//
//     tau(0,0) = 1 - lH*lA*rho     tau(0,1) = 1 + lH*rho
//     tau(1,0) = 1 + lA*rho        tau(1,1) = 1 - rho
//
// The term exists because independent Poisson under-predicts 0-0 and 1-1, so
// rho is negative and the correction RAISES those two scorelines. Dixon and
// Coles fitted about -0.13 on English league data, and that is the value in
// general use.
//
// A positive rho runs every one of those four terms the other way: it lowers
// 0-0 and 1-1 and raises 1-0 and 0-1. Measured on an average fixture
// (lH 1.55, lA 1.18), that is
//
//     draw  22.76% at +0.10, 25.15% at 0, 28.25% at -0.13
//     home  47.06%           45.86%       44.31%
//
// against an observed Premier League draw rate of roughly 24-26%. So the old
// value pushed draws below what actually happens while inflating home and away
// — and the engine publishes a Draw selection, so it suppressed those tips
// and manufactured edge on the other two at the same time. A backwards
// correction is worse than none.
//
// The sign is not in doubt; the magnitude is a fitting question this repository
// has no data to answer, so it takes the published value rather than a guess.
const DC_RHO = -0.13;

// ─── LEAGUE AVERAGES (Football) ───────────────────────────────
const LEAGUE_AVERAGES = {
  17:  { homeGoals: 1.53, awayGoals: 1.21 }, // Premier League
  8:   { homeGoals: 1.57, awayGoals: 1.14 }, // La Liga
  35:  { homeGoals: 1.68, awayGoals: 1.25 }, // Bundesliga
  23:  { homeGoals: 1.49, awayGoals: 1.12 }, // Serie A
  34:  { homeGoals: 1.51, awayGoals: 1.18 }, // Ligue 1
  7:   { homeGoals: 1.64, awayGoals: 1.22 }, // Champions League
};
const LEAGUE_AVG_FALLBACK = { homeGoals: 1.55, awayGoals: 1.18 };
function getLeagueAvg(tournamentId) {
  return LEAGUE_AVERAGES[tournamentId] || LEAGUE_AVG_FALLBACK;
}

// ═══════════════════════════════════════════════════════════════
// SOFASCORE CACHE — populated once at 06:00, refreshed at 13:00
// The 15-min cycle reads ONLY from this cache — zero API calls mid-cycle
// ═══════════════════════════════════════════════════════════════

const sofascoreCache = {
  // { sportKey: [ { home_team, away_team, commence_time, tournamentId, eventId, bookmakers[], teamStats } ] }
  events:      {},
  fetchedDate: '',
  oddsFetchedAt: null,
  // Per sport, because the fetch is per sport. The single oddsFetchedAt above
  // is stamped at the end of morningFetch whatever happened inside it, while
  // sofascoreCache.events[sport.key] is only written when that sport's fetch
  // succeeded. So one league failing left yesterday's fixtures in the cache
  // with a timestamp saying they had just been fetched — and the staleness
  // check in runEngine, looking at that one timestamp, could not see it.
  //
  // oddsFetchedAt is kept because /status reports it.
  fetchedAt: {},
};

// Sofascore API wrapper — rate limited to 5 req/sec
let rapidApiCallCount = 0;
let rapidApiCallDate  = '';
let rapidApiBudgetWarned = false;

// The API is metered and nothing was metering it. rapidApiCallCount was
// incremented, logged every tenth call, and never once compared against
// anything — so a retry storm, a restart loop, or a fixture list larger than
// expected would spend the month's quota with nothing in the way. Several
// comments elsewhere cite "each restart spends RapidAPI quota" as a reason to
// keep the process alive, so the cost was understood; it just had no ceiling.
//
// The budget is a hard stop for the rest of the UK day. Set
// RAPIDAPI_DAILY_BUDGET to match the plan; the default is deliberately well
// above a normal day (a full morning fetch plus a midday reprice) so it only
// ever fires on something going wrong.
const RAPIDAPI_DAILY_BUDGET = parseInt(process.env.RAPIDAPI_DAILY_BUDGET || '1500', 10);

// 22 hours: comfortably past the ~18h overnight gap that normal operation
// produces, comfortably short of a full day. Anything above this means a fetch
// has failed rather than that the schedule is between refreshes.
const ODDS_MAX_AGE_MS = 22 * 3600 * 1000;

// A 429 is retryable, which is right for a per-minute limit and exactly wrong
// for an exhausted quota: the call is repeated with a 500ms backoff, then a
// second, and every one of those counts. Iterating a few hundred events that
// way turns hitting the limit into three times the spend, all of it failing.
//
// A cooldown covers both. A per-minute limit clears well inside it; a quota
// limit stops costing anything.
const RAPIDAPI_COOLDOWN_MS = 15 * 60 * 1000;
let rapidApiCooldownUntil = 0;

// Returns false when a call must not be made. Rolls the daily counter itself,
// so there is one place that decides what day it is.
function apiBudgetAllows(path) {
  const today = ukDateString();
  if (rapidApiCallDate !== today) {
    rapidApiCallDate = today;
    rapidApiCallCount = 0;
    rapidApiBudgetWarned = false;
  }
  if (Date.now() < rapidApiCooldownUntil) return false;
  if (rapidApiCallCount >= RAPIDAPI_DAILY_BUDGET) {
    if (!rapidApiBudgetWarned) {
      rapidApiBudgetWarned = true;
      console.error(`🚨 RapidAPI daily budget of ${RAPIDAPI_DAILY_BUDGET} reached — `
        + `refusing further calls until tomorrow (first refused: ${path})`);
    }
    return false;
  }
  return true;
}

// Whether a call could be made right now.
//
// apiBudgetAllows() is the gate the fetcher uses, and it rolls the counter and
// warns as a side effect. This asks the same question without those, for code
// that needs to know WHY it has no data rather than whether to go and ask for
// it. It does not roll the day: a stale count from yesterday makes it answer
// false, and every caller treats false as "do not draw a conclusion", which is
// the safe direction.
function apiCallsAvailable() {
  return Date.now() >= rapidApiCooldownUntil
      && rapidApiCallCount < RAPIDAPI_DAILY_BUDGET;
}

function trackApiCall() {
  const today = ukDateString();
  if (rapidApiCallDate !== today) { rapidApiCallDate = today; rapidApiCallCount = 0; rapidApiBudgetWarned = false; }
  rapidApiCallCount++;
  if (rapidApiCallCount % 10 === 0) console.log(`📡 RapidAPI calls today: ${rapidApiCallCount}`);
}

// When a tip stops being worth asking about.
//
// Ask-before-voiding means a tip that never resolves is retried on every cycle,
// which is every fifteen minutes, forever — and each retry costs metered calls.
// Sofascore's last-matches endpoint pages back three pages, so a fixture a month
// old is not reachable through it in any case: past this, asking again cannot
// produce a different answer, and the tip is voided without a call.
//
// The gap between 72 hours and 30 days is the range where the answer really can
// change: a service that was down, a budget that was spent, a source that was
// slow to publish.
const SETTLE_ASK_UNTIL_HOURS = 24 * 30;

// How many times a single Sofascore request is attempted before giving up.
const SOFASCORE_MAX_ATTEMPTS = 3;

async function sofascoreFetch(path, params = {}, attempt = 1) {
  // Checked on every attempt, not just the first: a retry is another call and
  // costs the same as the one before it.
  if (!apiBudgetAllows(path)) return null;
  const url = new URL(`${SOFASCORE_BASE}${path}`);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  trackApiCall();

  // Retry transient failures. morningFetch runs once a day and a failed call
  // makes it `continue` past that league, so before this a single blip at 06:00
  // cost a whole competition's fixtures until the next morning — and with the
  // scheduler firing exactly once a day, there was no second chance.
  const retry = async (why) => {
    if (attempt >= SOFASCORE_MAX_ATTEMPTS) {
      console.log(`⚠️ Sofascore ${path}: ${why} — gave up after ${attempt} attempts`);
      return null;
    }
    const wait = 500 * Math.pow(2, attempt - 1);   // 500ms, then 1000ms
    console.log(`⚠️ Sofascore ${path}: ${why} — retrying in ${wait}ms (${attempt + 1}/${SOFASCORE_MAX_ATTEMPTS})`);
    await new Promise(r => setTimeout(r, wait));
    return sofascoreFetch(path, params, attempt + 1);
  };

  let timeout;
  try {
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), 10000); // 10s timeout
    const res = await fetch(url.toString(), {
      signal: controller.signal,
      headers: {
        'x-rapidapi-key':  RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST,
        'Content-Type':    'application/json',
      },
    });

    if (!res.ok) {
      // 429 and 5xx are worth another go. 401/403/404 are not: the key is wrong
      // or the path does not exist, and retrying only burns quota.
      if (res.status === 429) {
        // Honour Retry-After when the API sends one; otherwise fall back to the
        // fixed cooldown. Either way, stop the rest of this batch rather than
        // asking a few hundred more times.
        const after = parseInt(res.headers.get('retry-after') || '0', 10);
        const waitMs = Number.isFinite(after) && after > 0
          ? Math.min(after * 1000, RAPIDAPI_COOLDOWN_MS)
          : RAPIDAPI_COOLDOWN_MS;
        if (attempt >= SOFASCORE_MAX_ATTEMPTS) {
          rapidApiCooldownUntil = Date.now() + waitMs;
          console.error(`🚨 RapidAPI rate limited (429) after ${attempt} attempts — `
            + `pausing all calls for ${Math.round(waitMs / 60000)} min`);
          return null;
        }
        return retry('HTTP 429');
      }
      if (res.status >= 500) return retry(`HTTP ${res.status}`);
      console.log(`⚠️ Sofascore ${path}: ${res.status}`);
      return null;
    }

    const text = await res.text();
    if (!text || text.trim() === '') return retry('empty response');

    if (!text.trim().startsWith('{') && !text.trim().startsWith('[')) {
      // A well-formed non-JSON body is a contract problem, not a blip.
      console.error(`Sofascore ${path}: unexpected body — ${text.slice(0, 150)}`);
      return null;
    }
    try {
      return JSON.parse(text);
    } catch(parseErr) {
      return retry('JSON parse error');
    }
  } catch(e) {
    if (e.name === 'AbortError') return retry('timeout');
    return retry(`fetch error: ${e.message}`);
  } finally {
    // Previously only cleared on the success path, so a thrown request left a
    // live 10s timer behind on every failure.
    clearTimeout(timeout);
  }
}

// ─── SOFASCORE DATA HELPERS ───────────────────────────────────

// Fetch scheduled events for a tournament (next 2 days)
async function fetchTournamentEvents(tournamentId) {
  // Get current season for this tournament
  const seasonsData = await sofascoreFetch(`/tournaments/get-seasons`, { tournamentId });
  if (!seasonsData?.seasons?.length) return [];
  const season = seasonsData.seasons[0];

  await new Promise(r => setTimeout(r, 250)); // rate limit gap

  // Get next scheduled matches
  const data = await sofascoreFetch(`/tournaments/get-next-matches`, {
    tournamentId,
    seasonId: season.id,
    page: 0,
  });
  return data?.events || [];
}

// ─── ROBUST ODDS PARSER ───────────────────────────────────────
// Handles: fractional strings "11/10", "3/1", decimal numbers, strings like "1.95"
// Returns decimal odds (e.g. 2.10) or 0 if unparseable
function parseOdds(value) {
  if (value == null) return 0;
  const s = String(value).trim();
  // Fractional format: "11/10", "3/1", "1/2" etc
  if (s.includes('/')) {
    const parts = s.split('/');
    if (parts.length !== 2) return 0;
    const num = parseFloat(parts[0]);
    const den = parseFloat(parts[1]);
    if (!isFinite(num) || !isFinite(den) || den === 0) return 0;
    const frac = parseFloat((1 + num / den).toFixed(4));
    return frac > 1 ? frac : 0;
  }
  // Decimal format.
  //
  // Anything at or below 1.00 is not a price: it says a winning bet returns
  // less than the stake. This accepted d > 0, so a feed sending 0.5 produced an
  // implied probability of 2.0, which de-vigged against the rest of the book
  // into a perfectly plausible-looking 79% — a number with nothing behind it.
  // Returning 0 makes getChoicePrice fall through to the next field, which is
  // what it does for every other unusable value.
  const d = parseFloat(s);
  return isFinite(d) && d > 1 ? d : 0;
}

// Get best available price from a Sofascore choice object
function getChoicePrice(choice) {
  return parseOdds(choice.fractionalValue) ||
         parseOdds(choice.initialFractionalValue) ||
         parseOdds(choice.odds);
}

// Fetch odds for a specific event
async function fetchEventOdds(eventId) {
  await new Promise(r => setTimeout(r, 500)); // 500ms gap — stay under 5 req/sec
  const data = await sofascoreFetch(`/matches/get-all-odds`, { matchId: eventId });
  return data?.markets || null;
}

// Fetch team stats for a tournament season
async function fetchTournamentStandings(tournamentId, seasonId) {
  await new Promise(r => setTimeout(r, 250));
  const data = await sofascoreFetch(`/tournaments/get-standings`, { tournamentId, seasonId });
  return data?.standings || null;
}

// Fetch recent team matches for form
// ─── PARSE SOFASCORE ODDS INTO ENGINE FORMAT ──────────────────
// Football: "Full time" 1X2 market, choices "1"/"X"/"2"
// NBA/NHL:  "Full time"/"Home/Away" 2-way market, choices "1"/"2"
// targetTotalsLine: exact line to use — '2.5' for football, '5.5' for NHL, null for NBA
function parseSofascoreOdds(markets, homeTeam, awayTeam, targetTotalsLine = null) {
  if (!markets || !Array.isArray(markets)) return [];

  const h2hOutcomes    = [];
  const totalsOutcomes = [];

  // ── Football 1X2 (3-way) ──────────────────────────────────
  const ftMarket = markets.find(m =>
    m.marketName === 'Full time' && m.marketGroup === '1X2' && m.marketPeriod === 'Full-time'
  );

  if (ftMarket?.choices) {
    for (const choice of ftMarket.choices) {
      const price = getChoicePrice(choice);
      if (!price) continue;
      if (choice.name === '1')      h2hOutcomes.push({ name: homeTeam, price });
      else if (choice.name === 'X') h2hOutcomes.push({ name: 'Draw',   price });
      else if (choice.name === '2') h2hOutcomes.push({ name: awayTeam, price });
    }
  }

  // ── NBA/NHL 2-way moneyline ───────────────────────────────
  if (h2hOutcomes.length < 2) {
    const moneylineMarket = markets.find(m =>
      m.marketGroup === 'Home/Away' ||
      m.marketName === 'Money line' ||
      m.marketName === 'Home/Away' ||
      m.marketName === 'Winner' ||
      (m.marketId === 1 && m.choices?.length === 2)
    );

    if (moneylineMarket?.choices) {
      // Resolve the side by name first, and fall back to the choice's position
      // in the SOURCE array — not to h2hOutcomes.length, which counts only the
      // choices parsed so far. If the first choice had no usable price, the
      // second one was landing at length 0 and being labelled home, so the away
      // price was attached to the home team and every edge computed from it was
      // for the wrong side of the game.
      moneylineMarket.choices.forEach((choice, idx) => {
        const price = getChoicePrice(choice);
        if (!price) return;
        const label = String(choice.name || '').trim();
        // A label that matches both teams identifies neither. This tried home
        // first and took it, so a feed abbreviating 'Manchester United' and
        // 'Manchester City' to 'Manchester' would have put the away price on
        // the home side — the same silent inversion the position fallback
        // below was added to fix. Fall through to position instead, which at
        // least follows the feed's own ordering.
        const isHome = nameMatch(label, homeTeam);
        const isAway = nameMatch(label, awayTeam);
        const named  = isHome !== isAway;
        if      (label === '1' || (named && isHome)) h2hOutcomes.push({ name: homeTeam, price });
        else if (label === '2' || (named && isAway)) h2hOutcomes.push({ name: awayTeam, price });
        else if (idx === 0)                          h2hOutcomes.push({ name: homeTeam, price });
        else if (idx === 1)                          h2hOutcomes.push({ name: awayTeam, price });
      });
    }
  }

  // ── Totals — only fetch the exact target line, no fallback ──
  if (targetTotalsLine) {
    const totalsMarket = markets.find(m =>
      (m.marketName === 'Match goals' || m.marketName === 'Total' || m.marketName === 'Over/Under') &&
      m.choiceGroup === targetTotalsLine
    );
    if (totalsMarket?.choices) {
      for (const choice of totalsMarket.choices) {
        const price = getChoicePrice(choice);
        if (!price) continue;
        if (choice.name === 'Over')  totalsOutcomes.push({ name: 'Over',  price, point: parseFloat(targetTotalsLine) });
        if (choice.name === 'Under') totalsOutcomes.push({ name: 'Under', price, point: parseFloat(targetTotalsLine) });
      }
    }
    // NBA: targetTotalsLine is null — no totals fetched at all
  }

  if (h2hOutcomes.length < 2) return [];

  return [{
    title: 'Sofascore',
    markets: [
      { key: 'h2h', outcomes: h2hOutcomes },
      ...(totalsOutcomes.length >= 2 ? [{ key: 'totals', outcomes: totalsOutcomes }] : []),
    ],
  }];
}

// ─── BUILD TEAM STATS FROM STANDINGS ─────────────────────────
function buildTeamStatsFromStandings(standings, tournamentId) {
  const teams = {};
  if (!standings) return teams;

  const rows = standings[0]?.rows || standings;
  // The guard above catches null and undefined; it does not catch a shape.
  // A third-party feed that starts answering with an object, or an error
  // string, gets this far and throws on the for..of below — the loop's own
  // per-row guards never run, because there is nothing to iterate. Caught by
  // the caller either way, but as a stack trace rather than a sentence, and
  // this file already argues at length that one bad ROW should cost one team;
  // one bad shape should cost one league, and say so.
  if (!Array.isArray(rows)) {
    console.warn(`⚠️ standings for tournament ${tournamentId} arrived as `
      + `${rows === null ? 'null' : typeof rows} rather than a list — skipping this league`);
    return teams;
  }
  const leagueAvg = getLeagueAvg(tournamentId);
  const totalAvg  = leagueAvg.homeGoals + leagueAvg.awayGoals;
  const homeRatio = leagueAvg.homeGoals / totalAvg;
  const awayRatio = leagueAvg.awayGoals / totalAvg;

  for (const row of rows) {
    const name    = row.team?.name || row.team?.shortName;
    const teamId  = row.team?.id;
    if (!name) continue;

    // Coerced and checked. These arrive from a third-party feed, and a field
    // that came through as text made every figure below NaN — which then
    // survived the lambda clamps, the score matrix and every threshold meant to
    // catch nonsense, because no comparison against NaN is ever true. Dropping
    // the row here means one bad row costs one team rather than quietly
    // poisoning every fixture that team appears in.
    const played = Number(row.matches ?? row.played ?? 0);
    const gf     = Number(row.scoresFor ?? row.goalsScored ?? 0);
    const ga     = Number(row.scoresAgainst ?? row.goalsConceded ?? 0);
    if (!Number.isFinite(played) || !Number.isFinite(gf) || !Number.isFinite(ga)) {
      console.warn(`⚠️ standings row for ${name}: non-numeric ` +
        `(played=${row.matches ?? row.played}, for=${row.scoresFor ?? row.goalsScored}, ` +
        `against=${row.scoresAgainst ?? row.goalsConceded}) — skipping this team`);
      continue;
    }

    // Numeric is not the same as possible. The check above rejects text, and
    // the one below rejects too small a sample, but nothing looked at whether
    // the figures could describe a football season: a negative goal count gave
    // the team a NEGATIVE scoring rate, and an absurd one sailed through —
    // measured, 900 goals in 10 matches becomes 83.8 goals per game, and that
    // is what the Poisson model would be handed as this team's attack.
    //
    // Same reasoning as the row guards around it: drop the team rather than
    // let one impossible row shape every fixture it appears in. Ten a game is
    // far above anything a real league produces, so this only ever catches a
    // feed that has gone wrong.
    const MAX_GOALS_PER_GAME = 10;
    if (gf < 0 || ga < 0 || gf > played * MAX_GOALS_PER_GAME || ga > played * MAX_GOALS_PER_GAME) {
      console.warn(`⚠️ standings row for ${name}: implausible goals `
        + `(for=${gf}, against=${ga} in ${played} matches) — skipping this team`);
      continue;
    }

    // Too small a sample to average over. A promoted side mid-season, or the
    // opening week of a new one, would otherwise carry a two-match average.
    if (played < 4) continue;

    const hg = Math.max(1, Math.round(played * homeRatio));
    const ag = Math.max(1, played - hg);

    teams[name] = {
      teamId,
      homeScored:   Math.round(gf * homeRatio),
      homeConceded: Math.round(ga * homeRatio),
      homeGames:    hg,
      awayScored:   Math.round(gf * awayRatio),
      awayConceded: Math.round(ga * awayRatio),
      awayGames:    ag,
      playedGames:  played,
      source: 'sofascore-standings',
    };
  }
  return teams;
}

// ─── BUILD FORM FROM RECENT MATCHES ───────────────────────────
function buildFormFromMatches(matches, teamId) {
  if (!matches?.length) return null;
  // Sort explicitly rather than trusting the feed's order. "Last five" has to
  // mean the five most recent; taking slice(0,5) off however the endpoint
  // happens to return them would quietly compute form from the OLDEST matches
  // if the page is ascending, and stale form degrades every tip that uses it.
  const relevant = matches
    .filter(m => m.status?.type === 'finished' || m.status?.description === 'Ended')
    .slice()
    .sort((a, b) => (b.startTimestamp || 0) - (a.startTimestamp || 0))
    .slice(0, 5);

  if (relevant.length < 3) return null;

  let wins = 0, draws = 0, losses = 0, gf = 0, ga = 0;
  const chars = [];

  for (const m of relevant) {
    const isHome = m.homeTeam?.id === teamId;
    const hg = m.homeScore?.current ?? m.homeScore?.normaltime ?? 0;
    const ag = m.awayScore?.current ?? m.awayScore?.normaltime ?? 0;
    const tg = isHome ? hg : ag;
    const og = isHome ? ag : hg;
    gf += tg; ga += og;
    if (tg > og)       { wins++;   chars.push('W'); }
    else if (tg === og){ draws++;  chars.push('D'); }
    else               { losses++; chars.push('L'); }
  }

  const played = relevant.length;
  return {
    formScore:       (wins * 3 + draws) / (played * 3),
    avgGoalsFor:     gf / played,
    avgGoalsAgainst: ga / played,
    formString:      chars.join(''),
    wins, draws, losses, played,
  };
}

// ═══════════════════════════════════════════════════════════════
// MORNING FETCH — 06:00 UK
// Pulls all fixtures + odds + team stats for next 48 hours
// Populates sofascoreCache.events
// ═══════════════════════════════════════════════════════════════

async function morningFetch() {
  console.log('\n🌅 Morning fetch starting...');
  const today = ukDateString();

  for (const sport of SPORTS) {
    console.log(`  📡 Fetching ${sport.league}...`);
    try {
      const events = await fetchTournamentEvents(sport.tournamentId);
      if (!events.length) {
        console.log(`  ⚠️ No events for ${sport.league}`);
        continue;
      }

      // Filter to next 48 hours
      const now = Date.now();
      const cutoff = now + 48 * 3600 * 1000;
      const upcoming = events.filter(e => {
        const t = (e.startTimestamp || 0) * 1000;
        return t > now && t < cutoff;
      });

      console.log(`  → ${upcoming.length} fixtures in next 48h`);

      // Fetch odds in batches of 3
      const enriched = [];
      const BATCH = 3;
      for (let i = 0; i < upcoming.length; i += BATCH) {
        const batch = upcoming.slice(i, i + BATCH);
        const results = await Promise.all(batch.map(async event => {
          const homeTeam = event.homeTeam?.name || event.homeTeam?.shortName || '';
          const awayTeam = event.awayTeam?.name || event.awayTeam?.shortName || '';
          if (!homeTeam || !awayTeam) return null;
          const oddsRaw    = await fetchEventOdds(event.id);
          const targetLine = sport.name === 'Football' ? '2.5' : sport.name === 'Ice Hockey' ? '5.5' : null;
          const bookmakers = parseSofascoreOdds(oddsRaw, homeTeam, awayTeam, targetLine);
          return {
            id:            event.id,
            home_team:     homeTeam,
            away_team:     awayTeam,
            home_team_id:  event.homeTeam?.id,
            away_team_id:  event.awayTeam?.id,
            commence_time: new Date((event.startTimestamp || 0) * 1000).toISOString(),
            tournamentId:  sport.tournamentId,
            bookmakers,
          };
        }));
        for (const r of results) { if (r) enriched.push(r); }
        if (i + BATCH < upcoming.length) await new Promise(r => setTimeout(r, 700));
      }
      console.log(`  ✅ ${sport.league}: ${enriched.filter(e => e.bookmakers.length).length}/${enriched.length} with odds`);

      // Fetch match context sequentially AFTER odds — avoids rate limit collisions
      // Only for football and NHL — NBA uses its own stats source
      if (sport.name === 'Football' || sport.name === 'Ice Hockey') {
        let ctxLoaded = 0;
        for (const event of enriched) {
          const hId = event.home_team_id;
          const aId = event.away_team_id;
          if (!hId || !aId) {
            console.log(`  ⚠️ No team IDs for ${event.home_team} vs ${event.away_team}`);
            continue;
          }
          await fetchMatchContext(event.id, hId, aId, sport.name);
          ctxLoaded++;
          await new Promise(r => setTimeout(r, 400)); // 400ms between fixtures
        }
        console.log(`  📊 ${sport.league}: context loaded for ${ctxLoaded}/${enriched.length} fixtures`);
      }

      sofascoreCache.events[sport.key] = enriched;
      sofascoreCache.fetchedAt[sport.key] = new Date();

    } catch(e) {
      console.error(`Morning fetch error (${sport.league}):`, e.message);
    }

    await new Promise(r => setTimeout(r, 200));
  }

  // Fetch team stats for football leagues
  await fetchFootballStats();

  sofascoreCache.fetchedDate   = today;
  sofascoreCache.oddsFetchedAt = new Date();

  console.log(`✅ Morning fetch complete. ${rapidApiCallCount} API calls used today.`);
}

// ─── MATCH CONTEXT CACHE ─────────────────────────────────────
// These are keyed by event id and the process is long-lived, so without a cap
// they only grow: one entry per fixture, for every fixture ever seen. Team
// stats are keyed by team and so are bounded by SIZE without any help here.
//
// Bounded is not the same as current, though, which is a separate problem with
// its own fix at the morning refill: a team that leaves a tournament keeps its
// entry, because nothing writes over a key that is no longer produced, and the
// fuzzy fallback in findStats walks insertion order — so a departed club is
// reached before a current one. That is scrubbed per tournament where the
// stats are written, not capped here.
//
// Oldest-first eviction on insertion order, which for these is arrival order —
// good enough, since the entries that matter are always the recent ones.
const CACHE_MAX = 5000;
function cacheSet(cache, key, value) {
  if (!(key in cache) && Object.keys(cache).length >= CACHE_MAX) {
    const oldest = Object.keys(cache).slice(0, Math.ceil(CACHE_MAX / 10));
    for (const k of oldest) delete cache[k];
  }
  cache[key] = value;
  return value;
}

const teamStatsCache = {};
const matchContextCache = {};

// ─── FETCH MATCH CONTEXT (form, injuries, H2H) ───────────────
// Called once per fixture during morning fetch.
// Returns context object merged into the event cache.
async function fetchMatchContext(eventId, homeTeamId, awayTeamId, sport) {
  if (!homeTeamId || !awayTeamId) {
    console.log(`  ⚠️ fetchMatchContext [${eventId}]: missing team IDs (home:${homeTeamId} away:${awayTeamId})`);
    return {};
  }

  const ctx = {
    homeForm:       null,
    awayForm:       null,
    injuries:       { home: [], away: [] },
    h2h:            null,
    lineups:        null,
  };

  try {
    // ── H2H ─────────────────────────────────────────────────
    await new Promise(r => setTimeout(r, 150));
    const h2hData = await sofascoreFetch('/matches/get-h2h', { matchId: eventId });
    if (h2hData?.events?.length) {
      // Newest first before slicing. Head-to-head history can stretch back
      // years, so taking whatever order the feed returns risks weighting a
      // rivalry on meetings from a decade ago rather than recent ones.
      const recent = h2hData.events
        .slice()
        .sort((a, b) => (b.startTimestamp || 0) - (a.startTimestamp || 0))
        .slice(0, 10);
      let hw = 0, aw = 0, dr = 0, hg = 0, ag = 0;
      for (const e of recent) {
        const hScore = e.homeScore?.current ?? 0;
        const aScore = e.awayScore?.current ?? 0;
        hg += hScore; ag += aScore;
        if (hScore > aScore) hw++;
        else if (hScore < aScore) aw++;
        else dr++;
      }
      ctx.h2h = {
        homeWins: hw, awayWins: aw, draws: dr, total: recent.length,
        avgHomeGoals: hg / recent.length, avgAwayGoals: ag / recent.length,
      };
    }

    // ── INJURIES ─────────────────────────────────────────────
    await new Promise(r => setTimeout(r, 150));
    const injuryData = await sofascoreFetch('/matches/get-incidents', { matchId: eventId });
    // injuries come from team squad endpoint — use matches/get-lineups which has missing players
    // For injuries we use teams/get-squad and check injury status
    if (injuryData) {
      // Parse any pre-match injury/suspension incidents
      const incidents = injuryData.incidents || [];
      for (const inc of incidents) {
        if (inc.incidentType === 'injuryTime' || inc.incidentType === 'injury') {
          // Tag to home or away based on team
          const side = inc.isHome ? 'home' : 'away';
          ctx.injuries[side].push({ player: inc.player?.name || 'Unknown', type: inc.text || 'Injury' });
        }
      }
    }

    // ── HOME FORM + REST DAYS ────────────────────────────────
    await new Promise(r => setTimeout(r, 150));
    const homeMatches = await sofascoreFetch('/teams/get-last-matches', { id: homeTeamId, page: 0 });
    if (homeMatches?.events?.length) {
      ctx.homeForm = parseTeamForm(homeMatches.events, homeTeamId);
      if (ctx.homeForm) console.log(`  📊 Home [${homeTeamId}]: ${ctx.homeForm.wins}W${ctx.homeForm.draws}D${ctx.homeForm.losses}L rest:${ctx.homeForm.restDays}d`);
    } else {
      console.log(`  ⚠️ No form data for home team ${homeTeamId}`);
    }

    // ── AWAY FORM + REST DAYS ────────────────────────────────
    await new Promise(r => setTimeout(r, 150));
    const awayMatches = await sofascoreFetch('/teams/get-last-matches', { id: awayTeamId, page: 0 });
    if (awayMatches?.events?.length) {
      ctx.awayForm = parseTeamForm(awayMatches.events, awayTeamId);
      if (ctx.awayForm) console.log(`  📊 Away [${awayTeamId}]: ${ctx.awayForm.wins}W${ctx.awayForm.draws}D${ctx.awayForm.losses}L rest:${ctx.awayForm.restDays}d`);
    } else {
      console.log(`  ⚠️ No form data for away team ${awayTeamId}`);
    }

  } catch(e) {
    console.log(`  ⚠️ Match context error [${eventId}]: ${e.message}`);
  }

  cacheSet(matchContextCache, eventId, ctx);
  return ctx;
}

// ─── FETCH LINEUPS (called at 21:00 UK) ──────────────────────
async function fetchLineupsForToday() {
  const today = ukDateString();
  let fetched = 0;

  for (const sport of SPORTS) {
    const events = sofascoreCache.events[sport.key] || [];
    for (const event of events) {
      const eventDate = event.commence_time?.split('T')[0];
      if (eventDate !== today && eventDate !== ukDateString(new Date(Date.now() + 86400000))) continue;
      try {
        await new Promise(r => setTimeout(r, 200));
        const lineupData = await sofascoreFetch('/matches/get-lineups', { matchId: event.id });
        if (lineupData?.home && lineupData?.away) {
          const ctx = matchContextCache[event.id] || {};
          ctx.lineups = {
            homeFormation: lineupData.home?.formation || null,
            awayFormation: lineupData.away?.formation || null,
            homeConfirmed: lineupData.confirmed || false,
            homeMissing:   (lineupData.home?.missingPlayers || []).map(p => ({ name: p.player?.name, reason: p.type })),
            awayMissing:   (lineupData.away?.missingPlayers || []).map(p => ({ name: p.player?.name, reason: p.type })),
          };
          cacheSet(matchContextCache, event.id, ctx);
          fetched++;
          console.log(`  📋 Lineups: ${event.home_team} vs ${event.away_team} — confirmed: ${lineupData.confirmed}`);
        }
      } catch(e) { /* silent */ }
    }
  }
  console.log(`  📋 Lineups fetched: ${fetched} matches`);
}

// ─── PARSE TEAM FORM FROM LAST MATCHES ───────────────────────
function parseTeamForm(events, teamId) {
  // Filter to finished matches only
  const finished = events.filter(e =>
    e.status?.type === 'finished' && e.homeScore?.current != null
  );
  // Newest first, explicitly. lastMatchTimestamp below reads the first element
  // and calls it the most recent match, which is only true if this is sorted.
  finished.sort((a, b) => (b.startTimestamp || 0) - (a.startTimestamp || 0));
  const last5 = finished.slice(0, 5);
  if (!last5.length) return null;

  let wins = 0, draws = 0, losses = 0, goalsFor = 0, goalsAgainst = 0;
  let lastMatchTimestamp = null;

  for (const e of last5) {
    const isHome   = e.homeTeam?.id === teamId;
    const myScore  = isHome ? (e.homeScore?.current ?? 0) : (e.awayScore?.current ?? 0);
    const oppScore = isHome ? (e.awayScore?.current ?? 0) : (e.homeScore?.current ?? 0);
    goalsFor     += myScore;
    goalsAgainst += oppScore;
    if      (myScore > oppScore) wins++;
    else if (myScore < oppScore) losses++;
    else                         draws++;
    if (!lastMatchTimestamp && e.startTimestamp) lastMatchTimestamp = e.startTimestamp;
  }

  const restDays = lastMatchTimestamp
    ? Math.floor((Date.now() / 1000 - lastMatchTimestamp) / 86400)
    : 7;

  return {
    wins, draws, losses,
    gamesPlayed:  last5.length,
    goalsFor:     goalsFor  / last5.length,
    goalsAgainst: goalsAgainst / last5.length,
    formScore:    (wins * 3 + draws) / (last5.length * 3), // 0=poor, 1=perfect
    restDays,
  };
}

// ─── CONTEXT MODIFIERS FOR MODEL ─────────────────────────────
// Returns { attackMult, defenceMult, dataQuality, notes } for a team
// based on form, rest, injuries and lineups.
// leagueAvgGoals is what a team on this side of a fixture in this sport scores
// in an average game. It has to be passed in because this function is shared
// between football and NHL, whose scoring rates differ by a factor of two, and
// the recent-form adjustment below is only meaningful relative to one of them.
function getContextModifiers(teamSide, ctx, isHome, leagueAvgGoals, restBands, squadSize) {
  if (!ctx) return { attackMult: 1.0, defenceMult: 1.0, restPenalty: 0, dataQuality: 0.85, notes: 'No context' };

  const form    = isHome ? ctx.homeForm    : ctx.awayForm;
  const missing = ctx.lineups ? (isHome ? ctx.lineups.homeMissing : ctx.lineups.awayMissing) : [];
  const notes   = [];
  let attackMult  = 1.0;
  let defenceMult = 1.0;
  let dataQuality = 1.0;

  // ── Form multiplier ───────────────────────────────────────
  // formScore: 0=terrible, 0.5=average, 1.0=perfect
  if (form) {
    const formAdj = (form.formScore - 0.5) * 0.20; // ±10% max
    attackMult  += formAdj;
    defenceMult -= formAdj * 0.5;
    notes.push(`Form: ${form.wins}W${form.draws}D${form.losses}L`);

    // ── Rest days penalty ─────────────────────────────────
    // Bands come from the caller because they mean different things per sport
    // — see REST_BANDS_FOOTBALL and REST_BANDS_NHL. Without them, no penalty:
    // an adjustment that cannot be calibrated is better left out than guessed.
    const bands = restBands && Number.isFinite(restBands.heavy) && Number.isFinite(restBands.mild)
      ? restBands : null;
    if (!bands) {
      notes.push(`Rest: ${form.restDays}d`);
    } else if (form.restDays <= bands.heavy) {
      attackMult  *= 0.93; // fatigue reduces attacking output
      defenceMult *= 1.05; // and weakens defence
      notes.push(`Rest: ${form.restDays}d ⚠️`);
    } else if (form.restDays <= bands.mild) {
      attackMult  *= 0.97;
      notes.push(`Rest: ${form.restDays}d`);
    } else {
      notes.push(`Rest: ${form.restDays}d ✓`);
    }

    // ── Recent goals adjustment ────────────────────────────
    // If team's recent form goals are very different from season avg,
    // blend them in slightly
    if (form.gamesPlayed >= 3 && Number.isFinite(leagueAvgGoals) && leagueAvgGoals > 0) {
      // Proportional to the league, not to a constant.
      //
      // This was `(form.goalsFor - 1.3) * 0.10`, and 1.3 is roughly what a
      // football team scores in a game. The same function is used for NHL,
      // where a team averages 3.10 — so a side scoring exactly the league
      // average came out at (3.10 - 1.3) * 0.10 = +0.18, an eighteen percent
      // attack boost for being unremarkable. Both sides got it, so both lambdas
      // rose together: P(Over 5.5) went from 58.6% to 73.8%, a fair price of
      // 1.71 quoted as 1.36. The engine believed it had enormous edge on every
      // NHL over it could see.
      //
      // The comment on the original said "if recent form goals are very
      // different from season avg", which is the right idea — 1.3 was standing
      // in for the season average and only worked for the sport it came from.
      // A ratio says the same thing in a way that cannot be wrong per sport.
      const recentAttAdj = ((form.goalsFor / leagueAvgGoals) - 1) * 0.10;
      // A modifier has a safe default that a lambda does not: no adjustment.
      attackMult = clampFinite(attackMult + recentAttAdj, 0.7, 1.4) ?? 1;
    }
  } else {
    dataQuality = 0.90;
    notes.push('No form data');
  }

  // ── Missing players (from lineups) ────────────────────────
  // NOT position-weighted. The comment here used to say it was — "forwards
  // matter more for attack, defenders/GK for defence" — and no such weighting
  // exists; the impact is a flat count. It could not exist either, because
  // fetchLineupsForToday keeps only { name, reason } from each entry and throws
  // the position away. Saying so plainly beats describing a model the code does
  // not implement.
  //
  // Taken as a fraction of the side rather than a flat rate per player: three
  // absences out of eleven is a different thing from three out of eighteen, and
  // the same list used to cost both sports the same. Football is unchanged.
  if (missing.length > 0) {
    if (Number.isFinite(squadSize) && squadSize > 0) {
      const share = Math.min(1, missing.length / squadSize);
      attackMult  = Math.max(0.75, attackMult  - share * MISSING_ATTACK_IMPACT);
      defenceMult = Math.max(0.80, defenceMult + share * MISSING_DEFENCE_IMPACT);
    }
    // Without a squad size the absences are recorded and not priced, rather
    // than priced with a number that means nothing.
    notes.push(`Missing: ${missing.map(p => p.name).join(', ')}`);
    // A CAP, not a boost. This is Math.min, so it can only lower the tier —
    // from 1.0 to 0.95 — and the comment used to call it a quality boost. The
    // cap itself is right: knowing who is out beats not knowing, but a side
    // missing people is still a less certain thing to model.
    dataQuality = Math.min(dataQuality, 0.95);
  } else if (ctx.lineups?.homeConfirmed) {
    notes.push('Full squad ✓');
  }

  // Both multiply straight into expected goals, so both need bounding. Only
  // attackMult was clamped, and only inside the recent-form branch — so a team
  // with fewer than three recent matches, or with none, came out unbounded, and
  // defenceMult was never bounded at all. Neutral is the right fallback for a
  // modifier: an unusable adjustment means no adjustment.
  return {
    attackMult:  parseFloat((clampFinite(attackMult, 0.7, 1.4) ?? 1).toFixed(3)),
    defenceMult: parseFloat((clampFinite(defenceMult, 0.7, 1.4) ?? 1).toFixed(3)),
    dataQuality: parseFloat((clampFinite(dataQuality, 0.5, 1.0) ?? 0.85).toFixed(3)),
    notes:       notes.join(' | '),
    restDays:    form?.restDays || null,
  };
}

async function fetchFootballStats() {
  console.log('  ⚽ Fetching football team stats...');
  const footballSports = SPORTS.filter(s => s.name === 'Football');

  for (const sport of footballSports) {
    try {
      const seasonsData = await sofascoreFetch(`/tournaments/get-seasons`, { tournamentId: sport.tournamentId });
      if (!seasonsData?.seasons?.length) continue;
      const season = seasonsData.seasons[0];

      await new Promise(r => setTimeout(r, 300));

      const standings = await fetchTournamentStandings(sport.tournamentId, season.id);
      if (!standings) continue;

      const teams = buildTeamStatsFromStandings(standings, sport.tournamentId);

      // Drop this tournament's previous entries before writing the new ones.
      //
      // Every current team is overwritten each morning, so nothing here goes
      // stale the way the NHL and NBA caches did. What did accumulate is teams
      // that have LEFT the tournament: a relegated side keeps its entry for
      // ever, because nothing writes over a key that is no longer produced.
      //
      // That matters because of how the lookup falls back. An exact key miss —
      // a name variant from the fixture feed — walks the whole cache and
      // returns the first fuzzy match, and object key order is insertion
      // order, so last season's teams are tried BEFORE this season's. A
      // departed team whose name is close enough would hand its old stats to a
      // current fixture, as the model input for a published tip.
      //
      // Scoped to this tournament, and only once its fetch has succeeded, so a
      // league that failed today keeps yesterday's numbers rather than losing
      // them — which is the resilience the `continue`s above exist for.
      if (Object.keys(teams).length) {
        const suffix = `_${sport.tournamentId}`;
        for (const k of Object.keys(teamStatsCache)) {
          if (k.endsWith(suffix)) delete teamStatsCache[k];
        }
      }
      for (const [name, stats] of Object.entries(teams)) {
        teamStatsCache[`${name}_${sport.tournamentId}`] = stats;
      }

      // Name the season. Both this and fetchTournamentEvents take seasons[0]
      // and trust it, so a rollover that picks the wrong one, or a new season
      // whose table is not published yet, is invisible: it reads as a quiet
      // day. The season label makes it a fact in the log instead.
      const label = season.year || season.name || season.id;
      const count = Object.keys(teams).length;
      if (count) {
        console.log(`  ⚽ ${sport.league}: ${count} teams loaded (season ${label})`);
      } else {
        // Not recoverable further down: analyseFootballFixture returns null
        // when either side is missing from the cache, so this league publishes
        // nothing at all until a table appears.
        console.warn(`  ⚠️ ${sport.league}: standings for season ${label} produced no teams `
          + `— no ${sport.league} tips can be priced until this returns a table`);
      }
    } catch(e) {
      console.error(`Stats fetch error (${sport.league}):`, e.message);
    }
    await new Promise(r => setTimeout(r, 400));
  }

  // Fetch NHL goalie starters for today's games
  await fetchNHLGoalieData();

  // Pre-load NBA team stats once — all 30 teams in 1-2 API calls
  await fetchNBAAllTeamStats();
}

// ═══════════════════════════════════════════════════════════════
// MIDDAY ODDS REFRESH — 13:00 UK
// Refreshes odds only — keeps fixtures and stats from morning
// ═══════════════════════════════════════════════════════════════

async function middayOddsRefresh() {
  console.log('\n🔄 Midday odds refresh...');

  for (const sport of SPORTS) {
    const events = sofascoreCache.events[sport.key] || [];
    if (!events.length) continue;

    let updated = 0;
    for (const event of events) {
      try {
        const oddsRaw    = await fetchEventOdds(event.id);
        const targetLine = sport.name === 'Football' ? '2.5' : sport.name === 'Ice Hockey' ? '5.5' : null;
        const bookmakers = parseSofascoreOdds(oddsRaw, event.home_team, event.away_team, targetLine);
        if (bookmakers.length) {
          event.bookmakers = bookmakers;
          updated++;
        }
      } catch(e) {
        console.error(`Odds refresh error (${event.home_team} vs ${event.away_team}):`, e.message);
      }
      await new Promise(r => setTimeout(r, 300));
    }
    console.log(`  🔄 ${sport.league}: ${updated}/${events.length} events repriced`);
    // Only if something was actually repriced. A pass that updated nothing has
    // not refreshed these prices, and saying it did is the whole bug above.
    if (updated > 0) sofascoreCache.fetchedAt[sport.key] = new Date();
  }

  sofascoreCache.oddsFetchedAt = new Date();
  console.log('✅ Midday refresh complete.');
}

// ═══════════════════════════════════════════════════════════════
// NHL STATS — Free NHL API (api.nhle.com) — unchanged from v7
// ═══════════════════════════════════════════════════════════════

const nhlTeamCache = {};
let nhlTeamCacheDate = '';
let nhlAllTeamsCache = null;
let nhlAllTeamsCacheDate = '';

// Goalie data cache — populated once per morning fetch
// Structure: { 'Team Name': { starter: 'First Last', savePercent: 0.915, gaa: 2.45, gamesPlayed: 38, isConfirmed: true } }
const nhlGoalieCache = {};
let nhlGoalieCacheDate = '';

// ─── FETCH TODAY'S NHL GOALIE STARTERS ───────────────────────
// Uses api-web.nhle.com/v1/score/{date} which includes startingGoalie
// pre-game — boxscore (FUT state) has no player data before puck drop.
async function fetchNHLGoalieData() {
  const today = new Date().toLocaleDateString('en-CA', { timeZone: 'Europe/London' });
  if (nhlGoalieCacheDate === today && Object.keys(nhlGoalieCache).length > 0) return;

  try {
    const scoreRes = await fetch(`https://api-web.nhle.com/v1/score/${today}`, {
      headers: { 'User-Agent': 'TipsterEdge/1.0' }
    });
    if (!scoreRes.ok) {
      console.log(`  🥅 NHL score endpoint unavailable (HTTP ${scoreRes.status})`);
      nhlGoalieCacheDate = today;
      return;
    }
    const scoreData = await scoreRes.json();
    const games = scoreData.games || [];

    if (!games.length) {
      console.log(`  🥅 NHL: no games on ${today}`);
      nhlGoalieCacheDate = today;
      return;
    }

    let found = 0;
    for (const game of games) {
      for (const side of ['homeTeam', 'awayTeam']) {
        const team = game[side];
        if (!team) continue;
        const teamName = team.name?.default || team.commonName?.default || team.placeName?.default || '';
        if (!teamName) continue;

        const goalie = team.startingGoalie;
        if (!goalie) {
          console.log(`  🥅 ${teamName}: no starter confirmed yet`);
          continue;
        }

        const fullName = `${goalie.firstName?.default || ''} ${goalie.lastName?.default || ''}`.trim();
        let savePct  = parseFloat(goalie.savePctg  || goalie.savePct || 0);
        let gaa      = parseFloat(goalie.goalsAgainstAverage || goalie.gaa || 0);
        let gamesP   = parseInt(goalie.gamesPlayed || 0);

        // If stats not on starter object, fetch from player landing
        if (!savePct && goalie.playerId) {
          try {
            await new Promise(r => setTimeout(r, 200));
            const pRes = await fetch(`https://api-web.nhle.com/v1/player/${goalie.playerId}/landing`, {
              headers: { 'User-Agent': 'TipsterEdge/1.0' }
            });
            if (pRes.ok) {
              const pData = await pRes.json();
              const seasonStats = pData.seasonTotals?.find(s =>
                s.season === parseInt(`${currentSeason()}${currentSeason()+1}`) && s.gameTypeId === 2
              );
              savePct = parseFloat(seasonStats?.savePctg || pData.featuredStats?.regularSeason?.subSeason?.savePctg || 0);
              gaa     = parseFloat(seasonStats?.goalsAgainstAverage || pData.featuredStats?.regularSeason?.subSeason?.goalsAgainstAvg || 0);
              gamesP  = parseInt(seasonStats?.gamesPlayed || 0);
            }
          } catch(e) { /* silent */ }
        }

        nhlGoalieCache[teamName] = { starter: fullName, savePercent: savePct, gaa, gamesPlayed: gamesP, isConfirmed: true };
        console.log(`  🥅 ${teamName}: ${fullName} (SV%: ${savePct.toFixed(3)}, GAA: ${gaa.toFixed(2)}, GP: ${gamesP})`);
        found++;
      }
    }

    nhlGoalieCacheDate = today;
    console.log(`  🥅 NHL goalies loaded: ${found} confirmed starters for ${games.length} games`);
  } catch(e) {
    console.log(`  🥅 NHL goalie fetch failed: ${e.message} — model running without goalie data`);
  }
}

// ─── GOALIE QUALITY SCORE ────────────────────────────────────
// Returns a multiplier applied to expected goals against.
// Elite goalie (SV% ≥ .920) → reduces goals against
// Weak goalie  (SV% ≤ .895) → increases goals against
// No data → neutral (1.0)
// Recovered by fitting the band table this replaced: see goalieQualityMultiplier.
const GOALIE_DAMPING          = 0.67;
const GOALIE_FALLBACK_AVG_SV  = 0.905;
const GOALIE_MIN_SAMPLE       = 8;      // teams needed before an average means anything

// The league's average starting save percentage, from the goalies actually
// cached today. Falls back to the fitted constant when too few are known — an
// average over three teams is noise, and centring the whole scale on noise is
// worse than centring it on a stale constant.
function nhlLeagueAverageSavePct() {
  const svs = Object.values(nhlGoalieCache)
    .map(g => g && g.savePercent)
    .filter(v => Number.isFinite(v) && v > 0.5 && v < 1);
  if (svs.length < GOALIE_MIN_SAMPLE) return GOALIE_FALLBACK_AVG_SV;
  const mean = svs.reduce((a, b) => a + b, 0) / svs.length;
  // A mean outside this range means the feed changed shape rather than that the
  // league did.
  return (mean > 0.86 && mean < 0.94) ? mean : GOALIE_FALLBACK_AVG_SV;
}

function goalieQualityMultiplier(goalieName, teamName) {
  // Try exact team name, then fuzzy
  const data = nhlGoalieCache[teamName] ||
    Object.entries(nhlGoalieCache).find(([k]) => nameMatch(k, teamName))?.[1];

  if (!data || !data.savePercent) return { multiplier: 1.0, label: null };

  const sv = data.savePercent;

  // Measured against the league, not against a fixed table.
  //
  // This was six bands from 0.82 at 93% down to 1.18 below 89%. Fitting
  // `1 + d * ((1-sv)/(1-avg) - 1)` to those six points recovers avg = 0.905 and
  // d = 0.67 with an rms of 0.005 — so the bands were a discretised form of
  // exactly this, calibrated against a league save percentage of 0.905. The
  // shape was right; two things about it were not.
  //
  // Goals allowed scale with the shots that go in, so (1 - sv) is the quantity
  // that matters, and the damping is there because save percentage is partly
  // noise and partly the defence in front of the goalie.
  //
  // First, no band produced 1.0. They stepped 0.96 -> 1.03 straight over it, so
  // a goalie at exactly the league average was given a 3.5% penalty. That
  // double-counts: NHL_LEAGUE_AVG_GF already describes scoring against average
  // goaltending, and the multiplier is applied on top of it.
  //
  // Second, 0.905 was frozen. League save percentage moves by era — it ran
  // above 0.915 in the mid-2010s and below 0.900 recently — so the whole scale
  // drifts out of centre over time with nothing to say it has. The average now
  // comes from the goalie cache itself, which holds every team's starter, and
  // falls back to the fitted constant when too few are known to average.
  const avg = nhlLeagueAverageSavePct();
  const raw_ = 1 + GOALIE_DAMPING * (((1 - sv) / (1 - avg)) - 1);
  // The old bands stopped at 0.82 and 1.18; an outlier goalie should not move
  // the model further than the table ever allowed.
  const multiplier = Math.min(1.18, Math.max(0.82, parseFloat(raw_.toFixed(4))));

  return {
    multiplier,
    label: `${data.starter} (SV%: ${sv.toFixed(3)}, GAA: ${data.gaa.toFixed(2)})`,
    savePercent: sv,
    gaa: data.gaa,
    isConfirmed: data.isConfirmed,
  };
}

// The season to ask the NHL and NBA APIs about. Nothing else uses this —
// football seasons are handled by Sofascore's own season list.
//
// The boundary was July, and both of these leagues start in October. So from 1
// July every year the engine asked for a season that did not exist yet. Checked
// rather than assumed: on 20 August 2026, seasonId 20262027 returns 0 teams
// from api.nhle.com and 20252026 returns 32. Every NHL tip published in that
// window ran on league-average defaults while presenting a confidence figure
// that implies team data.
//
// October is the boundary because that is when these seasons begin. Between
// July and September it now names the season that just finished, which is the
// best data available about these teams until the new one has games in it.
const SEASON_START_MONTH = 10;
function currentSeason(now = new Date()) {
  return (now.getMonth() + 1) >= SEASON_START_MONTH
    ? now.getFullYear()
    : now.getFullYear() - 1;
}

// How many games a season needs before its numbers describe a team rather than
// a handful of nights. Below this the previous season is the better answer, and
// the alternative on offer is not "wait" but "use the league average".
const SEASON_MIN_GAMES = 5;

async function fetchNHLAllTeams() {
  const today = ukDateString();
  if (nhlAllTeamsCache && nhlAllTeamsCacheDate === today) return nhlAllTeamsCache;

  const seasonFor = (year) => `${year}${year + 1}`;
  const load = async (seasonId) => {
    const res = await fetch(
      `https://api.nhle.com/stats/rest/en/team/summary?cayenneExp=seasonId=${seasonId}%20and%20gameTypeId=2`
    );
    if (!res.ok) return null;
    const data = await res.json();
    return data.data || [];
  };

  try {
    const year = currentSeason();
    let seasonId = seasonFor(year);
    let teams = await load(seasonId);

    // A season that has started is not the same as a season with numbers in it.
    // Through the first fortnight every team is under SEASON_MIN_GAMES, and
    // fetchNHLTeamStats rejects those rows one by one — so the model silently
    // falls back to league averages for the opening weeks of every season. Last
    // season's rates are a far better description of a team than the league
    // average, so use them until this one has something to say.
    const usable = (rows) => (rows || []).filter(t => (t.gamesPlayed || 0) >= SEASON_MIN_GAMES).length;
    if (usable(teams) === 0) {
      const prevId = seasonFor(year - 1);
      const prev = await load(prevId);
      if (usable(prev) > 0) {
        console.log(`🏒 NHL ${seasonId} has no team with ${SEASON_MIN_GAMES}+ games yet `
          + `— using ${prevId}`);
        teams = prev;
        seasonId = prevId;
      }
    }
    if (teams === null) return null;

    nhlAllTeamsCache = teams;
    nhlAllTeamsCacheDate = today;
    console.log(`🏒 NHL team stats loaded: ${nhlAllTeamsCache.length} teams (season ${seasonId})`);
    return nhlAllTeamsCache;
  } catch(e) { console.error('NHL API error:', e.message); return null; }
}

async function fetchNHLTeamStats(teamName) {
  // fetchNHLAllTeams below refreshes daily, so this cache in front of it has
  // to expire with it. Keyed by season alone it froze a team's scoring rates
  // at whatever they were the first time that team appeared on a card — five
  // games in, at their noisiest — and served those for the rest of the season,
  // because the fresh all-teams data is never consulted for a team already
  // cached. Everything downstream is built from these two numbers: expected
  // margin, win probability, confidence, edge, stake.
  const today = ukDateString();
  if (nhlTeamCacheDate !== today) {
    for (const k of Object.keys(nhlTeamCache)) delete nhlTeamCache[k];
    nhlTeamCacheDate = today;
  }
  const cacheKey = `${teamName}_${currentSeason()}`;
  if (nhlTeamCache[cacheKey]) return nhlTeamCache[cacheKey];
  const allTeams = await fetchNHLAllTeams();
  if (!allTeams) return null;
  const team = allTeams.find(t =>
    nameMatch(t.teamFullName, teamName) || nameMatch(t.teamName, teamName)
  );
  if (!team || (team.gamesPlayed || 0) < SEASON_MIN_GAMES) return null;
  const result = {
    teamId:       team.teamId,
    gamesPlayed:  team.gamesPlayed,
    goalsFor:     parseFloat(team.goalsForPerGame || 0),
    goalsAgainst: parseFloat(team.goalsAgainstPerGame || 0),
  };
  nhlTeamCache[cacheKey] = result;
  console.log(`  🏒 NHL ${teamName}: ${result.goalsFor.toFixed(2)} GF/gm, ${result.goalsAgainst.toFixed(2)} GA/gm`);
  return result;
}

// ═══════════════════════════════════════════════════════════════
// NBA STATS — NBA Official Stats API (stats.nba.com)
// Free, no API key required. Uses season team stats endpoint.
// Data cached per morning fetch — no mid-cycle calls.
// ═══════════════════════════════════════════════════════════════

const nbaTeamCache = {};
let nbaTeamCacheDate = '';
let nbaAllTeamsCache = null;
let nbaAllTeamsCacheDate = '';

// NBA team name → abbreviation map for matching Sofascore names
const NBA_TEAM_ABBREVS = {
  'Atlanta Hawks': 'ATL', 'Boston Celtics': 'BOS', 'Brooklyn Nets': 'BKN',
  'Charlotte Hornets': 'CHA', 'Chicago Bulls': 'CHI', 'Cleveland Cavaliers': 'CLE',
  'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN', 'Detroit Pistons': 'DET',
  'Golden State Warriors': 'GSW', 'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND',
  'Los Angeles Clippers': 'LAC', 'Los Angeles Lakers': 'LAL', 'Memphis Grizzlies': 'MEM',
  'Miami Heat': 'MIA', 'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN',
  'New Orleans Pelicans': 'NOP', 'New York Knicks': 'NYK', 'Oklahoma City Thunder': 'OKC',
  'Orlando Magic': 'ORL', 'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHX',
  'Portland Trail Blazers': 'POR', 'Sacramento Kings': 'SAC', 'San Antonio Spurs': 'SAS',
  'Toronto Raptors': 'TOR', 'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS',
};

// One season's team stats, or null if that season has nothing to give.
//
// Split out of fetchNBAAllTeamStats so the season can be chosen by whether it
// has data, rather than by the calendar alone. seasonStr is the NBA's own form,
// e.g. "2025-26".
async function loadNBASeason(seasonStr) {
  try {
    const url = `https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=${seasonStr}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision=`;

    const res = await fetch(url, {
      headers: {
        'User-Agent':  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer':     'https://www.nba.com/',
        'Origin':      'https://www.nba.com',
        'Accept':      'application/json, text/plain, */*',
        'x-nba-stats-origin': 'stats',
        'x-nba-stats-token':  'true',
      },
    });

    if (!res.ok) {
      console.log(`  🏀 NBA stats API HTTP ${res.status} (${seasonStr})`);
      return null;
    }

    const data = await res.json();
    const headers = data.resultSets?.[0]?.headers || [];
    const rows    = data.resultSets?.[0]?.rowSet   || [];

    if (!rows.length) { console.log(`  🏀 NBA stats: no rows for ${seasonStr}`); return null; }

    // Build lookup: teamName → { ptsFor, ptsAgainst, gamesPlayed, teamId }
    const idx = (h) => headers.indexOf(h);
    const teamNameIdx = idx('TEAM_NAME');
    const gpIdx       = idx('GP');
    const ptsIdx      = idx('PTS');
    const oppPtsIdx   = idx('OPP_PTS') !== -1 ? idx('OPP_PTS') : -1;

    const result = {};
    for (const row of rows) {
      const name = row[teamNameIdx];
      const gp   = parseInt(row[gpIdx] || 0);
      const pts  = parseFloat(row[ptsIdx] || 0);
      // OPP_PTS may not be in Base — fallback handled below
      const oppPts = oppPtsIdx !== -1 ? parseFloat(row[oppPtsIdx] || 0) : 0;
      if (name && gp >= SEASON_MIN_GAMES) {
        result[name] = { ptsFor: pts, ptsAgainst: oppPts, gamesPlayed: gp };
      }
    }

    // If OPP_PTS missing, fetch opponent stats separately
    if (oppPtsIdx === -1 || !Object.values(result).some(t => t.ptsAgainst > 0)) {
      const urlOpp = url.replace('MeasureType=Base', 'MeasureType=Opponent');
      try {
        const resOpp = await fetch(urlOpp, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nba.com/', 'Origin': 'https://www.nba.com',
            'x-nba-stats-origin': 'stats', 'x-nba-stats-token': 'true',
          },
        });
        if (resOpp.ok) {
          const dataOpp   = await resOpp.json();
          const hdrsOpp   = dataOpp.resultSets?.[0]?.headers || [];
          const rowsOpp   = dataOpp.resultSets?.[0]?.rowSet   || [];
          const nameIdxO  = hdrsOpp.indexOf('TEAM_NAME');
          const ptsIdxO   = hdrsOpp.indexOf('OPP_PTS');
          for (const row of rowsOpp) {
            const name = row[nameIdxO];
            if (result[name] && ptsIdxO !== -1) {
              result[name].ptsAgainst = parseFloat(row[ptsIdxO] || 0);
            }
          }
        }
      } catch(e) { /* silent — model still works with pts allowed estimate */ }
    }

    return result;
  } catch(e) {
    console.log(`  🏀 NBA stats fetch error (${seasonStr}): ${e.message}`);
    return null;
  }
}

async function fetchNBAAllTeamStats() {
  const today = ukDateString();
  if (nbaAllTeamsCache && nbaAllTeamsCacheDate === today) return nbaAllTeamsCache;

  const year  = currentSeason();
  const label = (y) => `${y}-${String(y + 1).slice(2)}`;   // "2025-26"

  let seasonStr = label(year);
  let result = await loadNBASeason(seasonStr);

  // Same reasoning as the NHL side. A season that has started is not a season
  // with numbers in it: every team is below SEASON_MIN_GAMES for the opening
  // fortnight, which leaves this map empty and the model on league averages.
  // Last season's rates describe these teams far better than that.
  if (!result || Object.keys(result).length === 0) {
    const prevStr = label(year - 1);
    const prev = await loadNBASeason(prevStr);
    if (prev && Object.keys(prev).length) {
      console.log(`  🏀 NBA ${seasonStr} has no team with ${SEASON_MIN_GAMES}+ games yet `
        + `— using ${prevStr}`);
      result = prev;
      seasonStr = prevStr;
    }
  }
  if (!result) return null;

  nbaAllTeamsCache     = result;
  nbaAllTeamsCacheDate = today;
  console.log(`  🏀 NBA team stats loaded: ${Object.keys(result).length} teams (season ${seasonStr})`);
  return result;
}

async function fetchNBATeamStats(teamName) {
  // Same daily expiry as the NHL side, and the same reason. This one also
  // fronts the opening-fortnight fallback to last season's rates: without an
  // expiry, a team first requested before the new season had numbers kept
  // last season's all the way through this one.
  const today = ukDateString();
  if (nbaTeamCacheDate !== today) {
    for (const k of Object.keys(nbaTeamCache)) delete nbaTeamCache[k];
    nbaTeamCacheDate = today;
  }
  const cacheKey = `${teamName}_${currentSeason()}`;
  if (nbaTeamCache[cacheKey]) return nbaTeamCache[cacheKey];

  const allTeams = await fetchNBAAllTeamStats();
  if (!allTeams) return null;

  // Try exact match first, then fuzzy
  let stats = allTeams[teamName];
  if (!stats) {
    const match = Object.entries(allTeams).find(([k]) => nameMatch(k, teamName));
    if (match) stats = match[1];
  }

  if (!stats) {
    console.log(`  🏀 NBA: no stats match for "${teamName}" — available: ${Object.keys(allTeams).slice(0,3).join(', ')}...`);
    return null;
  }

  const result = { gamesPlayed: stats.gamesPlayed, ptsFor: stats.ptsFor, ptsAgainst: stats.ptsAgainst };
  nbaTeamCache[cacheKey] = result;
  console.log(`  🏀 NBA ${teamName} [${result.gamesPlayed}gm]: ${result.ptsFor.toFixed(1)} pts, ${result.ptsAgainst.toFixed(1)} allowed`);
  return result;
}

// ═══════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════

// Does one team name refer to the same team as another?
//
// This decides which price belongs to which side, which cached fixture a tip
// settles against, and which team's stats feed the model. Everything below it
// treats the answer as ground truth, so a wrong answer here is invisible
// downstream — there is no later check that would notice.
//
// It used to strip to [a-z0-9] and accept containment in either direction.
// Two problems with that.
//
// A name made entirely of characters outside [a-z0-9] cleaned to the empty
// string, and every string contains the empty string — so any Cyrillic, Greek
// or punctuation-only name matched EVERY team. `nameMatch('Зенит', 'Arsenal')`
// returned true. The `!a || !b` guard above caught a name that arrived empty,
// not one that became empty. Worst case: the settler's fixture lookup matched
// the first cached event and graded a tip against a different match's score.
//
// And plain containment matches names that merely share a prefix. 'Inter'
// matched 'Internacional'; 'Athletic' matched 'Athletico'. Comparing token sets
// instead keeps every legitimate case — 'Roma' inside 'AS Roma', 'Forest'
// inside 'Nottingham Forest', 'Tottenham' inside 'Tottenham Hotspur' — while
// refusing both of those, because a token has to match a whole token.
//
// Unicode-aware, so a non-Latin name is compared rather than erased. It still
// will not match a name against its own transliteration; that is a false
// negative, and a tip nobody can match is dropped rather than mis-assigned.
function nameMatch(a, b) {
  if (!a || !b) return false;
  // NFD then drop the combining marks, so Munchen and München are the same
  // word. Feeds disagree about diacritics constantly, and without this the
  // disagreement reads as two different clubs. Cyrillic and Greek letters are
  // unaffected — they survive as letters rather than decomposing away.
  const tokens = (s) => String(s).normalize('NFD').replace(/\p{M}/gu, '').toLowerCase()
    .split(/[^\p{L}\p{N}]+/u)
    .filter(Boolean);
  const at = tokens(a), bt = tokens(b);
  // A name with nothing in it matches nothing, rather than everything.
  if (!at.length || !bt.length) return false;

  const aj = at.join(''), bj = bt.join('');
  if (aj === bj) return true;

  // Otherwise every token of the shorter name has to appear in the longer one.
  const [short, long] = at.length <= bt.length ? [at, bt] : [bt, at];
  const set = new Set(long);
  return short.every(t => set.has(t));
}

// Wall-clock time in London, as a Date whose local Y/M/D/H/M/S fields hold the
// UK values. The scheduler reads getHours/getMinutes/getDay/toDateString off it.
//
// This previously did:
//     new Date(new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' }))
// en-GB formats as DD/MM/YYYY, but V8's legacy string parser reads a
// slash-separated date as MM/DD/YYYY. So on days 1-12 it silently produced the
// WRONG date (11/08 became 8 November, breaking the Saturday check), and on
// days 13-31 the month was out of range and it returned Invalid Date — where
// getHours() is NaN, every `h === 6/7/8/13/21` comparison is false, and no
// scheduled job fired at all. That is 19 days in every month with no morning
// fetch, no Pro emails, no free emails, no acca and no odds refresh.
//
// formatToParts avoids parsing a formatted string back into a Date. The `now`
// parameter exists so this can be tested against fixed instants.
// Start of the current UK day, as a real instant.
//
// Vercel runs in UTC, so `new Date(); d.setHours(0,0,0,0)` anchors to UTC
// midnight — which is 01:00 UK during BST. Between 00:00 and 01:00 UK the
// window therefore still covered the previous day, so these pages queried
// yesterday's tips and advertised yesterday's date in the title, the H1 and
// the structured data.
// The UK calendar day, which is the only day this product has: tips are carded
// by it, emails are scheduled by it, and the admin panel keys its overrides by
// it. toISOString() gives the UTC day, which during BST is the previous day
// between 00:00 and 01:00 UK — so a lookup written against the UK date and read
// against the UTC one silently miss each other inside that hour.
function ukDateString(now = new Date()) {
  return now.toLocaleDateString('en-CA', { timeZone: 'Europe/London' });
}

function ukDayStart(now = new Date()) {
  // The calendar date as London sees it, e.g. "2026-08-18".
  const ymd = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(now);

  // Take UTC midnight of that date, then subtract whatever offset London was
  // on at that instant. Reading the offset from the guess (rather than assuming
  // GMT or BST) keeps this correct across both transitions.
  const guess = new Date(ymd + 'T00:00:00Z');
  const hourInLondon = Number(new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London', hour: '2-digit', hour12: false,
  }).format(guess)) % 24;

  return new Date(guess.getTime() - hourInLondon * 3600000);
}

function ukTime(now = new Date()) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  }).formatToParts(now).reduce((acc, p) => { acc[p.type] = p.value; return acc; }, {});

  return new Date(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    // Some ICU builds render midnight as "24" under hour12:false.
    Number(parts.hour) % 24,
    Number(parts.minute),
    Number(parts.second)
  );
}

function generateTipRef(sport) {
  const prefixes = { 'Football': 'FB', 'Basketball': 'BB', 'Ice Hockey': 'IH' };
  const prefix = prefixes[sport] || 'TT';
  const ts   = Date.now().toString(36).toUpperCase().slice(-4);
  const rand = crypto.randomBytes(3).toString('hex').toUpperCase();
  return `${prefix}-${ts}${rand}`;
}

// ─── MATHS ────────────────────────────────────────────────────

// Math.min and Math.max propagate NaN. Every clamp in the analysers reads as
// though it bounds its value to a sensible range, and does nothing whatever
// when the input is NaN: Math.min(4.0, NaN) is NaN and Math.max(0.3, NaN) is
// NaN. Infinity clamps correctly; only NaN walks through.
//
// That matters because NaN then survives everything downstream. A NaN lambda
// gives a NaN score matrix, NaN probabilities and a NaN fair price, and every
// comparison against NaN is false — so none of the thresholds meant to catch
// nonsense reject it. It is reachable from one non-numeric field in a
// standings response.
//
// Returns null rather than a number when the input is not finite, because
// there is no safe default for expected goals; the caller has to decide.
function clampFinite(x, lo, hi) {
  return Number.isFinite(x) ? Math.max(lo, Math.min(hi, x)) : null;
}

function poisson(lambda, k) {
  // A non-finite lambda would otherwise return NaN and poison the whole matrix.
  if (!Number.isFinite(lambda)) return 0;
  if (lambda <= 0) return k === 0 ? 1 : 0;
  let logP = -lambda + k * Math.log(lambda);
  for (let i = 1; i <= k; i++) logP -= Math.log(i);
  return Math.exp(logP);
}

function dixonColesTau(h, a, lH, lA, rho) {
  if (h === 0 && a === 0) return 1 - lH * lA * rho;
  if (h === 1 && a === 0) return 1 + lA * rho;
  if (h === 0 && a === 1) return 1 + lH * rho;
  if (h === 1 && a === 1) return 1 - rho;
  return 1;
}

function buildScoreMatrix(lH, lA) {
  const N = MATRIX_MAX_GOALS;
  const matrix = [];
  let total = 0;
  for (let h = 0; h <= N; h++) {
    matrix[h] = [];
    for (let a = 0; a <= N; a++) {
      const raw = poisson(lH, h) * poisson(lA, a);
      const tau = dixonColesTau(h, a, lH, lA, DC_RHO);
      // Math.max(0, NaN) is NaN — the same trap as the clamps in the analysers,
      // one layer further down. poisson already refuses a non-finite lambda,
      // but dixonColesTau multiplies the lambdas directly and will hand back
      // NaN for one, so the guard has to be here rather than only upstream.
      const cell = raw * tau;
      matrix[h][a] = Number.isFinite(cell) ? Math.max(0, cell) : 0;
      total += matrix[h][a];
    }
  }
  if (total > 0) {
    for (let h = 0; h <= N; h++)
      for (let a = 0; a <= N; a++)
        matrix[h][a] /= total;
  }
  return matrix;
}

function calcOutcomes(matrix) {
  const N = MATRIX_MAX_GOALS;
  let homeWin = 0, draw = 0, awayWin = 0, over25 = 0;
  for (let h = 0; h <= N; h++) {
    for (let a = 0; a <= N; a++) {
      const p = matrix[h][a];
      if (h > a) homeWin += p;
      else if (h === a) draw += p;
      else awayWin += p;
      if (h + a > 2.5) over25 += p;
    }
  }
  return { homeWin, draw, awayWin, over25 };
}

function fairOdds(prob) {
  // NaN <= 0.001 is false, so without this a NaN probability produced a NaN
  // price and published it.
  if (!Number.isFinite(prob) || prob <= 0.001) return 999.0;
  return parseFloat((1 / prob).toFixed(2));
}

function calcEdge(modelProb, trueImpliedProb) {
  return parseFloat(((modelProb - trueImpliedProb) * 100).toFixed(2));
}

// Stake tiers, keyed on the quarter-Kelly fraction.
//
// These used to be [0.40, 0.28, 0.18, 0.11, 0.06]. Those are full-Kelly
// numbers, but they were compared against the QUARTER-Kelly value, which can
// never exceed 0.25 (full Kelly is p - q/b, bounded by 1). So the 3.0u and
// 2.5u tiers were unreachable and 66% of publishable bets collapsed onto the
// 0.5u floor — the model's discrimination was being thrown away.
//
// The thresholds were calibrated over odds 1.10-6.00 with edge 8-20 points,
// where they give 0.5u 20% / 1u 36% / 1.5u 30% / 2u 8% / 2.5u 5% / 3u 2% and
// average 1.24u. That calibration band is not the band the engine actually
// stakes in, which is BET_ODDS_MIN (1.35) to ODDS_ELITE_MAX (10.0). Measured
// over the real band:
//
//     edge 8-20 pts:   0.5u 30%   1u 43%   1.5u 21%   2u 4%   2.5u 2%   3u 0%   avg 1.03u
//     edge 0-20 pts:   0.5u 56%   1u 27%   1.5u 13%   2u 3%   2.5u 1%   3u 0%   avg 0.83u
//
// So 3u is effectively unreachable in practice and the distribution sits lower
// than the calibration suggests. Recorded rather than retuned: moving these
// thresholds changes what every subscriber is advised to stake, and the right
// band to calibrate over depends on whether ODDS_ELITE_MAX really should be
// 10.0 — the file header says the ceiling is 2.50, which the code has never
// enforced.
//
// This table is the single place to tune staking. Lower every threshold to
// stake more, raise them to stake less.
const KELLY_TIERS = [
  [0.1725, 3.0],
  [0.1079, 2.5],
  [0.0786, 2.0],
  [0.0546, 1.5],
  [0.0371, 1.0],
];
const KELLY_MIN_STAKE = 0.5;

function kellyStake(modelProb, decimalOdds, fraction = 0.25) {
  const b = decimalOdds - 1;
  // Degenerate input means we cannot size a bet, so stake nothing. This
  // returned 1.0 — a full unit staked off odds <= 1.00 or a probability
  // outside (0,1), which is exactly the input you least want to bet on.
  // Note this runs during analysis, BEFORE applyStrictRules filters the odds
  // range, so a bad price from the feed reached it directly. Returning 0 is
  // also what the negative-edge branch below already does.
  if (!Number.isFinite(b) || b <= 0) return 0;
  if (!Number.isFinite(modelProb) || modelProb <= 0 || modelProb >= 1) return 0;
  const q    = 1 - modelProb;
  const full = (b * modelProb - q) / b;
  if (full <= 0) return 0;
  const sized = full * fraction;
  for (const [threshold, units] of KELLY_TIERS) {
    if (sized >= threshold) return units;
  }
  return KELLY_MIN_STAKE;
}

function normalCDF(x) {
  // Abramowitz & Stegun 26.2.17 specifies 0.2316419; this had 0.2315419.
  // The error was tiny — at most 4.3e-5 of probability, against edges measured
  // in whole percentage points — so this is correctness housekeeping rather
  // than a behaviour change.
  const t = 1 / (1 + 0.2316419 * Math.abs(x));
  const d = 0.3989423 * Math.exp(-x * x / 2);
  const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.7814779 + t * (-1.8212560 + t * 1.3302744))));
  return x >= 0 ? 1 - p : p;
}

function winProbFromMargin(expectedMargin, stdDev = NBA_SCORE_STD_DEV) {
  return normalCDF(expectedMargin / stdDev);
}

// ─── MARKET DATA EXTRACTION ───────────────────────────────────
// Splits football (3-way) from NBA/NHL (2-way) cleanly.
// Returns null if no valid market found.
// Pick the book to de-vig against.
//
// This used to be books[0] — whichever the feed happened to list first. Two
// problems with that, and the second is the worse one.
//
// A book's margin is how far its prices sit from the true probability, so the
// tightest book is the best estimate available. Taking an arbitrary one means a
// soft book with a 12% margin can set the model's view of a match while a 3%
// book sits unused two entries later.
//
// And feed order is not guaranteed stable. If the bookmakers come back in a
// different order at 13:00 than at 06:00, the de-vigged probability moves with
// no price having changed anywhere — so the published edge shifts for no
// reason, and the line-move rejection in saveTips is comparing against noise.
// Sorting by margin makes the choice deterministic; the title tie-break makes
// it deterministic even between two books quoting identical prices.
//
// Averaging across books is not the alternative: de-vigging needs a coherent
// set of prices from one book, and an average of several is not one.
function sharpestBook(books, sumImplied) {
  let best = null, bestMargin = Infinity;
  for (const b of books) {
    const m = sumImplied(b);
    if (!Number.isFinite(m) || m <= 0) continue;
    if (m < bestMargin || (m === bestMargin && best && String(b.title) < String(best.title))) {
      best = b; bestMargin = m;
    }
  }
  return best;
}

function extractMarketData(event) {
  const books1x2  = [];
  const books2way = [];
  let bestOver25 = 0, bestOver25Book = '';
  let bestOver25True = 0;   // de-vigged P(Over) from the same book

  for (const book of (event.bookmakers || [])) {
    const h2h = book.markets?.find(m => m.key === 'h2h');
    if (h2h) {
      const outcomes = h2h.outcomes || [];
      const drawOut  = outcomes.find(o => o.name === 'Draw');
      const bDraw    = drawOut?.price || 0;

      // Map by name — never by position to avoid feed-order flipping
      const homeOut = outcomes.find(o =>
        o.name !== 'Draw' && nameMatch(o.name, event.home_team)
      );
      const awayOut = outcomes.find(o =>
        o.name !== 'Draw' && nameMatch(o.name, event.away_team)
      );

      // Fallback: if name matching fails, use position (first non-draw = home, second = away)
      const nonDraw = outcomes.filter(o => o.name !== 'Draw');
      const bHome = homeOut?.price || nonDraw[0]?.price || 0;
      const bAway = awayOut?.price || nonDraw[1]?.price || 0;

      if (bHome > 0 && bDraw > 0 && bAway > 0) {
        books1x2.push({ title: book.title, home: bHome, draw: bDraw, away: bAway });
      } else if (bHome > 0 && bAway > 0) {
        books2way.push({ title: book.title, home: bHome, away: bAway });
      }
    }
    // Totals. The Over price alone is not enough: every other market here is
    // de-vigged against the rest of its own book's line, and the Over was not,
    // so its "true implied" carried the bookmaker's margin while the moneyline
    // markets did not. The published edge then meant two different things
    // depending on the market.
    //
    // The Under is already in the feed — the parser only emits a totals market
    // when both sides are present — it was simply never read. De-vig against
    // the SAME book, which is the only pairing that yields a real overround.
    const totals = book.markets?.find(m => m.key === 'totals');
    if (totals) {
      const over  = totals.outcomes.find(o => o.name === 'Over'  && (o.price || 0) > 0);
      const under = totals.outcomes.find(o => o.name === 'Under' && (o.price || 0) > 0);
      if (over && over.price > bestOver25) {
        bestOver25 = over.price;
        bestOver25Book = book.title;
        bestOver25True = under
          ? (1 / over.price) / ((1 / over.price) + (1 / under.price))
          : 0;   // no Under from this book — cannot de-vig, so do not price it
      }
    }
  }

  // 3-way (football)
  if (books1x2.length > 0) {
    const b = sharpestBook(books1x2, (x) => (1/x.home) + (1/x.draw) + (1/x.away));
    if (!b) return null;
    const totalIP = (1/b.home) + (1/b.draw) + (1/b.away);
    return {
      trueHome:   (1/b.home) / totalIP,
      trueDraw:   (1/b.draw) / totalIP,
      trueAway:   (1/b.away) / totalIP,
      homeOdds:   b.home, drawOdds: b.draw, awayOdds: b.away,
      over25Odds: bestOver25,
      over25True: bestOver25True,
      homeBook:   b.title, drawBook: b.title, awayBook: b.title, over25Book: bestOver25Book,
      bookCount:  books1x2.length,
      avgMargin:  parseFloat(((totalIP - 1) * 100).toFixed(2)),
      isTwoWay:   false,
    };
  }

  // 2-way (NBA/NHL)
  if (books2way.length > 0) {
    const b = sharpestBook(books2way, (x) => (1/x.home) + (1/x.away));
    if (!b) return null;
    const totalIP = (1/b.home) + (1/b.away);
    return {
      trueHome:   (1/b.home) / totalIP,
      trueDraw:   0,
      trueAway:   (1/b.away) / totalIP,
      homeOdds:   b.home, drawOdds: 0, awayOdds: b.away,
      over25Odds: bestOver25,
      over25True: bestOver25True,
      homeBook:   b.title, drawBook: '', awayBook: b.title, over25Book: bestOver25Book,
      bookCount:  books2way.length,
      avgMargin:  parseFloat(((totalIP - 1) * 100).toFixed(2)),
      isTwoWay:   true,
    };
  }

  return null;
}

// ─── FALSE-EDGE PROTECTION ────────────────────────────────────
// Rejects suspicious edge values that likely stem from model error
// rather than genuine market inefficiency.
// Returns null if the candidate should be rejected, otherwise candidate unchanged.
function falseEdgeCheck(candidate, market) {
  const { edge, modelProb, trueImplied } = candidate;

  // NOTE: this clause is currently unreachable and has no effect.
  // The intent reads as "allow a >20% edge if at least three books confirm it",
  // but edge is (modelProb - trueImplied) * 100, so edge > 20 implies
  // divergence > 0.20 and the check below rejects the candidate regardless of
  // bookCount. Verified over 8,100 probability pairs: bookCount never changes
  // the verdict, and nothing with edge > 20 is ever accepted.
  //
  // Left in place rather than deleted, because restoring the intended
  // behaviour means loosening a rejection filter — a decision about which bets
  // get published, not a tidy-up.
  if (edge > 20 && market.bookCount < 3) return null;

  // Reject if model probability diverges from market by > 20 percentage points
  // in EITHER direction. This is what actually caps extreme edges.
  const divergence = Math.abs(modelProb - trueImplied);
  if (divergence > 0.20) return null;

  return candidate;
}

// ─── CANDIDATE SCORING ────────────────────────────────────────

// Confidence ceilings. These were declared and never read — the caps below were
// hardcoded as literals, so raising a ceiling here changed nothing. The NHL
// value was also wrong as documentation: the real cap is 88 once confirmed
// goalie data is present, which a single constant could not express.
const NBA_CONFIDENCE_CEILING      = 80; // cap until injury/rest data available
const NHL_CONFIDENCE_CEILING      = 80; // without confirmed goalie data
const NHL_CONFIDENCE_CEILING_GOALIE = 88; // with it
const CONFIDENCE_FLOOR            = 50; // never publish below this
const CONFIDENCE_CEILING          = 95; // absolute cap, all sports

// ─── CONFIDENCE = MODEL WIN PROBABILITY ──────────────────────
// Confidence is literally how likely we think this outcome is to happen.
// modelProb is already the margin-stripped win probability from the model.
// We display it directly — no bands, no transformations.
//
// Adjustments applied:
//   - Data penalty: fewer games = less reliable probability → reduce slightly
//   - Sport caps: NBA/NHL capped at 80% until we have lineup/injury data
//     (a model saying 85% without goalie data is overconfident)
//
// Output: integer percentage, e.g. 67 means "67% chance this wins"
function confidenceFromSignals({ modelProb, dataQualityTier, sport, gamesPlayed }) {
  // Start with the raw model probability as a percentage
  let conf = Math.round(modelProb * 100);

  // Data penalty — fewer games means less reliable estimates
  const games = gamesPlayed || 0;
  if      (games < 4)  conf -= 8;
  else if (games < 8)  conf -= 4;
  else if (games < 15) conf -= 2;
  // 15+ games: no penalty

  // Data quality tier penalty (incomplete stats)
  if (!dataQualityTier || dataQualityTier < 1.0) conf -= 3;

  // Sport caps — NHL cap lifted to 88 when confirmed goalie data is present
  // Without goalie data (dataQualityTier < 1.0), cap stays at 80
  if (sport === 'Ice Hockey') {
    const cap = (dataQualityTier >= 1.0) ? NHL_CONFIDENCE_CEILING_GOALIE : NHL_CONFIDENCE_CEILING;
    conf = Math.min(conf, cap);
  }
  if (sport === 'Basketball') conf = Math.min(conf, NBA_CONFIDENCE_CEILING);

  // Hard floor — don't publish tips below this win probability
  conf = Math.max(CONFIDENCE_FLOOR, Math.min(CONFIDENCE_CEILING, conf));

  return conf;
}

function scoreCandidate({ edgePct, modelProb, dataQualityTier }) {
  const edgeScore    = Math.min(1, Math.max(0, edgePct) / 20);
  const probStrength = Math.min(1, Math.abs(modelProb - 0.5) / 0.5);
  // 60% edge, 25% probability strength, 15% data quality
  return edgeScore * 0.60 + probStrength * 0.25 + (dataQualityTier || 1.0) * 0.15;
}

function pickBestCandidate(candidates) {
  if (!candidates.length) return null;
  const scored = candidates.map(c => {
    const qualityScore = scoreCandidate({
      edgePct:         c.edge,
      modelProb:       c.modelProb,
      dataQualityTier: c.dataQualityTier || 1.0,
    });
    // Composite rank: 70% quality score + 30% normalised edge (capped at 25%)
    const normEdge   = Math.min(1, Math.max(0, c.edge) / 25);
    const composite  = qualityScore * 0.70 + normEdge * 0.30;
    return { ...c, qualityScore, composite };
  });
  const best = scored.reduce((a, b) => a.composite >= b.composite ? a : b);
  if (best.qualityScore < MIN_QUALITY_SCORE) return null;
  return best;
}

// Refuse a candidate on the shape of the match rather than on its price.
//
// Worth writing down which of these can fire, because four conditions read as
// four filters and two of them are already guaranteed by the callers:
//
//   hasCoreData        football returns null earlier when stats are missing,
//                      and the other two call sites pass a literal true.
//                      Never false at any caller.
//   hasCompleteMarket  every caller has already returned on a null market.
//                      Never false either.
//   topOutcomeProb     live for football, where three outcomes share the
//                      probability and the best can sit below 0.42. Dead for
//                      the two-way sports: their probabilities sum to exactly
//                      one, so the larger is never below 0.50.
//   probGap            live everywhere. For a two-way sport it is the only
//                      condition that can fire, and it rejects a match the
//                      model calls within five points of even.
//
// So: a coin-flip filter for the NHL and NBA, and coin-flip plus
// no-clear-favourite for football. Leaving the redundant conditions is fine;
// believing all four do something is not.
function vetoCandidate({ sport, hasCoreData, hasCompleteMarket, homeWinProb, drawProb = 0, awayWinProb }) {
  const minTopOutcomeProb = sport === 'Football' ? 0.42 : 0.50;
  let topOutcomeProb, probGap;
  if (sport === 'Football') {
    const probs = [homeWinProb, drawProb, awayWinProb].sort((a, b) => b - a);
    topOutcomeProb = probs[0];
    probGap = probs[0] - probs[1];
  } else {
    topOutcomeProb = Math.max(homeWinProb, awayWinProb);
    probGap = Math.abs(homeWinProb - awayWinProb);
  }
  if (!hasCoreData)       return true;
  if (!hasCompleteMarket) return true;
  if (topOutcomeProb < minTopOutcomeProb) return true;
  if (probGap < 0.05)    return true;  // reduced from 0.10 — don't block close games
  return false;
}

// ═══════════════════════════════════════════════════════════════
// FOOTBALL MODEL (Dixon-Coles Poisson)
// ═══════════════════════════════════════════════════════════════

async function analyseFootballFixture(event, sport) {
  try {
    const market = extractMarketData(event);
    if (!market || market.bookCount < 1) return null;

    // Look up team stats from morning fetch cache
    const findStats = (teamName) => {
      const key = `${teamName}_${sport.tournamentId}`;
      if (teamStatsCache[key]) return teamStatsCache[key];
      // fuzzy match
      for (const [k, v] of Object.entries(teamStatsCache)) {
        if (k.endsWith(`_${sport.tournamentId}`) && nameMatch(k.split(`_${sport.tournamentId}`)[0], teamName)) return v;
      }
      return null;
    };

    const hStats = findStats(event.home_team);
    const aStats = findStats(event.away_team);
    const hasFullStats = !!(hStats && aStats);
    const leagueAvg = getLeagueAvg(sport.tournamentId);

    let lH, lA;
    if (hasFullStats) {
      const hAtt = hStats.homeGames > 0 ? (hStats.homeScored / hStats.homeGames) / leagueAvg.homeGoals : 1;
      const hDef = hStats.homeGames > 0 ? (hStats.homeConceded / hStats.homeGames) / leagueAvg.awayGoals : 1;
      const aAtt = aStats.awayGames > 0 ? (aStats.awayScored / aStats.awayGames) / leagueAvg.awayGoals : 1;
      const aDef = aStats.awayGames > 0 ? (aStats.awayConceded / aStats.awayGames) / leagueAvg.homeGoals : 1;
      lH = clampFinite(hAtt * aDef * leagueAvg.homeGoals, 0.3, 4.0);
      lA = clampFinite(aAtt * hDef * leagueAvg.awayGoals, 0.3, 4.0);
      if (lH === null || lA === null) {
        console.warn(`⚠️ ${event.home_team} vs ${event.away_team}: team stats gave a `
          + `non-finite expected goals — skipping rather than modelling from NaN`);
        return null;
      }
    } else {
      // No stats — skip, don't fall back to market consensus for football
      return null;
    }

    // Apply match context modifiers (form, rest, injuries, lineups)
    // (the pre-modifier matrix used to be built here and never read — an
    //  81-cell Poisson grid per fixture per cycle, discarded immediately)
    const ctx      = matchContextCache[event.id] || null;
    const homeMod  = getContextModifiers('home', ctx, true,  leagueAvg.homeGoals, REST_BANDS_FOOTBALL, SQUAD_SIZE_FOOTBALL);
    const awayMod  = getContextModifiers('away', ctx, false, leagueAvg.awayGoals, REST_BANDS_FOOTBALL, SQUAD_SIZE_FOOTBALL);

    // Apply multipliers to expected goals
    let lHmod = clampFinite(lH * homeMod.attackMult * awayMod.defenceMult, 0.3, 4.0);
    let lAmod = clampFinite(lA * awayMod.attackMult * homeMod.defenceMult, 0.3, 4.0);

    // H2H adjustment — if one team dominates historically, nudge lambda slightly
    if (ctx?.h2h && ctx.h2h.total >= 5) {
      const h2hHomeRate = ctx.h2h.homeWins / ctx.h2h.total;
      const h2hAwayRate = ctx.h2h.awayWins / ctx.h2h.total;
      const h2hAdj = (h2hHomeRate - h2hAwayRate) * 0.08; // max ±8% nudge
      lHmod = clampFinite(lHmod * (1 + h2hAdj), 0.3, 4.0);
      lAmod = clampFinite(lAmod * (1 - h2hAdj), 0.3, 4.0);
    }
    // One check covering the modifiers and the H2H nudge: any of them can be
    // NaN if the context data is malformed, and the clamps no longer hide it.
    if (lHmod === null || lAmod === null) {
      console.warn(`⚠️ ${event.home_team} vs ${event.away_team}: match context gave a `
        + `non-finite expected goals — skipping`);
      return null;
    }

    const dataQuality = Math.min(homeMod.dataQuality, awayMod.dataQuality);
    const modMatrix = buildScoreMatrix(lHmod, lAmod);
    const { homeWin, draw, awayWin, over25 } = calcOutcomes(modMatrix);
    // No over-2.5 candidate is built for football. calcOutcomes still returns
    // the probability and the market still carries the price, so finishing that
    // market is a matter of adding a candidate here — but publishing a new
    // market changes what subscribers are advised to bet, so it is left as a
    // decision rather than switched on. The unused locals that used to sit here
    // read as though the market were live.

    const contextNote = `Form H:${homeMod.notes} | A:${awayMod.notes}`;

    const candidates = [];

    // Home win
    if (market.homeOdds >= INSIGHT_ODDS_MIN && market.homeOdds <= ODDS_ELITE_MAX && market.trueHome > 0) {
      const edge = calcEdge(homeWin, market.trueHome);
      const kelly = kellyStake(homeWin, market.homeOdds);
      const games = Math.min(hStats?.homeGames || 0, aStats?.awayGames || 0);
      const conf  = confidenceFromSignals({ modelProb: homeWin, dataQualityTier: dataQuality, sport: 'Football', gamesPlayed: games });
      const qs    = scoreCandidate({ edgePct: edge, modelProb: homeWin, dataQualityTier: dataQuality });
      const c = { market: 'home', edge, modelProb: homeWin, trueImplied: market.trueHome, dataQualityTier: dataQuality,
        fairPrice: fairOdds(homeWin), bookOdds: market.homeOdds, bookmaker: market.homeBook,
        stake: kelly, conf, qualityScore: qs, selection: `${event.home_team} Win` };
      if (conf >= MIN_CONFIDENCE && falseEdgeCheck(c, market)) candidates.push(c);
    }

    // Away win
    if (market.awayOdds >= INSIGHT_ODDS_MIN && market.awayOdds <= ODDS_ELITE_MAX && market.trueAway > 0) {
      const edge = calcEdge(awayWin, market.trueAway);
      const kelly = kellyStake(awayWin, market.awayOdds);
      const games = Math.min(aStats?.awayGames || 0, hStats?.homeGames || 0);
      const conf  = confidenceFromSignals({ modelProb: awayWin, dataQualityTier: dataQuality, sport: 'Football', gamesPlayed: games });
      const qs    = scoreCandidate({ edgePct: edge, modelProb: awayWin, dataQualityTier: dataQuality });
      const c = { market: 'away', edge, modelProb: awayWin, trueImplied: market.trueAway, dataQualityTier: dataQuality,
        fairPrice: fairOdds(awayWin), bookOdds: market.awayOdds, bookmaker: market.awayBook,
        stake: kelly, conf, qualityScore: qs, selection: `${event.away_team} Win` };
      if (conf >= MIN_CONFIDENCE && falseEdgeCheck(c, market)) candidates.push(c);
    }

    // Draw — only tip if model has meaningful draw probability
    // UNREACHABLE as things stand, and worth saying so rather than leaving it
    // to be rediscovered. confidenceFromSignals sets conf = round(modelProb *
    // 100), and the gate below is conf >= MIN_CONFIDENCE, which is 52. A draw
    // in football sits around 0.25 to 0.30 — 0.2825 for an average fixture on
    // the corrected DC_RHO — so conf comes out in the twenties and the
    // candidate is always dropped. It has never published a tip.
    //
    // The cause is that confidence and win probability are the same number
    // here. That is defensible for a moneyline, where "how likely is this to
    // win" and "how much do we like it" nearly coincide; it is not for any
    // market whose winning probability is structurally below a half. The same
    // arithmetic caps the whole engine at about 3.15 in price, whatever
    // ODDS_ELITE_MAX says — see DEPLOY.md.
    //
    // Left in place rather than deleted: publishing draws is a decision about
    // what subscribers are advised to bet, not a tidy-up, and the candidate is
    // complete and correct apart from the gate it cannot pass.
    if (market.drawOdds >= INSIGHT_ODDS_MIN && draw > 0.20 && market.trueDraw > 0) {
      const edge = calcEdge(draw, market.trueDraw);
      const kelly = kellyStake(draw, market.drawOdds);
      const games = Math.min(hStats?.homeGames || 0, aStats?.awayGames || 0);
      const conf  = confidenceFromSignals({ modelProb: draw, dataQualityTier: dataQuality, sport: 'Football', gamesPlayed: games });
      const qs    = scoreCandidate({ edgePct: edge, modelProb: draw, dataQualityTier: dataQuality });
      const c = { market: 'draw', edge, modelProb: draw, trueImplied: market.trueDraw, dataQualityTier: dataQuality,
        fairPrice: fairOdds(draw), bookOdds: market.drawOdds, bookmaker: market.drawBook,
        stake: kelly, conf, qualityScore: qs, selection: 'Draw' };
      if (conf >= MIN_CONFIDENCE && falseEdgeCheck(c, market)) candidates.push(c);
    }

    // Over 2.5 — priced against the de-vigged Over probability, like every
    // other market here. Requires over25True, so a line with no Under to
    // de-vig against is skipped rather than priced off a vigged number.
    if (PUBLISH_FOOTBALL_OVERS
        && market.over25Odds >= INSIGHT_ODDS_MIN && market.over25Odds <= ODDS_ELITE_MAX
        && market.over25True > 0) {
      const edge  = calcEdge(over25, market.over25True);
      const kelly = kellyStake(over25, market.over25Odds);
      const games = Math.min(hStats?.homeGames || 0, aStats?.awayGames || 0);
      const conf  = confidenceFromSignals({ modelProb: over25, dataQualityTier: dataQuality, sport: 'Football', gamesPlayed: games });
      const qs    = scoreCandidate({ edgePct: edge, modelProb: over25, dataQualityTier: dataQuality });
      const c = { market: 'over25', edge, modelProb: over25, trueImplied: market.over25True, dataQualityTier: dataQuality,
        fairPrice: fairOdds(over25), bookOdds: market.over25Odds, bookmaker: market.over25Book,
        stake: kelly, conf, qualityScore: qs, selection: 'Over 2.5' };
      if (conf >= MIN_CONFIDENCE && falseEdgeCheck(c, market)) candidates.push(c);
    }

    if (!candidates.length) return null;

    const vetoed = candidates.filter(c => !vetoCandidate({
      sport: 'Football', hasCoreData: hasFullStats,
      hasCompleteMarket: !!market,
      homeWinProb: homeWin, drawProb: draw, awayWinProb: awayWin,
    }));

    const pick = pickBestCandidate(vetoed);
    if (!pick) return null;

    return {
      tip_ref:       generateTipRef('Football'),
      sport:         'Football',
      league:        sport.league,
      home_team:     event.home_team,
      away_team:     event.away_team,
      event_time:    event.commence_time,
      event_id:      event.id,
      selection:     pick.selection,
      market:        pick.market === 'over25' ? 'totals' : 'h2h',
      odds:          parseFloat(pick.bookOdds.toFixed(2)),
      stake:         pick.stake,
      confidence:    pick.conf,
      tier:          'pro',
      status:        'pending',
      bookmaker:     pick.bookmaker || 'Sofascore',
      model_edge:    parseFloat(pick.edge.toFixed(2)),
      model_prob:    parseFloat((pick.modelProb * 100).toFixed(1)),
      implied_prob:  parseFloat((pick.trueImplied * 100).toFixed(1)),
      fair_odds:     pick.fairPrice,
      quality_score: parseFloat((pick.qualityScore || 0).toFixed(3)),
      book_count:    market.bookCount,
      notes:         `Model goals: ${lH.toFixed(2)}-${lA.toFixed(2)} (adj: ${lHmod.toFixed(2)}-${lAmod.toFixed(2)}) | Model: ${(pick.modelProb*100).toFixed(1)}% | Fair: ${pick.fairPrice} | Edge: ${pick.edge >= 0 ? '+' : ''}${pick.edge.toFixed(1)}% | ${contextNote}`,
    };
  } catch(e) {
    console.error(`Football model error [${event.home_team} vs ${event.away_team}]:`, e.message);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════
// NHL MODEL (Poisson)
// ═══════════════════════════════════════════════════════════════

async function analyseNHLFixture(event, sport) {
  try {
    const [homeStats, awayStats] = await Promise.all([
      fetchNHLTeamStats(event.home_team),
      fetchNHLTeamStats(event.away_team),
    ]);
    if (!homeStats || !awayStats) return null;

    const homeAttack = homeStats.goalsFor     / NHL_LEAGUE_AVG_GF;
    const homeDef    = homeStats.goalsAgainst / NHL_LEAGUE_AVG_GF;
    const awayAttack = awayStats.goalsFor     / NHL_LEAGUE_AVG_GF;
    const awayDef    = awayStats.goalsAgainst / NHL_LEAGUE_AVG_GF;

    // Apply goalie quality — adjusts expected goals against based on starter SV%
    const homeGoalie = goalieQualityMultiplier(null, event.home_team);
    const awayGoalie = goalieQualityMultiplier(null, event.away_team);

    // homeGoalie faces away shots → multiplier applies to lA (goals scored by away)
    // awayGoalie faces home shots → multiplier applies to lH (goals scored by home)
    let lH = clampFinite(
      homeAttack * awayDef * NHL_LEAGUE_AVG_GF * awayGoalie.multiplier + NHL_HOME_ADVANTAGE,
      0.5, 6.0);
    let lA = clampFinite(
      awayAttack * homeDef * NHL_LEAGUE_AVG_GF * homeGoalie.multiplier,
      0.5, 6.0);
    if (lH === null || lA === null) {
      console.warn(`⚠️ ${event.home_team} vs ${event.away_team}: team or goalie data gave a `
        + `non-finite expected goals — skipping`);
      return null;
    }

    // Data quality tier — lower if goalie data absent (less confident model)
    const hasGoalieData = homeGoalie.label && awayGoalie.label;
    let dataQualityTier = hasGoalieData ? 1.0 : 0.85;

    const goalieNote = hasGoalieData
      ? `H: ${homeGoalie.label} | A: ${awayGoalie.label}`
      : 'No goalie data';

    // Apply match context (form, rest, H2H) to NHL lambda
    const ctx     = matchContextCache[event.id] || null;
    const homeMod = getContextModifiers('home', ctx, true,  NHL_LEAGUE_AVG_GF, REST_BANDS_NHL, SQUAD_SIZE_NHL);
    const awayMod = getContextModifiers('away', ctx, false, NHL_LEAGUE_AVG_GF, REST_BANDS_NHL, SQUAD_SIZE_NHL);

    lH = clampFinite(lH * homeMod.attackMult * awayMod.defenceMult, 0.5, 6.0);
    lA = clampFinite(lA * awayMod.attackMult * homeMod.defenceMult, 0.5, 6.0);

    // H2H adjustment for NHL
    if (ctx?.h2h && ctx.h2h.total >= 5) {
      const h2hAdj = ((ctx.h2h.homeWins - ctx.h2h.awayWins) / ctx.h2h.total) * 0.06;
      lH = clampFinite(lH * (1 + h2hAdj), 0.5, 6.0);
      lA = clampFinite(lA * (1 - h2hAdj), 0.5, 6.0);
    }
    if (lH === null || lA === null) {
      console.warn(`⚠️ ${event.home_team} vs ${event.away_team}: match context gave a `
        + `non-finite expected goals — skipping`);
      return null;
    }

    // Merge context data quality with goalie data quality
    dataQualityTier = Math.min(dataQualityTier, Math.min(homeMod.dataQuality, awayMod.dataQuality));

    const N = NHL_MATRIX_MAX;
    const matrix = [];
    let total = 0;
    for (let h = 0; h <= N; h++) {
      matrix[h] = [];
      for (let a = 0; a <= N; a++) {
        const p = poisson(lH, h) * poisson(lA, a);
        matrix[h][a] = p;
        total += p;
      }
    }
    if (total > 0) {
      for (let h = 0; h <= N; h++)
        for (let a = 0; a <= N; a++)
          matrix[h][a] /= total;
    }

    let homeWin = 0, awayWin = 0, draw = 0, over55 = 0;
    for (let h = 0; h <= N; h++) {
      for (let a = 0; a <= N; a++) {
        const p = matrix[h][a];
        if (h > a) homeWin += p;
        else if (h < a) awayWin += p;
        else draw += p;
        if (h + a > 5.5) over55 += p;
      }
    }

    const homeWinML = homeWin + draw * 0.5;
    const awayWinML = awayWin + draw * 0.5;

    const market = extractMarketData(event);
    if (!market) return null;

    const candidates = [];

    if (market.homeOdds >= INSIGHT_ODDS_MIN && market.homeOdds <= ODDS_ELITE_MAX && market.trueHome > 0) {
      const edge = calcEdge(homeWinML, market.trueHome);
      const conf  = confidenceFromSignals({ modelProb: homeWinML, dataQualityTier, sport: 'Ice Hockey', gamesPlayed: homeStats.gamesPlayed });
      const stake = kellyStake(homeWinML, market.homeOdds);
      const qs    = scoreCandidate({ edgePct: edge, modelProb: homeWinML, dataQualityTier });
      const c = { selection: `${event.home_team} Win`, market: 'home', edge,
        modelProb: homeWinML, trueImplied: market.trueHome, dataQualityTier,
        fairPrice: fairOdds(homeWinML), bookOdds: market.homeOdds, bookmaker: market.homeBook,
        stake, conf, qualityScore: qs,
        notes: `GF/GA: ${lH.toFixed(2)}/${lA.toFixed(2)} | ${goalieNote} | Model: ${(homeWinML*100).toFixed(1)}% | Fair: ${fairOdds(homeWinML)} | Edge: ${edge >= 0 ? '+' : ''}${edge.toFixed(1)}%` };
      if (conf >= NHL_MIN_CONFIDENCE && falseEdgeCheck(c, market)) candidates.push(c);
    }

    if (market.awayOdds >= INSIGHT_ODDS_MIN && market.awayOdds <= ODDS_ELITE_MAX && market.trueAway > 0) {
      const edge = calcEdge(awayWinML, market.trueAway);
      const conf  = confidenceFromSignals({ modelProb: awayWinML, dataQualityTier, sport: 'Ice Hockey', gamesPlayed: awayStats.gamesPlayed });
      const stake = kellyStake(awayWinML, market.awayOdds);
      const qs    = scoreCandidate({ edgePct: edge, modelProb: awayWinML, dataQualityTier });
      const c = { selection: `${event.away_team} Win`, market: 'away', edge,
        modelProb: awayWinML, trueImplied: market.trueAway, dataQualityTier,
        fairPrice: fairOdds(awayWinML), bookOdds: market.awayOdds, bookmaker: market.awayBook,
        stake, conf, qualityScore: qs,
        notes: `GF/GA: ${lH.toFixed(2)}/${lA.toFixed(2)} | ${goalieNote} | Model: ${(awayWinML*100).toFixed(1)}% | Fair: ${fairOdds(awayWinML)} | Edge: ${edge >= 0 ? '+' : ''}${edge.toFixed(1)}%` };
      if (conf >= NHL_MIN_CONFIDENCE && falseEdgeCheck(c, market)) candidates.push(c);
    }

    // over25True is the de-vigged P(Over) from the same book as the price.
    // It was 1 / over25Odds — the raw implied probability, margin included —
    // while every other market here was de-vigged. That understated the edge on
    // this market and made the published "+x% edge" incomparable between an
    // Over 5.5 tip and a moneyline tip, even though both are shown as one
    // number in the email and on the site. A line with no Under price cannot be
    // de-vigged, and is skipped rather than priced off a vigged number.
    if (market.over25Odds >= INSIGHT_ODDS_MIN && market.over25Odds <= ODDS_ELITE_MAX
        && market.over25True > 0) {
      const over55IP = market.over25True;
      const edge = calcEdge(over55, over55IP);
      const conf  = confidenceFromSignals({ modelProb: over55, dataQualityTier, sport: 'Ice Hockey', gamesPlayed: Math.min(homeStats.gamesPlayed, awayStats.gamesPlayed) });
      const stake = kellyStake(over55, market.over25Odds);
      const qs    = scoreCandidate({ edgePct: edge, modelProb: over55, dataQualityTier });
      const c = { selection: 'Over 5.5', market: 'over55', edge,
        modelProb: over55, trueImplied: over55IP, dataQualityTier,
        fairPrice: fairOdds(over55), bookOdds: market.over25Odds, bookmaker: market.over25Book,
        stake, conf, qualityScore: qs,
        notes: `GF/GA: ${lH.toFixed(2)}/${lA.toFixed(2)} | ${goalieNote} | Model: ${(over55*100).toFixed(1)}% over 5.5 | Edge: ${edge >= 0 ? '+' : ''}${edge.toFixed(1)}%` };
      if (conf >= NHL_MIN_CONFIDENCE && falseEdgeCheck(c, market)) candidates.push(c);
    }

    if (!candidates.length) return null;

    const vetoed = candidates.filter(c => !vetoCandidate({
      sport: 'Ice Hockey', hasCoreData: true, hasCompleteMarket: !!market,
      homeWinProb: homeWinML, awayWinProb: awayWinML,
    }));
    const pick = pickBestCandidate(vetoed);
    if (!pick) return null;

    return {
      tip_ref:       generateTipRef('Ice Hockey'),
      sport:         'Ice Hockey',
      league:        sport.league,
      home_team:     event.home_team,
      away_team:     event.away_team,
      event_time:    event.commence_time,
      event_id:      event.id,
      selection:     pick.selection,
      market:        pick.market.includes('ver') ? 'totals' : 'h2h',
      odds:          parseFloat(pick.bookOdds.toFixed(2)),
      stake:         pick.stake,
      confidence:    pick.conf,
      tier:          'pro',
      status:        'pending',
      bookmaker:     pick.bookmaker,
      model_edge:    parseFloat(pick.edge.toFixed(2)),
      model_prob:    parseFloat((pick.modelProb * 100).toFixed(1)),
      implied_prob:  parseFloat((pick.trueImplied * 100).toFixed(1)),
      fair_odds:     pick.fairPrice,
      quality_score: parseFloat((pick.qualityScore || 0).toFixed(3)),
      book_count:    market.bookCount,
      notes:         pick.notes,
    };
  } catch(e) {
    console.error(`NHL model error [${event.home_team} vs ${event.away_team}]:`, e.message);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════
// NBA MODEL
// ═══════════════════════════════════════════════════════════════

async function analyseNBAFixture(event, sport) {
  try {
    const [homeStats, awayStats] = await Promise.all([
      fetchNBATeamStats(event.home_team),
      fetchNBATeamStats(event.away_team),
    ]);
    if (!homeStats || !awayStats) return null;

    const homeOff = homeStats.ptsFor     / NBA_LEAGUE_AVG_PTS;
    const homeDef = homeStats.ptsAgainst / NBA_LEAGUE_AVG_PTS;
    const awayOff = awayStats.ptsFor     / NBA_LEAGUE_AVG_PTS;
    const awayDef = awayStats.ptsAgainst / NBA_LEAGUE_AVG_PTS;

    const homeExpected = homeOff * awayDef * NBA_LEAGUE_AVG_PTS + NBA_HOME_ADVANTAGE;
    const awayExpected = awayOff * homeDef * NBA_LEAGUE_AVG_PTS;
    const expectedMargin = homeExpected - awayExpected;
    const homeWinP = winProbFromMargin(expectedMargin);
    const awayWinP = 1 - homeWinP;

    const market = extractMarketData(event);
    if (!market) return null;

    const candidates = [];

    if (market.homeOdds >= INSIGHT_ODDS_MIN && market.homeOdds <= ODDS_ELITE_MAX && market.trueHome > 0) {
      const edge = calcEdge(homeWinP, market.trueHome);
      const conf  = confidenceFromSignals({ modelProb: homeWinP, dataQualityTier: 1.0, sport: 'Basketball', gamesPlayed: homeStats.gamesPlayed });
      const fec   = falseEdgeCheck({ edge, modelProb: homeWinP, trueImplied: market.trueHome }, market);
      const stake = kellyStake(homeWinP, market.homeOdds);
      const qs    = scoreCandidate({ edgePct: edge, modelProb: homeWinP, dataQualityTier: 1.0 });
      const c = { selection: `${event.home_team} Win`, market: 'home', edge,
        modelProb: homeWinP, trueImplied: market.trueHome, dataQualityTier: 1.0,
        fairPrice: fairOdds(homeWinP), bookOdds: market.homeOdds, bookmaker: market.homeBook,
        stake, conf, qualityScore: qs,
        notes: `Expected: ${homeExpected.toFixed(1)}-${awayExpected.toFixed(1)} | Model: ${(homeWinP*100).toFixed(1)}% | Fair: ${fairOdds(homeWinP)} | Edge: ${edge >= 0 ? '+' : ''}${edge.toFixed(1)}%` };
      if (conf >= NBA_MIN_CONFIDENCE && fec) candidates.push(c);
    }

    if (market.awayOdds >= INSIGHT_ODDS_MIN && market.awayOdds <= ODDS_ELITE_MAX && market.trueAway > 0) {
      const edge = calcEdge(awayWinP, market.trueAway);
      const conf  = confidenceFromSignals({ modelProb: awayWinP, dataQualityTier: 1.0, sport: 'Basketball', gamesPlayed: awayStats.gamesPlayed });
      const fec   = falseEdgeCheck({ edge, modelProb: awayWinP, trueImplied: market.trueAway }, market);
      const stake = kellyStake(awayWinP, market.awayOdds);
      const qs    = scoreCandidate({ edgePct: edge, modelProb: awayWinP, dataQualityTier: 1.0 });
      const c = { selection: `${event.away_team} Win`, market: 'away', edge,
        modelProb: awayWinP, trueImplied: market.trueAway, dataQualityTier: 1.0,
        fairPrice: fairOdds(awayWinP), bookOdds: market.awayOdds, bookmaker: market.awayBook,
        stake, conf, qualityScore: qs,
        notes: `Expected: ${homeExpected.toFixed(1)}-${awayExpected.toFixed(1)} | Model: ${(awayWinP*100).toFixed(1)}% | Fair: ${fairOdds(awayWinP)} | Edge: ${edge >= 0 ? '+' : ''}${edge.toFixed(1)}%` };
      if (conf >= NBA_MIN_CONFIDENCE && fec) candidates.push(c);
    }

    if (!candidates.length) return null;

    // Football and the NHL both run their candidates through vetoCandidate and
    // then pickBestCandidate. This path did neither, with nothing saying why,
    // and had its own selection instead:
    //
    //     a.edge * 0.7 + (a.qualityScore || 0) * 0.3
    //
    // That shape reads as a 70/30 blend and is not one. edge is in percentage
    // points, running about 8 to 20; qualityScore is a fraction between 0.15
    // and 1. So the quality term contributes between 1% and 2% of the total,
    // not 30% — two orders of magnitude adrift because the two quantities were
    // never put on the same scale. pickBestCandidate normalises the edge before
    // weighting it, which is the difference.
    //
    // That fault is latent rather than live: home and away probabilities sum to
    // one and both are gated on conf >= 52, so at most one NBA candidate ever
    // survives and the ranking has never had two things to compare. It would
    // become live the moment a totals market is added here, as the NHL has.
    //
    // The veto is live now. Of two-way candidates clearing the confidence gate
    // it rejects 2.1% — the band the model rates between 51.5% and 52.5%, which
    // is a coin flip it happens to call marginally one way.
    const vetoed = candidates.filter(c => !vetoCandidate({
      sport: 'Basketball', hasCoreData: true, hasCompleteMarket: !!market,
      homeWinProb: homeWinP, awayWinProb: awayWinP,
    }));
    const pick = pickBestCandidate(vetoed);
    if (!pick) return null;

    return {
      tip_ref:       generateTipRef('Basketball'),
      sport:         'Basketball',
      league:        sport.league,
      home_team:     event.home_team,
      away_team:     event.away_team,
      event_time:    event.commence_time,
      event_id:      event.id,
      selection:     pick.selection,
      market:        'h2h',
      odds:          parseFloat(pick.bookOdds.toFixed(2)),
      stake:         pick.stake,
      confidence:    pick.conf,
      tier:          'pro',
      status:        'pending',
      bookmaker:     pick.bookmaker,
      model_edge:    parseFloat(pick.edge.toFixed(2)),
      model_prob:    parseFloat((pick.modelProb * 100).toFixed(1)),
      implied_prob:  parseFloat((pick.trueImplied * 100).toFixed(1)),
      fair_odds:     pick.fairPrice,
      quality_score: parseFloat((pick.qualityScore || 0).toFixed(3)),
      book_count:    market.bookCount,
      notes:         pick.notes,
    };
  } catch(e) {
    console.error(`NBA model error [${event.home_team} vs ${event.away_team}]:`, e.message);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════
// STRICT RULES FILTER
// ═══════════════════════════════════════════════════════════════

function applyStrictRules(tip, existingBestOdds = null) {
  const ALLOWED_SPORTS = ['Ice Hockey', 'Basketball', 'Football'];
  if (!ALLOWED_SPORTS.includes(tip.sport)) return null;

  const isH2H      = tip.market === 'h2h';
  const isTotals   = tip.market === 'totals';
  const isOver     = isTotals && (tip.selection||'').toLowerCase().startsWith('over');
  const isUnder    = isTotals && (tip.selection||'').toLowerCase().startsWith('under');
  const isFootball = tip.sport === 'Football';

  if (!isH2H && !isTotals)    return null;
  if (isUnder)                 return null;
  // Football totals were rejected outright because none were ever published.
  // With PUBLISH_FOOTBALL_OVERS off nothing reaches here anyway; the gate
  // follows the flag so the two cannot disagree.
  if (isFootball && isTotals && !PUBLISH_FOOTBALL_OVERS) return null;

  const odds = parseFloat(tip.odds || 0);
  if (odds < INSIGHT_ODDS_MIN) return null; // below 1.05 — not worth showing
  if (odds > ODDS_ELITE_MAX)   return null;

  // Line movement check. Shortening odds mean a rising implied probability, so
  // the move is measured there rather than in decimal steps — see the constant.
  if (existingBestOdds && existingBestOdds > 0 && odds > 0) {
    const movePP = (100 / odds) - (100 / existingBestOdds);
    if (movePP >= LINE_MOVE_REJECT_PP) return null;
  }

  // Edge — informational only, used for grade display
  const edge = (tip.model_edge != null) ? parseFloat(tip.model_edge) : 0;

  // Grade for display
  let grade;
  if (isH2H) {
    grade = edge >= ELITE_H2H_EDGE ? 'A+' : edge >= 5 ? 'A' : edge >= 0 ? 'B' : 'C';
  } else {
    grade = edge >= ELITE_OVERS_EDGE ? 'A+' : edge >= 8 ? 'A' : edge >= 0 ? 'B' : 'C';
  }
  const bookCount = tip.book_count || 1;
  if (grade === 'A+' && bookCount < 2) grade = 'A';

  // The grade goes nowhere.
  //
  // Its only destination is the notes column, and nothing reads that column:
  // not buildProEmail, buildFreeEmail or buildSaturdayEmail, none of the six
  // Vercel handlers, and no page on the site. So ELITE_H2H_EDGE,
  // ELITE_OVERS_EDGE and the book-count downgrade above are a complete grading
  // system whose entire output is a string nobody can see.
  //
  // Left computing rather than removed, because notes also carries the model
  // diagnostics — expected goals, the fair price, the edge, the form context —
  // and those are the first thing anyone would want when a tip looks wrong.
  // Surfacing them somewhere is a small piece of work; deleting them is not
  // recoverable. Recorded in DEPLOY.md as a decision.

  // ── Short Price Watch — odds below BET_ODDS_MIN ──────────
  // Published as informational only — no stake, marked as insight
  if (odds < BET_ODDS_MIN) {
    return {
      ...tip,
      stake:      0,
      tier:       'insight',
      is_short_price: true,
      notes: (tip.notes || '') + ` | Short Price Watch | Grade: ${grade} | Edge: ${edge >= 0 ? '+' : ''}${edge.toFixed(1)}%`,
    };
  }

  // ── Normal bet tip ────────────────────────────────────────
  // The stake is the quarter-Kelly value already computed during analysis and
  // carried on the tip. This used to overwrite it with a three-way bucket on
  // confidence alone:
  //
  //     conf >= 78 -> 2.0 | conf >= 72 -> 1.5 | else 1.0
  //
  // which discarded Kelly entirely. Confidence is the model win probability, so
  // that sized every bet on probability alone and ignored the price — the two
  // inputs Kelly exists to combine. A 1.30 shot with a thin edge and a 4.00
  // shot with a large one drew the same stake whenever their probabilities
  // matched. It also made 0.5u, 2.5u and 3u unreachable, so the KELLY_TIERS
  // table and the rescaling work documented against it had no effect on
  // anything that shipped.
  //
  // The file header has said "Grades do NOT set the stake: that is
  // quarter-Kelly (see KELLY_TIERS)" and "Stakes: 0.5u-3.0u, quarter-Kelly"
  // throughout. This makes the code do what it already claimed.
  //
  // To go back to flat or confidence-based staking, replace this one line.
  const stake = parseFloat(tip.stake);
  if (!Number.isFinite(stake) || stake <= 0) {
    // kellyStake returns 0 for degenerate input, and a tip that cannot be
    // sized is not a bet. Publish it as insight rather than inventing a stake.
    return {
      ...tip,
      stake:      0,
      tier:       'insight',
      is_short_price: false,
      notes: (tip.notes || '') + ` | Unstakeable (no Kelly size) | Grade: ${grade} | Edge: ${edge >= 0 ? '+' : ''}${edge.toFixed(1)}%`,
    };
  }

  const edgeStr = edge >= 0 ? `+${edge.toFixed(1)}%` : `${edge.toFixed(1)}%`;
  return {
    ...tip,
    stake,
    is_short_price: false,
    notes: (tip.notes || '') + ` | Grade: ${grade} | Edge: ${edgeStr}`,
  };
}

// ═══════════════════════════════════════════════════════════════
// TIP GENERATION — reads from sofascoreCache (no API calls)
// ═══════════════════════════════════════════════════════════════

async function generateTips(events, sport) {
  const tips = [];
  // Why each fixture did not become a tip.
  //
  // The log's answer to "why were there no tips today" was one line reading
  // "-> 0 tips", which says the same thing whether the feed sent no prices,
  // every fixture was days away, the model could not price any of them, or
  // they were priced and rejected on merit. Those want different responses
  // from whoever is reading the log, and this loop is the only place all
  // four are distinguishable.
  //
  // Football is the one that reaches `unpriceable` in bulk: analyseFootballFixture
  // returns null when either side is missing from teamStatsCache, which is the
  // state after a restart if that league's standings fetch came back empty.
  const rejected = { noOdds: 0, outsideWindow: 0, unpriceable: 0, failedRules: 0 };
  for (const event of events) {
    if (!event.bookmakers || !event.bookmakers.length) { rejected.noOdds++; continue; }
    const hours = (new Date(event.commence_time) - new Date()) / 3600000;
    if (hours < 0 || hours > 48) { rejected.outsideWindow++; continue; }

    let tip = null;

    if (sport.name === 'Football') {
      tip = await analyseFootballFixture(event, sport);
    } else if (sport.name === 'Basketball') {
      tip = await analyseNBAFixture(event, sport);
    } else if (sport.name === 'Ice Hockey') {
      tip = await analyseNHLFixture(event, sport);
    }

    if (!tip) { rejected.unpriceable++; continue; }
    const approved = applyStrictRules(tip);
    if (approved) tips.push(approved);
    else rejected.failedRules++;
  }
  if (!tips.length && events.length) {
    console.log(`     ${events.length} fixtures, none published — `
      + `${rejected.noOdds} with no prices, `
      + `${rejected.outsideWindow} outside the 48h window, `
      + `${rejected.unpriceable} the model could not price, `
      + `${rejected.failedRules} rejected on merit`);
  }
  return tips;
}

// ═══════════════════════════════════════════════════════════════
// SAVE TIPS
// ═══════════════════════════════════════════════════════════════

async function saveTips(tips) {
  if (!tips.length) return;
  let saved = 0, updated = 0, skipped = 0;

  for (const tip of tips) {
    try {
      // Deduplicate: try event_id first (exact), then fuzzy name match
      let existing = null;
      // A read that failed is not proof this tip is new. Both probes below
      // returned null on failure, which is the same value as "no match", and
      // the insert below acts on it — so one bad response put a second copy of
      // a tip already on the card. The cycle runs every 15 minutes; skipping is
      // free.
      if (tip.event_id) {
        const { data: byId, error: byIdErr } = await supabase.from('tips').select('id, tip_ref, confidence, odds, best_odds, bookmaker, status')
          .eq('event_id', tip.event_id).eq('selection', tip.selection)
          .in('status', ['pending', 'won', 'lost', 'void']).maybeSingle();
        if (byIdErr) {
          console.error(`🚨 [${tip.tip_ref}] could not check for an existing tip `
            + `(${byIdErr.message}) — not saving it this cycle rather than risking a duplicate`);
          skipped++; continue;
        }
        existing = byId;
      }
      if (!existing) {
        // Fuzzy name match — catches "Inter" vs "Inter Milan", "Marseille" vs "Olympique de Marseille"
        const date = new Date(tip.event_time).toISOString().split('T')[0];
        const { data: candidates, error: candErr } = await supabase.from('tips').select('id, tip_ref, confidence, odds, best_odds, bookmaker, status, home_team, away_team')
          .gte('event_time', `${date}T00:00:00Z`).lte('event_time', `${date}T23:59:59Z`)
          .eq('selection', tip.selection)
          .in('status', ['pending', 'won', 'lost', 'void']);
        if (candErr) {
          console.error(`🚨 [${tip.tip_ref}] could not check for an existing tip `
            + `(${candErr.message}) — not saving it this cycle rather than risking a duplicate`);
          skipped++; continue;
        }
        existing = (candidates || []).find(c =>
          nameMatch(c.home_team, tip.home_team) && nameMatch(c.away_team, tip.away_team)
        ) || null;
      }

      if (existing) {
        if (existing.status !== 'pending') { skipped++; continue; }

        // FIX 5: Re-apply strict rules with existing best_odds for line movement check
        const reapproved = applyStrictRules(tip, existing.best_odds || existing.odds || null);
        if (!reapproved) {
          console.log(`📉 Line move reject: [${existing.tip_ref}] ${tip.home_team} vs ${tip.away_team}`);
          skipped++; continue;
        }

        const oddsDiff     = Math.abs(tip.odds - existing.odds);
        const confDiff     = Math.abs(tip.confidence - existing.confidence);
        const bookChanged  = tip.bookmaker !== existing.bookmaker;
        const currentBest  = parseFloat(existing.best_odds || existing.odds || 0);
        const newBest      = Math.max(currentBest, tip.odds);
        const bestImproved = newBest > currentBest + 0.001;

        if (oddsDiff > 0.01 || confDiff > 1 || bookChanged || bestImproved) {
          const updateKey = existing.tip_ref ? 'tip_ref' : 'id';
          const updateVal = existing.tip_ref || existing.id;
          // stake is deliberately NOT here, for the same reason advised_odds is
          // not: it is half the instruction. The email says "2.00, stake 1.5u",
          // and the price is frozen at publication — so resizing the stake on a
          // later refresh settles a bet nobody was advised to place. An
          // improving price used to do exactly that: a tip emailed at 1.5u
          // could settle at 2.5u against the advised 2.00, overstating the win
          // by two thirds.
          //
          // odds and best_odds still track the live and peak prices, which are
          // display and line-movement inputs rather than advice.
          const { error: updErr } = await supabase.from('tips').update({
            odds: tip.odds, best_odds: newBest, bookmaker: tip.bookmaker,
            confidence: tip.confidence,
            selection: tip.selection, market: tip.market, notes: tip.notes,
            model_edge: tip.model_edge, model_prob: tip.model_prob,
            implied_prob: tip.implied_prob, fair_odds: tip.fair_odds,
            quality_score: tip.quality_score, book_count: tip.book_count,
            event_id: tip.event_id,
            ...(existing.tip_ref ? {} : { tip_ref: tip.tip_ref }),
          }).eq(updateKey, updateVal);
          // Counted only when it happened, the way `saved` is on the insert
          // path below. This incremented unconditionally, so the summary line
          // reported reprices that had failed — and a persistent write failure
          // read as a healthy run with the old prices quietly still live.
          if (updErr) console.error(`Tip update error [${tip.tip_ref}]:`, updErr.message);
          else updated++;
        } else { skipped++; }
        continue;
      }

      // advised_odds is written once at publication and NEVER updated.
      // It is the price we told subscribers to take, and the only honest
      // basis for settlement. `odds` tracks the live price; `best_odds`
      // tracks the peak seen — neither is what we actually advised.
      // Spreads the whole tip object, so every key an analyser returns has to
      // exist as a column or the insert fails outright. Worth knowing before
      // adding a field to one of them.
      //
      // Three of the columns written here are never read back, by anything:
      //
      //   notes           the model diagnostics and the grade — see
      //                   applyStrictRules. No reader anywhere.
      //   fair_odds       written on insert and on every refresh, read nowhere.
      //                   The site shows the edge instead, which is derived
      //                   from the same pair of numbers.
      //   is_short_price  set true or false in three places and read in none.
      //
      // best_odds looks like a fourth and is not: nothing displays it, but
      // applyStrictRules reads it back as the peak price for the line-move
      // check, so it is internal rather than dead.
      const { error } = await supabase.from('tips').insert({ ...tip, best_odds: tip.odds, advised_odds: tip.odds });
      if (error) { if (error.code === '23505') skipped++; else console.error('Insert error:', error.message); }
      else saved++;
    } catch(e) { console.error('saveTips error:', e.message); }
  }
  console.log(`✅ Tips: ${saved} new, ${updated} updated, ${skipped} unchanged`);
}

// ═══════════════════════════════════════════════════════════════
// RESULT SETTLER — uses Sofascore match results
// ═══════════════════════════════════════════════════════════════

// Final scores, keyed by event id. Only ever holds results confirmed finished,
// so an entry is safe to keep — but it is still capped, because the settler
// touches every past event and the process is long-lived. See cacheSet.
const scoreCache = {};

// Descriptions the feed uses for a match that ended without being played out.
// Kept next to the check that uses it so the two cannot drift apart.
const NOT_PLAYED_OUT = /abandon|cancel|postpon|walkover|award|retire|interrupt|suspend|coverage lost/i;

async function fetchSofascoreResult(eventId) {
  if (scoreCache[eventId]) return scoreCache[eventId];

  // Primary: matches/detail
  const data = await sofascoreFetch(`/matches/detail`, { id: eventId });
  if (!data?.event) {
    // No status means no way to know whether the match has finished. The graph
    // fallback below used to run in this case and cached its last data point as
    // the final score with no finished check at all — so a transient failure on
    // this call, for a match still in play, pinned a half-time score as final.
    // The settler runs hourly against every tip whose kick-off has passed, so
    // it queries in-progress matches as a matter of course, and scoreCache is
    // never invalidated: the wrong score would have been settled against and
    // then kept for the life of the process.
    console.log(`  ⚠️ No detail for event ${eventId} — will retry next cycle`);
    return null;
  }

  const e = data.event;
  const status = e.status || {};
  const desc   = String(status.description || '');
  const isFinished = status.type === 'finished' || desc === 'Ended' || status.code === 100;
  if (!isFinished) {
    console.log(`  ⏳ Not finished yet [${eventId}]: ${desc} (${status.type})`);
    return null;
  }

  // "Finished" is not the same as "played out".
  //
  // The feed marks several outcomes as finished that are not a match played to
  // its end: abandoned, walkover, awarded, retired, coverage lost. Each of
  // those carries whatever score existed when play stopped, and a partial score
  // is the worst possible input to a settler because it looks exactly like a
  // real one. A hockey game abandoned at 1-0 in the first period would have
  // settled every moneyline on it and graded an Over 5.5 as a loss.
  //
  // Matched on the description rather than the numeric code, because the codes
  // are undocumented and guessing them wrong is worse than not checking. This
  // is deliberately a reject-list on top of an already-required 'finished': an
  // unfamiliar description still has to pass the check above.
  //
  // Returns null, which leaves the tip pending and re-checked each run. That is
  // the same choice the ungradeable-selection path makes: a bet nobody can
  // grade needs a person, and inventing a rule for it is how a ledger stops
  // meaning anything.
  if (NOT_PLAYED_OUT.test(desc)) {
    console.error(`🚨 [${eventId}] status "${desc}" — the match did not play out. `
      + `Not settling on a partial score; needs manual review.`);
    return null;
  }

  const result = {
    homeScore: e.homeScore?.current ?? e.homeScore?.normaltime ?? null,
    awayScore: e.awayScore?.current ?? e.awayScore?.normaltime ?? null,
    finished:  true,
  };
  if (result.homeScore !== null && result.awayScore !== null) {
    cacheSet(scoreCache, eventId, result);
    return result;
  }

  // Finished, but the detail payload carried no score. Only now is the graph
  // worth reading — we have confirmed the match is over, so its last data point
  // is a full-time score rather than a snapshot of one in progress.
  await new Promise(r => setTimeout(r, 300));
  const graphData = await sofascoreFetch(`/matches/get-graph`, { matchId: eventId });
  if (graphData) {
    const points = graphData.graphPoints || [];
    if (points.length > 0) {
      const last = points[points.length - 1];
      if (last.homeScore !== undefined && last.awayScore !== undefined) {
        const fromGraph = {
          homeScore: last.homeScore,
          awayScore: last.awayScore,
          finished:  true,
        };
        console.log(`  ✅ Score from graph [${eventId}]: ${fromGraph.homeScore}-${fromGraph.awayScore}`);
        cacheSet(scoreCache, eventId, fromGraph);
        return fromGraph;
      }
    }
  }

  console.log(`  ⚠️ No result data for event ${eventId} — will retry next cycle`);
  return null;
}

// ─── RUNNING P/L RENUMBER ─────────────────────────────────────
// running_pl is a cumulative sum, so it is only meaningful if every row's
// value equals the sum of all profit_loss up to and including it, in
// chronological order. Backfilled rows are inserted with a HISTORICAL
// settled_at, so they land in the middle of the ledger and invalidate every
// running_pl after them. Recompute the column in (settled_at, id) order.
// Returns the true cumulative total so the caller can re-seed from it.
async function recomputeRunningPL() {
  const rows = await selectAll('results_history', 'id, profit_loss, running_pl, settled_at');
  // A comparator must never return NaN. An unparseable settled_at gives NaN
  // from getTime(), and NaN !== NaN is true, so the old comparator took the
  // `ta - tb` branch and returned NaN — which leaves the sort order undefined.
  // Every running_pl is derived from this ordering, so one bad timestamp could
  // scramble the whole ledger rather than just its own row.
  //
  // What this does NOT do, despite an earlier version of this comment saying
  // so: treat a null settled_at as unusable. `new Date(null)` is not NaN — null
  // coerces to 0 — so a null sorts at the epoch, while a genuinely unparseable
  // value sorts at -Infinity. Two buckets rather than one.
  //
  // Both land before every real row, and both are deterministic, which is what
  // the ordering needs. Left alone on purpose: the behaviour is harmless and
  // test_renumber.js already asserts it row by row, so this was a comment that
  // disagreed with working code rather than code that needed changing.
  const at = (r) => {
    const t = new Date(r.settled_at).getTime();
    return Number.isFinite(t) ? t : -Infinity;
  };
  rows.sort((a, b) => {
    const ta = at(a), tb = at(b);
    if (ta !== tb) return ta < tb ? -1 : 1;
    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;   // stable tiebreak on PK
  });

  let acc = 0, fixed = 0;
  for (const r of rows) {
    acc = parseFloat((acc + parseFloat(r.profit_loss || 0)).toFixed(2));
    if (parseFloat(r.running_pl) === acc) continue;   // NaN on NULL -> always rewrites
    const wrote = await updateChecked('results_history', { running_pl: acc }, q => q.eq('id', r.id));
    if (!wrote.ok) { console.error(`  ❌ running_pl renumber [id ${r.id}]: ${wrote.why}`); continue; }
    fixed++;
  }
  if (fixed) console.log(`  🔢 Renumbered running_pl on ${fixed}/${rows.length} ledger rows.`);
  return { total: rows.length, fixed, final: acc };
}

// Concurrency guard. The hourly scheduler, the startup run and /admin/resettle
// all reach this function. Two overlapping runs read the same ledger snapshot,
// race each other's results_history inserts, and can interleave the running_pl
// renumber with a concurrent append — corrupting the column the renumber exists
// to repair.
let settleInFlight = false;
// Whether this process has verified the cumulative column yet. See the
// block at the end of settleResultsInner.
let runningPLVerified = false;

async function settleResults() {
  if (settleInFlight) {
    console.log('⏸️ Settle already in progress — skipping this run.');
    return { settled: 0, backfilled: 0, skipped: true };
  }
  settleInFlight = true;
  try { return await settleResultsInner(); }
  finally { settleInFlight = false; }
}

async function settleResultsInner() {
  const nowIso = new Date().toISOString();

  // PAGINATED — these feed the settler and the backfill. The tips table grows
  // without bound, so an unbounded select here hits the same PostgREST 1000-row
  // cap as the ledger did, and (with no ORDER BY) silently returns an arbitrary
  // subset — meaning some graded tips would never be backfilled at all.
  const pendingTips = await selectAll('tips', '*',
    q => q.eq('status', 'pending').lt('event_time', nowIso));

  const rhRows    = await selectAll('results_history', 'tip_ref');
  const inHistory = new Set(rhRows.map(r => r.tip_ref));

  // Every graded tip ever, on every settle — hourly, and on every boot. It is
  // used for exactly two repairs: giving a tip_ref to a graded tip that lacks
  // one, and finding graded tips missing from the ledger. Both are one-off, so
  // once the ledger is complete this read finds nothing and costs the same.
  //
  // Measured against the live table on 21 August 2026: 136 rows, 105 KB, of
  // which 0 were missing from the ledger. Most of that is `notes`, the model
  // diagnostics string nothing reads.
  //
  // Naming the columns the repairs actually use halves it today and keeps it
  // proportional as the table grows. Deliberately not narrowed by date: this is
  // also what /admin/resettle runs, and bounding it would quietly take away the
  // operator's way of repairing anything older than the window.
  const BACKFILL_COLUMNS = 'id, tip_ref, sport, home_team, away_team, selection, '
    + 'odds, advised_odds, stake, tier, status, confidence, event_time, '
    + 'result_updated_at, profit_loss, is_free';
  const alreadyGraded = await selectAll('tips', BACKFILL_COLUMNS,
    q => q.in('status', ['won', 'lost']).lt('event_time', nowIso));

  // Assign a tip_ref to ANY tip lacking one — graded tips included. tip_ref is
  // the only key linking a tip to its ledger row, so a graded tip with a NULL
  // ref cannot be matched against results_history: it was filtered out of the
  // backfill with no log line and stayed invisible permanently.
  for (const t of [...pendingTips, ...alreadyGraded].filter(t => !t.tip_ref)) {
    const prefixes = { 'Football':'FB','Basketball':'BB','Ice Hockey':'IH' };
    const ref = `${prefixes[t.sport]||'TT'}-${Date.now().toString(36).toUpperCase().slice(-4)}${Math.random().toString(36).toUpperCase().slice(-4)}`;
    const wrote = await updateChecked('tips', { tip_ref: ref }, q => q.eq('id', t.id));
    // Leave t.tip_ref NULL on failure — a ref we did not persist must not be
    // treated as backfillable, or the ledger row would reference nothing.
    // That is as true of a refused write as of a failed one, and only the
    // second was being caught.
    if (!wrote.ok) { console.error(`  ❌ tip_ref assign [id ${t.id}]: ${wrote.why}`); continue; }
    t.tip_ref = ref;
    if (t.status !== 'pending') console.log(`  🏷️ Assigned ${ref} to graded tip id ${t.id} (was NULL) — now eligible for backfill`);
  }

  // Computed AFTER ref assignment, so graded tips that just received one are
  // included rather than silently skipped.
  const missing = alreadyGraded.filter(t => t.tip_ref && !inHistory.has(t.tip_ref));
  const pending = pendingTips;

  // Same shape on every return path — the caller logs these counts, and
  // `ok: !!result` on /admin/resettle read a bare return as a failure.
  if (!pending.length && !missing.length) {
    console.log('🏁 Nothing to settle.');
    // Still worth one pass at the cumulative column if this process has not
    // checked it yet — a ledger that is already wrong will never settle
    // anything to trigger the repair below.
    if (!runningPLVerified) {
      runningPLVerified = true;
      try { await recomputeRunningPL(); }
      catch (e) { console.error('running_pl verify failed:', e.message); }
    }
    return { settled: 0, backfilled: 0 };
  }

  // A failed read seeds the running total at 0, which would renumber every row
  // written below from zero. Not damage that survives — the recompute at the
  // end of this function runs whenever anything was written — but it is worth
  // knowing it happened.
  const { data: lastRow, error: lastRowErr } = await supabase.from('results_history').select('running_pl').order('settled_at', { ascending: false }).limit(1).maybeSingle();
  if (lastRowErr) console.error('Could not read the last running total, seeding from 0 '
    + `(${lastRowErr.message}) — the renumber at the end of this pass corrects it`);
  let currentRunningPL = parseFloat(lastRow?.running_pl || 0);
  const now = Date.now();
  let count = 0, backfilled = 0, dirty = false;

  // ═══════════════════════════════════════════════════════════
  // PASS 1 — LEDGER BACKFILL (no API calls, no re-grading)
  // ═══════════════════════════════════════════════════════════
  // These tips already carry a verified result. They are NOT candidates
  // for voiding or re-grading — the only thing missing is the ledger row.
  // The old code fed them through the main loop, where the >72h staleness
  // check flipped them to 'void' and permanently erased the result.
  if (missing.length) {
    console.log(`🩹 Backfilling ${missing.length} graded tips missing from results_history...`);
    missing.sort((a, b) => new Date(a.event_time) - new Date(b.event_time));

    for (const tip of missing) {
      try {
        const oddsUsed = advisedPrice(tip);
        const stake    = parseFloat(tip.stake ?? 0);

        // These were one condition, `!oddsUsed || !stake`, which skipped a
        // stake of 0 because !0 is true. Pass 2 records those — an insight
        // pick settles with a ledger row carrying stake 0 and P/L 0 — so the
        // two passes disagreed about whether insight picks belong in the
        // ledger, and one whose row was lost could never be restored.
        //
        // Pass 2 is the behaviour to match: their P/L is 0 so they move no
        // total, every published figure already filters them out by tier and
        // stake, and keeping them preserves the record of how the short-price
        // picks actually did.
        if (!Number.isFinite(oddsUsed) || oddsUsed <= 1) {
          console.log(`  ⚠️ Skipping backfill [${tip.tip_ref}] — unusable price (${tip.advised_odds ?? tip.odds})`);
          continue;
        }
        if (!Number.isFinite(stake) || stake < 0) {
          console.log(`  ⚠️ Skipping backfill [${tip.tip_ref}] — unusable stake (${tip.stake})`);
          continue;
        }

        let pl = tip.profit_loss;
        if (pl === null || pl === undefined) {
          pl = tip.status === 'won'
            ? parseFloat(((oddsUsed - 1) * stake).toFixed(2))
            : parseFloat((-stake).toFixed(2));
          // The ledger row below carries this value regardless, so a failure
          // here costs nothing immediately and the next run recomputes it.
          // Logged all the same: silent and self-healing looks identical to
          // silent and permanently stuck.
          const { error: plErr } = await supabase.from('tips')
            .update({ profit_loss: pl }).eq('tip_ref', tip.tip_ref);
          if (plErr) console.error(`Backfill could not store P&L for [${tip.tip_ref}]:`, plErr.message);
        }
        pl = parseFloat(pl);

        currentRunningPL = parseFloat((currentRunningPL + pl).toFixed(2));

        const { error: insErr } = await supabase.from('results_history').insert({
          tip_ref:     tip.tip_ref,
          sport:       tip.sport,
          event:       `${tip.home_team} vs ${tip.away_team}`,
          selection:   tip.selection,
          odds:        oddsUsed,
          stake:       stake,
          tier:        tip.tier || 'pro',
          result:      tip.status === 'won' ? 'WON' : 'LOST',
          profit_loss: pl,
          // PROVISIONAL — settled_at below is historical, so this row does not
          // belong at the end of the ledger. recomputeRunningPL() below rewrites
          // this to the true cumulative value for its chronological position.
          running_pl:  currentRunningPL,
          settled_at:  tip.result_updated_at || tip.event_time || nowIso,
          confidence:  tip.confidence || 0,
          // tier is only ever 'pro' or 'insight'; which tips were FREE is
          // carried by is_free, and the ledger used to lose it at settle.
          is_free:     tip.is_free === true,
        });
        if (insErr) {
          // Unique violation = another cycle got there first; roll back our tally.
          if (insErr.code === '23505') currentRunningPL = parseFloat((currentRunningPL - pl).toFixed(2));
          else console.error(`  ❌ Backfill insert [${tip.tip_ref}]:`, insErr.message);
          continue;
        }
        backfilled++; dirty = true;
        console.log(`  🩹 Restored [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team} — ${tip.status.toUpperCase()} (${pl >= 0 ? '+' : ''}${pl}u)`);
      } catch(e) { console.error(`Backfill error [${tip.tip_ref}]:`, e.message); }
    }

    // Backfilled rows carry historical settled_at values, so they were inserted
    // into the MIDDLE of the ledger. Rebuild the cumulative column, then re-seed
    // the tally from the true total — otherwise the next append is computed from
    // a stale head row and the backfilled P/L is dropped from running_pl forever.
    if (backfilled) {
      try {
        const { final } = await recomputeRunningPL();
        currentRunningPL = final;
      } catch(e) { console.error('running_pl renumber failed:', e.message); }
    }
  }

  // ═══════════════════════════════════════════════════════════
  // PASS 2 — GRADE PENDING TIPS
  // ═══════════════════════════════════════════════════════════
  if (pending.length) console.log(`🏁 Settling ${pending.length} pending tips...`);

  for (const tip of pending) {
    try {
      // What to do when this tip has no score.
      //
      // This decision used to be made BEFORE asking: anything more than 72
      // hours past kick-off was voided on the spot, without a single request.
      // The 72 hours stood in for "we have tried and failed repeatedly", and
      // those are only the same thing while the settler is running. They come
      // apart exactly when it matters — if the service is down for three days,
      // the first run after it comes back voids every tip from those three
      // days rather than grading them, and a void writes no ledger row at all.
      // The published record loses the period silently, and the win rate and
      // ROI on the site are then computed from a truncated history.
      //
      // That is not hypothetical. The deployed build has been frozen since 12
      // August, so there is a week of pending tips waiting for exactly this.
      //
      // Ask first, decide after. And do not conclude anything at all while the
      // data source is unreachable: sofascoreFetch returns null for a spent
      // budget, a 429 cooldown, a timeout and a genuine "no result" alike, and
      // three of those four mean we never asked.
      const hoursOld = (now - new Date(tip.event_time).getTime()) / 3600000;

      // Old enough that no answer is coming. Voided without a call, because
      // asking again every fifteen minutes for the rest of the service's life
      // is the only other option.
      if (hoursOld > SETTLE_ASK_UNTIL_HOURS) {
        // Say what happened, not what was attempted. The log line below is the
        // only record that this tip was dealt with, and printing it regardless
        // of the write meant a failed void read as a completed one — while the
        // row stayed pending and came back round on every later cycle, for
        // ever, announcing itself as voided each time.
        const { error: voidErr } = await supabase.from('tips')
          .update({ status: 'void' }).eq('tip_ref', tip.tip_ref);
        if (voidErr) {
          console.error(`Could not void [${tip.tip_ref}]: ${voidErr.message} `
            + `— left pending, will be reconsidered next cycle`);
          continue;
        }
        console.log(`⚪ VOID (${Math.round(hoursOld / 24)} days old, beyond the window `
          + `the source retains): [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team}`);
        continue;
      }

      const noResult = async (why) => {
        if (hoursOld <= 72) {
          console.log(`⏳ ${why}: ${tip.home_team} vs ${tip.away_team}`);
          return;
        }
        if (!apiCallsAvailable()) {
          console.log(`⏸️ ${why} and ${Math.round(hoursOld)}h old, but the data source is `
            + `unavailable — [${tip.tip_ref}] left pending rather than voided`);
          return;
        }
        const { error: voidErr } = await supabase.from('tips')
          .update({ status: 'void' }).eq('tip_ref', tip.tip_ref);
        if (voidErr) {
          console.error(`Could not void [${tip.tip_ref}]: ${voidErr.message} `
            + `— left pending, will be reconsidered next cycle`);
          return;
        }
        console.log(`⚪ VOID (asked, no result after ${Math.round(hoursOld)}h): `
          + `[${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team}`);
      };

      let homeScore = null, awayScore = null;

      // Use stored event_id for direct Sofascore lookup — no name matching needed
      const eventId = tip.event_id;
      if (eventId) {
        const result = await fetchSofascoreResult(eventId);
        if (result) {
          homeScore = result.homeScore;
          awayScore = result.awayScore;
        } else {
          // /matches/detail returned 204 — try last-matches for this tournament
          const sport2 = SPORTS.find(s => s.league === tip.league);
          if (sport2) {
            try {
              const sd = await sofascoreFetch(`/tournaments/get-seasons`, { tournamentId: sport2.tournamentId });
              if (sd?.seasons?.length) {
                const season2 = sd.seasons[0];
                await new Promise(r => setTimeout(r, 250));
                let ldEvents = [];
                for (let pg = 0; pg <= 2; pg++) {
                  const ld = await sofascoreFetch(`/tournaments/get-last-matches`, { tournamentId: sport2.tournamentId, seasonId: season2.id, page: pg });
                  ldEvents = ldEvents.concat(ld?.events || []);
                  if ((ld?.events || []).length === 0) break;
                  await new Promise(r => setTimeout(r, 200));
                }
                const lev = ldEvents.find(e => String(e.id) === String(eventId));
                if (lev) {
                  homeScore = lev.homeScore?.current ?? lev.homeScore?.normaltime ?? null;
                  awayScore = lev.awayScore?.current ?? lev.awayScore?.normaltime ?? null;
                  if (homeScore !== null) console.log(`  📊 Score from last-matches by id [${eventId}]: ${homeScore}-${awayScore}`);
                }
              }
            } catch(e2) { console.log(`  ⚠️ last-matches fallback failed: ${e2.message}`); }
          }
          if (homeScore === null) { await noResult('No result yet'); continue; }
        }
      } else {
        // Fallback: name match from cache for tips created before event_id was stored
        const sport = SPORTS.find(s => s.league === tip.league);
        // A league this build does not know about is a permanent dead end for
        // this tip, but it still goes through noResult so that the 72-hour rule
        // stays the one place a void is decided.
        if (!sport) { await noResult('League not configured'); continue; }

        // Step 1: check live cache (upcoming/in-progress games)
        const cachedEvents = sofascoreCache.events[sport.key] || [];
        let cachedEvent = cachedEvents.find(e =>
          nameMatch(e.home_team, tip.home_team) && nameMatch(e.away_team, tip.away_team)
        );

        // Step 2: if not in live cache, fetch last matches from tournament (finished games drop out of next-matches)
        if (!cachedEvent?.id) {
          console.log(`🔍 Not in live cache, fetching last matches for ${tip.league}...`);
          try {
            const seasonsData = await sofascoreFetch(`/tournaments/get-seasons`, { tournamentId: sport.tournamentId });
            if (seasonsData?.seasons?.length) {
              const season = seasonsData.seasons[0];
              await new Promise(r => setTimeout(r, 250));
              let lastEvents = [];
              for (let pg = 0; pg <= 2; pg++) {
                const lastData = await sofascoreFetch(`/tournaments/get-last-matches`, {
                  tournamentId: sport.tournamentId,
                  seasonId: season.id,
                  page: pg,
                });
                lastEvents = lastEvents.concat(lastData?.events || []);
                if ((lastData?.events || []).length === 0) break;
                await new Promise(r => setTimeout(r, 200));
              }
              const found = lastEvents.find(e => {
                const ht = e.homeTeam?.name || e.home_team || '';
                const at = e.awayTeam?.name || e.away_team || '';
                return nameMatch(ht, tip.home_team) && nameMatch(at, tip.away_team);
              });
              if (found) {
                console.log(`✅ Found in last matches: ${tip.home_team} vs ${tip.away_team} (id: ${found.id})`);
                // Extract score directly from event object — /matches/detail 204s on finished games
                const hs  = found.homeScore?.current ?? found.homeScore?.normaltime ?? null;
                const as_ = found.awayScore?.current ?? found.awayScore?.normaltime ?? null;
                // Backfill event_id regardless
                await supabase.from('tips').update({ event_id: found.id }).eq('tip_ref', tip.tip_ref);
                if (hs !== null && as_ !== null) {
                  homeScore = hs;
                  awayScore = as_;
                  console.log(`  📊 Score from last-matches: ${hs}-${as_}`);
                } else {
                  const result = await fetchSofascoreResult(found.id);
                  if (!result) { await noResult('No result yet'); continue; }
                  homeScore = result.homeScore;
                  awayScore = result.awayScore;
                }
              } else {
                await noResult('No cached event');
                continue;
              }
            } else {
              // A null here is just as likely to be a spent budget as an empty
              // season list, which is why this goes through noResult too.
              await noResult('No cached event');
              continue;
            }
          } catch (e) {
            // A throw means we could not ask. Never a void.
            console.log(`⚠️ Last matches fetch failed for ${tip.league}: ${e.message}`);
            continue;
          }
        } else {
          const result = await fetchSofascoreResult(cachedEvent.id);
          if (!result) { await noResult('No result yet'); continue; }
          homeScore = result.homeScore;
          awayScore = result.awayScore;
        }
      }

      if (homeScore === null || awayScore === null) { await noResult('No score returned'); continue; }

      // Grade the selection. Resolve the side against BOTH teams — the old
      // ternary treated "not a home-name match" as "must be away", so a single
      // fuzzy-match miss silently graded the opposite team.
      // `push` is a whole-line totals tie — the total lands exactly on the line,
      // the stake comes back and the bet is neither won nor lost. Both branches
      // below used to fall to the losing side of the comparison, so a push was
      // recorded as a full stake lost.
      //
      // Unreachable from what the engine currently publishes: the only totals
      // selection it generates is 'Over 5.5', and a half line cannot tie. Admin
      // overrides do take free text, but they build an email-only object and
      // are never written to tips, so they are never settled. This is the
      // settlement maths being right rather than a live defect — whole lines
      // are ordinary if a totals market is ever added.
      let won = false, graded = true, push = false;
      const sel = (tip.selection || '').toLowerCase().trim();

      if (/ win$/i.test(tip.selection || '')) {
        const team   = tip.selection.replace(/ win$/i, '').trim();
        const isHome = nameMatch(team, tip.home_team);
        const isAway = nameMatch(team, tip.away_team);
        if (isHome && !isAway)      won = homeScore > awayScore;
        else if (isAway && !isHome) won = awayScore > homeScore;
        else {
          console.error(`🚨 UNGRADEABLE [${tip.tip_ref}] "${tip.selection}" matches ${isHome && isAway ? 'BOTH' : 'NEITHER'} of "${tip.home_team}" / "${tip.away_team}" — left pending for manual review`);
          graded = false;
        }
      } else if (sel === 'draw') {
        won = homeScore === awayScore;
      } else if (sel.startsWith('over')) {
        const line = parseFloat(sel.replace('over ', ''));
        if (!Number.isFinite(line)) graded = false;
        else if (homeScore + awayScore === line) push = true;
        else won = (homeScore + awayScore) > line;
      } else if (sel.startsWith('under')) {
        const line = parseFloat(sel.replace('under ', ''));
        if (!Number.isFinite(line)) graded = false;
        else if (homeScore + awayScore === line) push = true;
        else won = (homeScore + awayScore) < line;
      } else {
        console.error(`🚨 UNGRADEABLE [${tip.tip_ref}] unrecognised selection "${tip.selection}"`);
        graded = false;
      }
      if (!graded) continue;

      // Settle at the price we ACTUALLY ADVISED, not the peak price ever seen.
      // best_odds is the max observed across refreshes; paying wins at the peak
      // while losing full stake on losers systematically overstates ROI.
      // Legacy rows (pre-v9.9) have no advised_odds and fall back to `odds`.
      const settlementOdds = advisedPrice(tip);
      if (!Number.isFinite(settlementOdds) || settlementOdds <= 1) {
        console.error(`🚨 [${tip.tip_ref}] invalid settlement odds (${settlementOdds}) — skipping`);
        continue;
      }
      // Guarded the same way the odds are, immediately above. A null or
      // non-numeric stake would otherwise produce NaN, and NaN written into
      // profit_loss propagates through every aggregate that sums the ledger.
      const settlementStake = parseFloat(tip.stake);
      if (!Number.isFinite(settlementStake) || settlementStake < 0) {
        console.error(`🚨 [${tip.tip_ref}] invalid stake (${tip.stake}) — skipping`);
        continue;
      }

      const pl = push ? 0
        : won ? parseFloat(((settlementOdds - 1) * settlementStake).toFixed(2))
              : parseFloat((-settlementStake).toFixed(2));

      // If this read fails, `already` is null, which is what a tip that has
      // never been settled also looks like — and the insert below then writes a
      // second ledger row and adds its P&L to the running total a second time.
      // The unique constraint that would refuse it lives in
      // schema-migration.sql, which has not been run, so nothing else is
      // standing behind this. Settlement retries every pass.
      const { data: already, error: alreadyErr } = await supabase.from('results_history').select('id').eq('tip_ref', tip.tip_ref).maybeSingle();
      if (alreadyErr) {
        console.error(`🚨 [${tip.tip_ref}] could not check whether it is already in the ledger `
          + `(${alreadyErr.message}) — leaving it for the next pass rather than settling it twice`);
        continue;
      }

      // The ledger row and the tip's own status are two writes, and only the
      // second was checked. If this one fails the tip stays `pending` while
      // results_history holds its result — the site shows it as unsettled for
      // ever. It does retry on the next run, because this happens before the
      // `already` check, so the failure is recoverable; what it was not is
      // visible.
      const { error: statusErr } = await supabase.from('tips').update({
        status: push ? 'void' : won ? 'won' : 'lost', profit_loss: pl,
        result_updated_at: new Date().toISOString()
      }).eq('tip_ref', tip.tip_ref);
      if (statusErr) {
        console.error(`🚨 [${tip.tip_ref}] settled but its status could not be written `
          + `(${statusErr.message}) — it stays pending and will be retried`);
      }

      if (already) continue;

      currentRunningPL = parseFloat((currentRunningPL + pl).toFixed(2));

      const { error: ledgerErr } = await supabase.from('results_history').insert({
        tip_ref:    tip.tip_ref,
        sport:      tip.sport,
        event:      `${tip.home_team} vs ${tip.away_team}`,
        selection:  tip.selection,
        odds:       settlementOdds,
        stake:      settlementStake,
        tier:       tip.tier || 'pro',
        // VOID is already the value the acca settler uses for a void, and every
        // aggregate counts wins and losses explicitly, so a VOID row is
        // excluded from win rate while its 0 still sums correctly into P/L.
        result:     push ? 'VOID' : won ? 'WON' : 'LOST',
        profit_loss: pl,
        running_pl:  currentRunningPL,
        settled_at:  new Date().toISOString(),
        confidence: tip.confidence || 0,
        is_free:    tip.is_free === true,
      });

      // currentRunningPL was advanced above on the assumption this insert would
      // land. Discarding the error meant a failed insert left the tally ahead
      // of the ledger, so every remaining tip in the batch was written with a
      // running_pl that counted a row which does not exist. Roll it back, the
      // same way Pass 1 already does on a unique violation.
      if (ledgerErr) {
        currentRunningPL = parseFloat((currentRunningPL - pl).toFixed(2));
        console.error(`❌ Ledger insert failed [${tip.tip_ref}]:`, ledgerErr.message,
                      '— tip is graded but missing from results_history; Pass 1 will backfill it next run');
        continue;
      }

      console.log(`${push ? '➖ PUSH' : won ? '✅ WON' : '❌ LOST'}: [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team} — ${tip.selection} @ ${settlementOdds} (${pl >= 0 ? '+' : ''}${pl}u)`);
      count++; dirty = true;

    } catch(e) { console.error(`Settle error [${tip.tip_ref}]:`, e.message); }
  }

  // Recompute stats ONCE, not once per settled tip.
  if (dirty) await updateStatsCache();

  // Settle daily accas
  try {
    // The error was discarded, so a failed read looked exactly like "no accas
    // are pending": the loop simply did not run and nothing was logged. Accas
    // would quietly stop settling with no signal anywhere.
    const { data: pendingAccas, error: accaErr } = await supabase.from('daily_accas').select('*').eq('result', 'pending');
    if (accaErr) throw new Error(`could not read pending accas: ${accaErr.message}`);
    for (const acca of (pendingAccas || [])) {
      const tipRefs = (acca.selections || []).map(s => s.tip_ref).filter(Boolean);
      if (!tipRefs.length) continue;
      const { data: legTips, error: legErr } = await supabase.from('tips').select('tip_ref, status, odds, best_odds, advised_odds').in('tip_ref', tipRefs);
      // A failed read is not the same as legs that are genuinely absent. The
      // second needs manual review; the first just needs another run, and
      // saying "needs manual review" for it sends someone looking for rows
      // that are there.
      if (legErr) {
        console.error(`❌ Acca ${acca.date}: could not read leg tips — ${legErr.message}; will retry next run`);
        continue;
      }
      if (!legTips || legTips.length < tipRefs.length) {
        console.warn(`⚠️ Acca ${acca.date}: ${tipRefs.length - (legTips?.length || 0)} leg tip(s) missing from tips table — cannot settle, needs manual review`);
        continue;
      }
      const allSettled = legTips.every(t => ['won','lost','void'].includes(t.status));
      if (!allSettled) continue;
      const activeLegs = legTips.filter(t => t.status !== 'void');
      if (!activeLegs.length) {
        // Through updateChecked, like the WON/LOST write twenty lines below,
        // and for the same reason given there: an unchecked write leaves the
        // acca silently pending for ever, and this branch had no log at all to
        // notice it by.
        const voidWrote = await updateChecked('daily_accas', { result: 'VOID', profit_loss: 0 },
                                              q => q.eq('id', acca.id));
        if (!voidWrote.ok) {
          console.error(`Acca ${acca.date}: every leg voided, but the write failed `
            + `(${voidWrote.why}) — left pending, the next run re-settles it`);
        }
        continue;
      }
      const allWon = activeLegs.every(t => t.status === 'won');
      const result = allWon ? 'WON' : 'LOST';
      let pl;
      if (allWon) {
        // Accas settle at advised_odds for the same reason single tips do.
        // Using best_odds here compounds the peak-price overstatement across
        // every leg, and would leave the acca ledger and the single-tip ledger
        // reporting ROI on two different definitions of "the price".
        const combinedAdvised = activeLegs.reduce((acc, t) => acc * advisedPrice(t), 1);
        if (!Number.isFinite(combinedAdvised) || combinedAdvised <= 1) {
          console.error(`🚨 Acca ${acca.date} has invalid combined odds (${combinedAdvised}) — left pending`);
          continue;
        }
        pl = parseFloat(((combinedAdvised - 1) * parseFloat(acca.stake ?? 1)).toFixed(2));
      } else {
        pl = parseFloat((-parseFloat(acca.stake ?? 1)).toFixed(2));
      }
      const updWrote = await updateChecked('daily_accas', { result, profit_loss: pl },
                                           q => q.eq('id', acca.id));
      const updErr = updWrote.ok ? null : { message: updWrote.why };
      if (updErr) {
        // Left pending on purpose: the next run re-settles it. Logging matters
        // because otherwise the acca silently never leaves 'pending'.
        console.error(`❌ Acca ${acca.date}: settle write failed — ${updErr.message}; stays pending for the next run`);
        continue;
      }
      console.log(`📋 Acca ${acca.date} settled: ${result} (${pl >= 0 ? '+' : ''}${pl}u)`);
    }
  } catch(e) { console.error('Acca settlement error:', e.message); }

  // Verify the cumulative column rather than trusting it.
  //
  // currentRunningPL is seeded from the newest row's running_pl, so every
  // append is only as correct as that one value. Measured against the live
  // ledger: 135 of its 136 rows disagree with an accumulation of their own
  // profit_loss column, and the chain ends 2.42u short of the true total.
  // index.html draws its cumulative chart straight from running_pl, so the
  // published curve is wrong by that much — and because the next append
  // continues from the stale head row, the error is permanent.
  //
  // The repair already existed and was correct. It only ran after a
  // BACKFILL, which is the one case that inserts into the middle of the
  // ledger — so nothing ever fixed damage that was already there. It runs
  // whenever the ledger changed now, and once per process regardless.
  // Against a correct chain it reads the table and writes nothing.
  if (count || backfilled || !runningPLVerified) {
    runningPLVerified = true;
    // recomputeRunningPL logs its own count when it rewrites anything.
    try { await recomputeRunningPL(); }
    catch (e) { console.error('running_pl renumber failed:', e.message); }
  }

  console.log(`🏁 Settled ${count} tips${backfilled ? `, backfilled ${backfilled}` : ''}.`);
  return { settled: count, backfilled };
}

// ═══════════════════════════════════════════════════════════════
// STATS CACHE
// ═══════════════════════════════════════════════════════════════

async function updateStatsCache() {
  try {
    // PAGINATED — an unbounded select caps at 1000 rows, so published
    // win rate and ROI would silently be computed from a partial ledger.
    const rows = await selectAll('results_history', 'result, profit_loss, stake, tier');
    if (!rows.length) return;

    // Win rate and ROI have to describe the SAME set of tips, and until now
    // they did not. applyStrictRules publishes short-price selections with
    // stake 0 and tier 'insight' — informational, never advised as bets. Those
    // counted towards win rate but, having no stake, contributed nothing to
    // ROI. So the two headline figures were computed over different
    // populations, and because insight picks are short-priced they win more
    // often than average, which pulled the published win rate up.
    //
    // There is no way to include them in ROI — you cannot compute a return on a
    // stake of zero — so the only coherent resolution is to report the record
    // of actual bets. Both figures now cover the same staked rows.
    const data = rows.filter(isBet);
    const skipped = rows.length - data.length;
    if (skipped) console.log(`📈 Excluding ${skipped} informational pick(s) from the published record`);

    // The published total and the ledger total agree only because every row
    // excluded above carries a P/L of zero. That is true by construction —
    // settlement computes P/L from the stake, and these are the rows with no
    // stake — but nothing checked it, and the two figures are derived
    // separately: total_pl here sums the bets, while recomputeRunningPL
    // accumulates every row in the ledger.
    //
    // If that ever stopped holding, the running_pl column and the headline
    // figure would drift apart silently, each internally consistent and
    // disagreeing with the other. A ledger that disagrees with its own total is
    // the one failure the published record could not survive, so it is worth
    // one pass over rows already in hand to say so.
    const leaked = rows.filter(r => !isBet(r))
                       .reduce((sum, r) => sum + Math.abs(parseFloat(r.profit_loss || 0)), 0);
    if (leaked > 0.005) {
      console.error(`🚨 LEDGER DRIFT: rows excluded from the published figures carry `
        + `${leaked.toFixed(2)}u of P/L between them. running_pl accumulates every row and `
        + `total_pl sums only the staked ones, so the two have separated. Needs manual review.`);
    }

    // Every aggregate here and on the site compares against the upper-case
    // form, so a row spelled any other way is counted as neither won nor lost
    // — while its profit_loss still sums into P/L. The two headline figures
    // then describe different populations, which is precisely what the
    // insight-pick filter above exists to prevent.
    //
    // The live ledger held fourteen such rows: win rate over 93, ROI over 107.
    // schema-migration.sql normalises them; this says so if it happens again.
    const CANONICAL = ['WON', 'LOST', 'VOID'];
    const odd = data.filter(r => !CANONICAL.includes(r.result));
    if (odd.length) {
      console.error(`🚨 ${odd.length} ledger row(s) carry a result this code cannot read `
        + `(${[...new Set(odd.map(r => JSON.stringify(r.result)))].join(', ')}). They are in `
        + `P/L and not in the win rate, so the two figures describe different sets of tips.`);
    }

    if (!data.length) return;

    const won   = data.filter(r => r.result === 'WON').length;
    const lost  = data.filter(r => r.result === 'LOST').length;
    const total = won + lost;
    const pl    = data.reduce((s,r) => s + parseFloat(r.profit_loss || 0), 0);
    // A push returns the stake, so it is not turnover. Counting it would
    // inflate the ROI denominator and understate the return. Wins and losses
    // are already the only rows in the win-rate figures above, so this keeps
    // both headline numbers over the same set.
    const settledRows = data.filter(r => r.result === 'WON' || r.result === 'LOST');
    const stk   = settledRows.reduce((s,r) => s + parseFloat(r.stake ?? 1), 0);
    const { data: cached, error: statsErr } = await supabase.from('stats_cache').update({
      total_tips:   total, total_won: won, total_lost: lost,
      win_rate:     total > 0 ? parseFloat((won/total*100).toFixed(1)) : 0,
      total_pl:     parseFloat(pl.toFixed(2)),
      total_staked: parseFloat(stk.toFixed(2)),
      roi:          stk > 0 ? parseFloat((pl/stk*100).toFixed(1)) : 0,
    }).eq('id', 1).select('id');

    // The homepage headline figures read this row. A failed write left them
    // frozen at whatever they last were, silently — the numbers would simply
    // stop moving while settlement carried on, which looks like nothing
    // happening rather than like a fault.
    if (statsErr) { console.error('❌ Stats cache update failed:', statsErr.message); return; }
    if (!cached || !cached.length) { console.error('❌ Stats cache row id=1 does not exist — published figures will not update'); return; }

    console.log(`📈 Stats: ${won}W/${lost}L | ${total > 0 ? (won/total*100).toFixed(1) : 0}% | ${pl >= 0 ? '+' : ''}${pl.toFixed(2)}u`);
  } catch(e) { console.error('Stats cache error:', e.message); }
}

// ═══════════════════════════════════════════════════════════════
// MAIN ENGINE LOOP — reads from cache, zero API calls
// ═══════════════════════════════════════════════════════════════

async function runEngine() {
  console.log(`\n🚀 Engine v9.9 — ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`);
  console.log('═'.repeat(52));

  // Safety: if cache is empty (engine just started), don't run until morning fetch completes
  const hasData = Object.values(sofascoreCache.events).some(arr => arr.length > 0);
  if (!hasData) {
    console.log('⏳ Cache empty — waiting for morning fetch...');
    return;
  }

  // And do not advise a bet at a price nobody has checked recently.
  //
  // Odds are refreshed twice a day, at 06:00 and 13:00, so the longest they are
  // legitimately allowed to get is the overnight gap — about 17 hours, 18
  // across a clock change. Nothing checked. If a fetch failed, the cache kept
  // yesterday's events, generateTips happily found the ones still inside its
  // 48-hour window, and the engine went on publishing tips every fifteen
  // minutes at prices from the previous day.
  //
  // A fully stale cache was already harmless — every event falls outside the
  // window and no tips come out. This is the partial case, which is the one
  // that produces confident output from data nobody would stand behind.
  //
  // Checked per sport, because the fetch is per sport: one league failing while
  // the others succeed is the likeliest form of this, and a single cache-wide
  // timestamp cannot see it. One stale league is skipped; the rest still
  // publish, because withholding good tips is its own kind of wrong.
  const stale = (sport) => {
    const at = sofascoreCache.fetchedAt && sofascoreCache.fetchedAt[sport.key];
    const age = at ? Date.now() - new Date(at).getTime() : Infinity;
    return age > ODDS_MAX_AGE_MS
      ? (Number.isFinite(age) ? (age / 3600000).toFixed(1) + 'h old' : 'never fetched')
      : null;
  };

  let all = [], skipped = 0;
  for (const sport of SPORTS) {
    const events = sofascoreCache.events[sport.key] || [];
    if (!events.length) continue;
    const why = stale(sport);
    if (why) {
      skipped++;
      console.error(`🚨 ${sport.league}: odds are ${why} (limit ${ODDS_MAX_AGE_MS / 3600000}h) `
        + `— skipping until a fetch succeeds`);
      continue;
    }
    console.log(`Analysing ${sport.league} (${events.length} events from cache)...`);
    const tips = await generateTips(events, sport);
    console.log(`  → ${tips.length} tips`);
    all = all.concat(tips);
  }
  if (skipped && !all.length) {
    console.error(`🚨 Every sport with cached events has stale odds — publishing nothing this cycle`);
    return;
  }
  console.log(`\n💾 Saving ${all.length} tips...`);
  await saveTips(all);
  await tagFreeTips();
  console.log('✅ Cycle complete.\n');
}

// ═══════════════════════════════════════════════════════════════
// EMAIL SYSTEM (unchanged from v7)
// ═══════════════════════════════════════════════════════════════

const RESEND_API_KEY  = process.env.RESEND_API_KEY || '';
const FROM_EMAIL      = 'info@thetipsteredge.com';
const FROM_NAME       = 'The Tipster';
const SITE_URL        = 'https://www.thetipsteredge.com';

// userId, when given, adds the RFC 8058 one-click unsubscribe headers.
//
// There were none. Gmail and Yahoo have required them of bulk senders since
// 2024, so their absence costs inbox placement — and it means the only way out
// of the list was the link in the footer, which recipients who cannot find it
// replace with the spam button.
async function sendEmail({ to, subject, html, type = 'general', userId = null }) {
  if (!RESEND_API_KEY) { console.log(`📧 No RESEND key — skipping email to ${to}`); return false; }
  let ok = false;
  try {
    const extraHeaders = {};
    if (userId) {
      const unsub = `${SITE_URL}/unsubscribe?token=${generateUnsubToken(userId)}&uid=${userId}`;
      extraHeaders['List-Unsubscribe'] = `<${unsub}>`;
      extraHeaders['List-Unsubscribe-Post'] = 'List-Unsubscribe=One-Click';
    }
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: `${FROM_NAME} <${FROM_EMAIL}>`, to, subject, html,
        ...(Object.keys(extraHeaders).length ? { headers: extraHeaders } : {}) }),
    });
    const data = await res.json();
    if (!res.ok) console.error(`Email error (${type}):`, data.message || data);
    else { console.log(`📧 Sent (${type}) → ${to}`); ok = true; }
  } catch(e) { console.error(`Email error (${type}):`, e.message); }

  // Record the attempt. The admin dashboard reads email_log for its recent-email
  // table and its total-sent counter, but nothing ever wrote to it, so both were
  // permanently empty. Only the columns the dashboard actually reads are written.
  //
  // Deliberately non-fatal and non-blocking on failure: a delivered email must
  // not be reported as failed because the log row could not be inserted.
  try {
    const { error } = await supabase.from('email_log').insert({
      recipient: to,
      subject,
      type,
      status: ok ? 'sent' : 'failed',
      sent_at: new Date().toISOString(),
    });
    if (error) console.error('email_log write failed:', error.message);
  } catch(e) { console.error('email_log write failed:', e.message); }

  return ok;
}

function generateUnsubToken(uid) {
  // Message format is deliberately unchanged (bare uid): altering it would
  // invalidate every unsubscribe link already sent out.
  return crypto.createHmac('sha256', UNSUB_SECRET).update(uid).digest('hex').slice(0, 16);
}

function verifyUnsubToken(token, uid) {
  return safeEqual(token, generateUnsubToken(uid));
}

// The unsubscribe link is the one address in an email that nothing clicks in
// testing and everything depends on.
//
// It is served by THIS process but reached through the site, via a rewrite in
// the frontend's vercel.json. So a frontend deployed without that rewrite
// leaves every unsubscribe link in every email answering 404 — and nothing
// here would know, because the engine's own route is fine. It was answering
// 404 in production while this check was being written.
//
// That is a legal obligation as much as a deliverability one. PECR requires a
// working means of refusing further messages in every one sent, and a bulk
// sender whose only way out is the spam button loses the inbox with it.
//
// The route answers 403 to a well-formed token with a wrong signature, so a
// 403 coming back is proof the request reached this process through the site
// — which a 200 from some other page would not be. Log-only, and never
// blocking: the frontend being slow is not a reason to delay starting.
// What this build can and cannot do, said once at boot.
//
// Two variables are hard boot guards, and everything else degrades quietly:
// without RESEND_API_KEY every dispatch logs a skip and returns success,
// without RAPIDAPI_KEY every fetch fails and the card stays empty, without
// STRIPE_SECRET_KEY checkout returns 502 and /verify-pro answers "unverified"
// for ever. Each of those is a deliberate degradation and each is invisible
// unless you already suspect it — a service that looks healthy and quietly
// does nothing is the most expensive kind of misconfiguration.
//
// Names only. Never the values.
function logConfiguration() {
  const rows = [
    ['Supabase',      !!SUPABASE_URL && !!process.env.SUPABASE_SERVICE_KEY,
     'SUPABASE_URL + SUPABASE_SERVICE_KEY', 'nothing can be read or written'],
    ['Sofascore',     !!RAPIDAPI_KEY,
     'RAPIDAPI_KEY', 'no fixtures, no odds, no results — the card stays empty'],
    ['Resend',        !!RESEND_API_KEY,
     'RESEND_API_KEY', 'every dispatch is skipped and logged as skipped'],
    ['Stripe',        !!STRIPE_SECRET_KEY,
     'STRIPE_SECRET_KEY', 'checkout answers 502 and no subscription can be bought'],
    ['Stripe webhook', !!STRIPE_WEBHOOK_SECRET,
     'STRIPE_WEBHOOK_SECRET', 'payment events are refused, so nobody becomes Pro'],
    ['Unsub signing', !!UNSUB_SECRET && UNSUB_SECRET !== STRIPE_WEBHOOK_SECRET,
     'UNSUB_SECRET', 'links are signed with the Stripe secret, so rotating it '
     + 'breaks every unsubscribe link already delivered'],
  ];
  console.log('   Configuration:');
  for (const [name, ok, vars, consequence] of rows) {
    if (ok) console.log(`     ${name}: on`);
    else console.log(`     ⚠️ ${name}: OFF (${vars}) — ${consequence}`);
  }
  const proxyHops = Math.max(1, parseInt(process.env.TRUSTED_PROXY_HOPS || '1', 10) || 1);
  console.log(`     Rate limit: ${RATE_LIMIT}/min, ${RATE_LIMIT_WEBHOOK}/min on the Stripe `
    + `webhook, reading the client address ${proxyHops} hop(s) from the right of `
    + `x-forwarded-for`);
  console.log(`     RapidAPI budget: ${RAPIDAPI_DAILY_BUDGET} calls per UK day`);
}

// Does the database have the columns this build writes?
//
// logConfiguration above answers "is the service configured", and this answers
// the other half — "is the schema the one this code was written against".
// Without it a deploy that runs ahead of schema-migration.sql boots looking
// perfectly healthy: PostgREST refuses each statement that names a missing
// column, the failure surfaces as a per-write error buried among the run logs,
// and the visible symptom is a site that says "No tips yet" for ever.
//
// A read of a column the table does not have is refused with 42703, which is
// exactly the question being asked and costs one cheap request per column.
async function checkSchema() {
  const required = [
    ['tips', 'advised_odds',
     'EVERY tip insert is refused — nothing will be published at all'],
    ['tips', 'is_free',
     'the free tier falls back to position, so the free email and the site can disagree'],
    ['users', 'stripe_event_at',
     'EVERY subscription write is refused — nobody who pays is upgraded'],
    ['admin_jobs', 'claimed_at',
     'no job can be claimed, so the manual send buttons do nothing'],
    ['results_history', 'is_free',
     'the ledger cannot record which tips were free'],
  ];
  // In parallel, and time-boxed. Startup waits for this so the message lands
  // above the failures it explains, which means it must not become a reason
  // the service comes up late: five sequential round trips to a slow database
  // delayed the scheduler arming by long enough to matter.
  // Three answers, not two. A column can be present, absent, or unasked —
  // and an unreachable database gives the same silence as a healthy one.
  // Folding "could not ask" into "not missing" would print "all required
  // columns present" over a database this build never reached, which is the
  // most misleading line it could put in a deploy log.
  const probe = async ([table, column, consequence]) => {
    try {
      const { error } = await supabase.from(table).select(column).limit(1);
      if (!error) return { state: 'present' };
      if (error.code === '42703' || error.code === 'PGRST204'
          || /column .* does not exist/i.test(error.message || '')) {
        return { state: 'missing', row: [table, column, consequence] };
      }
      return { state: 'unknown', why: error.message };
    } catch (e) {
      return { state: 'unknown', why: e && e.message };
    }
  };
  const answered = await Promise.race([
    Promise.all(required.map(probe)),
    new Promise((r) => setTimeout(() => r(null), 8000)),
  ]);
  if (answered === null) {
    console.log('   Schema: not checked — the database did not answer in 8s');
    return;
  }
  const missing = answered.filter((a) => a.state === 'missing').map((a) => a.row);
  const unknown = answered.filter((a) => a.state === 'unknown');
  if (!missing.length && unknown.length) {
    console.log(`   Schema: not checked — ${unknown.length} of ${required.length} `
      + `columns could not be read (${unknown[0].why})`);
    return;
  }
  if (!missing.length) { console.log('   Schema: all required columns present'); return; }
  console.error('   🚨 SCHEMA INCOMPLETE — run schema-migration.sql before relying on this build:');
  for (const [table, column, consequence] of missing) {
    console.error(`     missing ${table}.${column} — ${consequence}`);
  }
}

// Do the Stripe prices charge what the site advertises?
//
// The amounts are not in this codebase: the engine hands Stripe a price id and
// Stripe decides the figure. Every test here can check that the pages, the
// JSON-LD offer and the upgrade email agree on £9.99 and £99.90 — none of them
// can check the only number that actually leaves a customer's account.
//
// It was written up as something only the operator could confirm, by opening
// the two price objects by hand. That was true of the tests and not of the
// engine, which holds the secret key and can simply ask. A mismatch here
// charges someone a different figure from the one they agreed to, which is the
// kind of thing worth finding at boot rather than in a chargeback.
const ADVERTISED_PRICES = [
  ['monthly', () => STRIPE_PRICE_MONTHLY, 999,  'gbp', 'month', '£9.99/mo'],
  ['annual',  () => STRIPE_PRICE_ANNUAL,  9990, 'gbp', 'year',  '£99.90/yr'],
];
async function checkStripePrices() {
  if (!STRIPE_SECRET_KEY) { console.log('   Prices: not checked — no Stripe key'); return; }
  const look = async ([label, id, pence, currency, interval, shown]) => {
    const price = await stripeRequest(`/prices/${id()}`);
    // stripeRequest returns null for a transport failure or a non-2xx, both of
    // which mean the question went unanswered. Not the same as a wrong price.
    if (!price || !price.id) return { label, state: 'unknown', shown };
    const wrong = [];
    if (price.unit_amount !== pence) {
      wrong.push(`charges ${price.unit_amount} not ${pence}`);
    }
    if (String(price.currency || '').toLowerCase() !== currency) {
      wrong.push(`is in ${price.currency}, not ${currency}`);
    }
    if (price.recurring && price.recurring.interval !== interval) {
      wrong.push(`renews every ${price.recurring.interval}, not every ${interval}`);
    }
    if (price.active === false) wrong.push('is not active');
    return { label, state: wrong.length ? 'wrong' : 'ok', wrong, shown };
  };
  const answers = await Promise.race([
    Promise.all(ADVERTISED_PRICES.map(look)),
    new Promise((r) => setTimeout(() => r(null), 8000)),
  ]);
  if (answers === null) { console.log('   Prices: not checked — Stripe did not answer in 8s'); return; }
  const wrong = answers.filter((a) => a.state === 'wrong');
  const unknown = answers.filter((a) => a.state === 'unknown');
  if (wrong.length) {
    console.error('   🚨 STRIPE PRICE MISMATCH — a customer would be charged something '
      + 'other than the price they agreed to:');
    for (const a of wrong) {
      console.error(`     ${a.label} (site shows ${a.shown}) ${a.wrong.join('; ')}`);
    }
    return;
  }
  if (unknown.length) {
    console.log(`   Prices: not checked — ${unknown.length} of ${answers.length} `
      + 'could not be read from Stripe');
    return;
  }
  console.log('   Prices: both match what the site advertises (£9.99/mo, £99.90/yr)');
}

async function checkUnsubscribeLink() {
  const probeUrl = `${SITE_URL}/unsubscribe?token=probe&uid=probe`;
  let status;
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 10000);
    try {
      status = (await fetch(probeUrl, { redirect: 'follow', signal: controller.signal })).status;
    } finally { clearTimeout(timer); }
  } catch (e) {
    console.warn(`⚠️ Could not reach the unsubscribe link (${probeUrl}): ${e.message}`);
    return null;
  }
  if (status === 403) console.log('   Unsubscribe link: reachable');
  else console.error(`🚨 UNSUBSCRIBE LINK BROKEN — ${probeUrl} answered ${status}, expected 403. `
    + `Every email already sent carries this address, and nobody can get out. `
    + `Check the /unsubscribe rewrite in the frontend's vercel.json.`);
  return status;
}

// Escape values interpolated into email HTML.
//
// Three sources reach these templates and none were escaped: team names and
// selections from the Sofascore feed, first names typed at signup with no
// validation, and the match/selection text an admin types into an email
// override. Brighton & Hove Albion alone means a bare ampersand is already
// going out in real emails — invalid HTML that most clients tolerate and some
// do not.
function esc(v) {
  return String(v == null ? '' : v)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function emailBase(content, userId) {
  // Every email this product sends recommends bets, and none of them carried
  // any of the messaging the website carries on every page: no 18+ notice, no
  // "past performance" line, no route to help. The emails are the channel the
  // advice actually arrives through, so they were the one surface where it
  // mattered most and the only one where it was absent. It goes in the shared
  // shell rather than the templates so a fifth email cannot be added without it.
  const unsubUrl = `${SITE_URL}/unsubscribe?token=${generateUnsubToken(userId)}&uid=${userId}`;
  return `<!DOCTYPE html><html lang="en-GB"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>The Tipster Edge</title></head>
<body style="margin:0;padding:0;background:#07090d;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#07090d;padding:32px 16px;">
<tr><td align="center">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;">
<tr><td style="padding-bottom:24px;">
<p style="font-family:monospace;font-size:11px;text-transform:uppercase;letter-spacing:4px;color:#18e07a;margin:0;">The Tipster Edge</p>
</td></tr>
<tr><td style="background:#0c0f15;border-radius:10px;padding:28px 24px;">${content}</td></tr>
<tr><td style="padding-top:20px;text-align:center;">
<p style="font-size:11px;color:#6c83a3;margin:0 0 10px;">© The Tipster Edge · <a href="${unsubUrl}" style="color:#6c83a3;">Unsubscribe</a></p>
<p style="font-size:10px;line-height:1.7;color:#6c83a3;margin:0;">&#9888; <strong style="color:#6c83a3;">18+ only.</strong> Tips are for informational purposes only. Past performance does not guarantee future results. Please gamble responsibly.</p>
<p style="font-size:10px;line-height:1.7;color:#6c83a3;margin:6px 0 0;"><a href="${SITE_URL}/responsible-gambling.html" style="color:#6c83a3;text-decoration:underline;">Responsible Gambling</a> · <a href="https://www.begambleaware.org" style="color:#6c83a3;text-decoration:underline;">BeGambleAware.org</a> · National Gambling Helpline 0808 8020 133</p>
</td></tr>
</table>
</td></tr>
</table></body></html>`;
}

function buildWelcomeEmail({ userId, firstName }) {
  const g = esc(firstName || 'there');
  const content = `
<p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#18e07a;margin:0 0 10px;">Welcome to Pro</p>
<h1 style="font-size:22px;font-weight:800;color:#dde6f0;margin:0 0 8px;">You're in, ${g}.</h1>
<p style="font-size:14px;color:#7a8fa6;margin:0 0 24px;">Your Pro subscription is now active. Your first tip card arrives tomorrow at 07:00 UK.</p>
<div style="text-align:center;"><a href="${SITE_URL}/#tips" style="display:inline-block;background:#f0b429;color:#07090d;font-size:13px;font-weight:700;padding:13px 32px;border-radius:5px;text-decoration:none;">View Today's Tips</a></div>`;
  return emailBase(content, userId);
}

// Emails lead with the advised price — the number settlement pays out at.
// When the market has moved since publication, show the live price beside it
// rather than quoting a figure the reader can no longer get. Rendered ONLY on
// a real move (>0.01, the same threshold saveTips treats as a change), so an
// unmoved tip — the common case — stays uncluttered.
function livePriceNote(tip, { block = false } = {}) {
  const advised = advisedPrice(tip);
  const live    = parseFloat(tip?.odds);
  if (!Number.isFinite(advised) || !Number.isFinite(live)) return '';
  if (Math.abs(live - advised) <= 0.01) return '';
  return block
    ? `<p style="font-family:monospace;font-size:10px;color:#7a8fa6;margin:3px 0 0;">now ${live.toFixed(2)}</p>`
    : ` <span style="color:#6c83a3;">· now ${live.toFixed(2)}</span>`;
}

// Stated once per email so the headline number needs no per-tip label.
const PRICE_FOOTNOTE = '<p style="font-size:10px;color:#6c83a3;margin:18px 0 0;text-align:center;line-height:1.5;">Odds shown are the prices advised at publication — the same prices our published results are settled at.</p>';

function buildProEmail({ tip, allTips, userId, firstName }) {
  const g    = esc(firstName || 'there');
  const edge = parseFloat(tip.model_edge != null ? tip.model_edge : 0).toFixed(1);
  const ec   = parseFloat(edge) >= 0 ? '#18e07a' : '#ff3d5a';
  const es   = parseFloat(edge) >= 0 ? `+${edge}%` : `${edge}%`;
  const extras = allTips.slice(1, 9).map(t => {
    const te  = parseFloat(t.model_edge != null ? t.model_edge : 0).toFixed(1);
    const tec = parseFloat(te) >= 0 ? '#18e07a' : '#ff3d5a';
    return `<tr style="border-top:1px solid #1c2535;"><td style="padding:10px 14px;">
<p style="font-size:10px;color:#6c83a3;margin:0 0 2px;font-family:monospace;text-transform:uppercase;">${esc(t.sport)} · ${esc(t.league)} · [${esc(t.tip_ref)}]</p>
<p style="font-size:13px;font-weight:700;color:#dde6f0;margin:0 0 2px;">${esc(t.home_team)} vs ${esc(t.away_team)}</p>
<p style="font-size:12px;color:#18e07a;margin:0;">${esc(t.selection)} <span style="color:#6c83a3;">@</span> <span style="color:#f0b429;font-family:monospace;">${advisedPrice(t).toFixed(2)}</span>${livePriceNote(t)}</p>
</td><td style="padding:10px 14px;text-align:right;white-space:nowrap;">
<p style="font-family:monospace;font-size:11px;color:${tec};margin:0;">${parseFloat(te)>=0?'+':''}${te}% edge</p>
<p style="font-family:monospace;font-size:11px;color:#6c83a3;margin:2px 0;">${t.confidence}% conf · ${t.stake}u</p>
</td></tr>`;
  }).join('');
  const content = `
<p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#f0b429;margin:0 0 10px;">Pro Early Access · 07:00</p>
<h1 style="font-size:20px;font-weight:800;color:#dde6f0;margin:0 0 4px;">Morning, ${g}. Here's your full card.</h1>
<p style="font-size:12px;color:#6c83a3;margin:0 0 22px;">${allTips.length} tips ready.</p>
<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#18e07a;margin:0 0 8px;">Best Pick · [${esc(tip.tip_ref)}]</p>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:16px;">
<tr><td style="padding:14px 16px;">
<p style="font-size:11px;color:#6c83a3;margin:0 0 3px;font-family:monospace;text-transform:uppercase;">${esc(tip.sport)} · ${esc(tip.league)}</p>
<p style="font-size:17px;font-weight:800;color:#dde6f0;margin:0 0 2px;">${esc(tip.home_team)} vs ${esc(tip.away_team)}</p>
<p style="font-size:14px;color:#18e07a;margin:0;">${esc(tip.selection)}</p>
</td><td style="padding:14px 16px;text-align:right;">
<p style="font-family:monospace;font-size:26px;font-weight:700;color:#f0b429;margin:0;line-height:1;">${advisedPrice(tip).toFixed(2)}</p>
${livePriceNote(tip, { block: true })}
<p style="font-family:monospace;font-size:10px;color:${ec};margin:4px 0 0;">${es} edge · ${tip.stake}u stake</p>
</td></tr></table>
${extras ? `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#0c0f15;border:1px solid #1c2535;border-radius:7px;margin-bottom:20px;">${extras}</table>` : ''}
<div style="text-align:center;"><a href="${SITE_URL}/#tips" style="display:inline-block;background:#f0b429;color:#07090d;font-size:13px;font-weight:700;padding:12px 28px;border-radius:5px;text-decoration:none;">View Full Card</a></div>${PRICE_FOOTNOTE}`;
  return emailBase(content, userId);
}

function buildFreeEmail({ tip, proTipCount, userId, firstName }) {
  const g    = esc(firstName || 'there');
  const edge = parseFloat(tip.model_edge != null ? tip.model_edge : 0).toFixed(1);
  const ec   = parseFloat(edge) >= 0 ? '#18e07a' : '#ff3d5a';
  const es   = parseFloat(edge) >= 0 ? `+${edge}%` : `${edge}%`;
  const content = `
<p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#18e07a;margin:0 0 10px;">Bet of the Day</p>
<h1 style="font-size:20px;font-weight:800;color:#dde6f0;margin:0 0 4px;">Morning, ${g}.</h1>
<p style="font-size:12px;color:#6c83a3;margin:0 0 22px;">Your daily pick · Ref: <span style="font-family:monospace;">[${esc(tip.tip_ref || '-')}]</span></p>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:16px;">
<tr><td style="padding:14px 16px;">
<p style="font-size:11px;color:#6c83a3;margin:0 0 3px;font-family:monospace;text-transform:uppercase;">${esc(tip.sport)} · ${esc(tip.league)}</p>
<p style="font-size:17px;font-weight:800;color:#dde6f0;margin:0 0 2px;">${esc(tip.home_team)} vs ${esc(tip.away_team)}</p>
<p style="font-size:14px;color:#18e07a;margin:0;">${esc(tip.selection)}</p>
</td><td style="padding:14px 16px;text-align:right;">
<p style="font-family:monospace;font-size:26px;font-weight:700;color:#f0b429;margin:0;line-height:1;">${advisedPrice(tip).toFixed(2)}</p>
${livePriceNote(tip, { block: true })}
<p style="font-family:monospace;font-size:10px;color:${ec};margin:4px 0 0;">${es} edge</p>
</td></tr></table>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#0c0f15;border:1px solid rgba(240,180,41,0.25);border-radius:7px;margin-bottom:20px;">
<tr><td style="padding:16px 18px;">
<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#f0b429;margin:0 0 8px;">Pro members got ${proTipCount} more tips at 07:00</p>
<a href="${SITE_URL}/#pricing" style="display:inline-block;background:#f0b429;color:#07090d;font-size:12px;font-weight:700;padding:9px 20px;border-radius:4px;text-decoration:none;">Go Pro — £9.99/mo</a>
</td></tr></table>
<div style="text-align:center;"><a href="${SITE_URL}/#tips" style="display:inline-block;background:#18e07a;color:#07090d;font-size:13px;font-weight:700;padding:12px 28px;border-radius:5px;text-decoration:none;">View Today's Tips</a></div>${PRICE_FOOTNOTE}`;
  return emailBase(content, userId);
}

function buildSaturdayEmail({ selections, combinedOdds, reasoning, userId }) {
  const rows = selections.map((s, i) => `<tr style="${i>0?'border-top:1px solid #1c2535;':''}"><td style="padding:11px 14px;">
<p style="font-size:12px;font-weight:700;color:#dde6f0;margin:0 0 2px;">${esc(s.match)}</p>
<p style="font-family:monospace;font-size:11px;color:#18e07a;margin:0;">${esc(s.selection)} <span style="color:#6c83a3;">@ ${parseFloat(s.odds).toFixed(2)}</span></p>
</td></tr>`).join('');
  const content = `
<p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#18e07a;margin:0 0 10px;">Weekend Accumulator</p>
<h1 style="font-size:22px;font-weight:800;color:#dde6f0;margin:0 0 6px;">Saturday's Best ${selections.length}-Fold</h1>
<p style="font-size:12px;color:#6c83a3;margin:0 0 24px;">Combined odds: <span style="font-family:monospace;font-weight:700;color:#f0b429;font-size:15px;">${parseFloat(combinedOdds).toFixed(2)}</span></p>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:20px;">${rows}</table>
<div style="text-align:center;"><a href="${SITE_URL}/#tips" style="display:inline-block;background:#18e07a;color:#07090d;font-size:13px;font-weight:700;padding:13px 28px;border-radius:5px;text-decoration:none;">View Full Weekend Card</a></div>`;
  return emailBase(content, userId);
}

async function getSubscribers(type = 'daily', tier = 'all') {
  const col = type === 'saturday' ? 'email_saturday' : 'email_daily';

  // Paginated. This was a single select, and PostgREST caps every response at
  // 1000 rows — so the moment the opted-in list passes a thousand, everyone
  // after the first thousand silently stops receiving email. Nothing surfaces:
  // the dispatch reports "sent 1000/1000", which reads as a complete run. For
  // Pro subscribers that is paid-for mail quietly not arriving.
  //
  // Returns null when the list could not be READ, versus [] when there
  // genuinely are no subscribers. The error used to be discarded, so a
  // database blip at dispatch time sent to nobody and logged "0/0" — which
  // reads as "no subscribers today" rather than "we never found out who they
  // are". Callers abort on null instead of quietly sending nothing. selectAll
  // throws on failure, so that contract is preserved here rather than letting
  // a partial list through as if it were the whole audience.
  try {
    const rows = await selectAll('users', 'id, email, first_name, subscription_status', q => {
      // `not is false`, not `eq true`, because null means opted in here.
      //
      // account.html renders these checkboxes with `email_daily !== false`, so a
      // null shows as ON — and /ensure-profile creates a profile row without
      // setting either column. Matching on `eq true` therefore skipped every
      // user whose row came from that path: their account page said they were
      // subscribed and they received nothing. Only an explicit false is an
      // opt-out, which is what the page has always claimed.
      q = q.eq('email_opt_in', true).not(col, 'is', false);
      if (tier === 'pro')  q = q.eq('subscription_status', 'pro');
      if (tier === 'free') q = q.neq('subscription_status', 'pro');
      return q;
    });

    // An audience is people who can be sent to.
    //
    // /ensure-profile takes the address from the caller's token, and a token
    // without one — phone or anonymous sign-in — creates a row with a null
    // email. Such a row passes every filter above, so the dispatcher sends to
    // it, Resend refuses, and email_log records a 'failed'. The resume check
    // only skips recipients logged 'sent', so that row is retried on every
    // dispatch from then on: a permanent failure in the count and a wasted API
    // call each time, for someone who was never reachable.
    //
    // Filtered here rather than in the query so an empty string is caught as
    // well as a null, and so the exclusion can be reported instead of quietly
    // shrinking the audience.
    const usable = (rows || []).filter(u => u && typeof u.email === 'string' && u.email.includes('@'));
    const dropped = (rows || []).length - usable.length;
    if (dropped) {
      console.warn(`⚠️ ${dropped} subscriber row(s) have no usable email address — `
        + `excluded from the ${type}/${tier} audience`);
    }
    return usable;
  } catch (err) {
    console.error(`getSubscribers(${type}/${tier}) failed:`, err.message);
    return null;
  }
}

// Sends one email per subscriber, isolating each so a single bad record cannot
// take the rest of the list with it.
//
// That was a real exposure: the loops built the HTML inline and awaited the
// send with no try/catch, so one throw — a malformed tip, an unexpected field —
// aborted the loop, and every remaining subscriber got nothing. The scheduler
// sets its per-day guard BEFORE dispatching, so there was no retry either: one
// bad row silently cost the rest of that day's list.
async function dispatchToSubscribers(label, subs, buildOne, { type = null, force = false } = {}) {
  if (subs === null) { console.error(`📧 ${label} aborted — could not read the subscriber list`); return { sent: 0, failed: 0, aborted: true }; }
  if (!subs.length)  { console.log(`📧 ${label}: no subscribers`); return { sent: 0, failed: 0, aborted: false }; }

  // Resume, do not restart.
  //
  // This used to be an all-or-nothing guard in each dispatcher: if email_log
  // held ANY 'sent' row for this type today, the entire dispatch was skipped.
  // That is right for a restart after the send finished and wrong for a
  // restart during it — and the loop below is sequential with a delay between
  // sends, so a few thousand subscribers take minutes and a restart inside
  // that is exactly the case the guard was written for. Everyone after the
  // interruption silently missed that day's email, and the log said
  // "already sent today".
  //
  // Skipping per recipient covers both cases with one mechanism: a completed
  // dispatch skips everyone, an interrupted one sends only the remainder, and
  // nobody is sent twice either way.
  let already = new Set();
  if (type && !force) {
    try {
      const rows = await selectAll('email_log', 'recipient',
        q => q.eq('type', type).eq('status', 'sent').gte('sent_at', ukDayStart().toISOString()));
      already = new Set((rows || []).map(r => r.recipient));
    } catch (e) {
      // Delivery beats duplicate-avoidance here, which is also what the old
      // guard chose: its read error returned null and the send proceeded.
      console.error(`📧 ${label}: could not read today's send log (${e.message}) — sending to everyone`);
      already = new Set();
    }
  }

  let sent = 0, failed = 0, skipped = 0;
  for (const u of subs) {
    if (already.has(u.email)) { skipped++; continue; }
    try {
      const msg = buildOne(u);
      if (await sendEmail(msg)) sent++; else failed++;
    } catch (e) {
      failed++;
      console.error(`📧 ${label} failed for ${u && u.email}:`, e && e.message);
    }
    await new Promise(r => setTimeout(r, 100));
  }
  console.log(`📧 ${label}: ${sent} sent, ${failed} failed, ${skipped} already had it, of ${subs.length}`);
  // Preserves the shape the dispatchers used to return when the old guard
  // fired, so callers and the admin dashboard read the same thing.
  if (!sent && !failed && skipped) {
    return { sent: 0, failed: 0, skipped: 'already sent today', aborted: false };
  }
  return { sent, failed, resumedPast: skipped, aborted: false };
}

async function getTodaysTips(limit = 15) {
  // Render runs in UTC; setHours anchors to the server's day, which is an hour
  // off the UK day during BST. A fixture kicking off between 00:00 and 01:00 UK
  // fell outside "today" and never made the card.
  const s = ukDayStart();
  const e = new Date(s.getTime() + 24 * 3600000 - 1);

  // Over-fetch, because the informational picks filtered out below would
  // otherwise eat into the limit — with a floor, because a multiple of the
  // limit is not enough at small limits.
  //
  // getBetOfTheDay calls this with limit 1, which fetched three rows. The
  // filter below removes stake-0 picks, and those are short-priced and
  // therefore high-confidence, so they cluster at the top of exactly this
  // ordering. Three informational picks at the head of the card — not a
  // remote case, the reasoning below says it is likelier than average —
  // emptied the result, getBetOfTheDay returned null, and the free dispatch
  // skipped for the day with "no bet of the day" while a full card of real
  // bets sat just underneath them.
  //
  // A day's card is bounded, so a flat floor covers it. The exact fix is to
  // push the predicate into the query (stake.gt.0 or stake.is.null) so the
  // limit applies to bets; that is a query change worth making the next time
  // someone can run it against the real database rather than a mock.
  // Throws rather than returning nothing. Every caller is a dispatch, and an
  // empty list is indistinguishable from a quiet day — so a blip at 07:00 sent
  // paying subscribers nothing and logged "no tips to send". runDaily releases
  // its guard when a job throws, so failing here buys the rest of the send
  // window to retry instead of writing the day off.
  const { data, error } = await supabase.from('tips').select('*').eq('status','pending')
    .gte('event_time', s.toISOString()).lte('event_time', e.toISOString())
    .order('confidence', { ascending: false }).limit(Math.max(limit * 3, 50));
  if (error) throw new Error(`getTodaysTips read failed: ${error.message}`);

  // Every caller of this function builds an email that recommends bets.
  // applyStrictRules publishes short-price selections with stake 0 and tier
  // 'insight' — explicitly not bets — but nothing downstream distinguished
  // them, so one could be emailed as the headline pick. Worse, they are
  // short-priced and therefore high-confidence, and this list is ordered by
  // confidence, so an un-staked pick was MORE likely than average to land at
  // the top and become the Bet of the Day.
  //
  // A missing stake counts as staked (defaulting to 1), matching how stakes are
  // read everywhere else; only an explicit 0 is treated as informational.
  // A bet nobody can price is not a bet. advisedPrice returns NaN for a row
  // with neither advised_odds nor odds, and every caller renders it with
  // .toFixed(2) — which puts the literal string "NaN" where the price goes, in
  // an email, which cannot be corrected once it has been sent. The four public
  // tip pages already drop these rows for exactly this reason; the dispatches
  // did not, and they are the surface where being wrong costs most.
  //
  // Not currently reachable — the engine sets a price on write, and no row in
  // the live table has a null one — so this is a guard rather than a fix for
  // something observed. It is loud because a tip going missing from a card
  // should never be silent.
  const staked = (data || []).filter(isBet).filter((t) => {
    const price = advisedPrice(t);
    if (Number.isFinite(price) && price > 1) return true;
    console.warn(`⚠️ [${t.tip_ref}] has no usable price `
      + `(advised_odds=${t.advised_odds}, odds=${t.odds}) — left off the card`);
    return false;
  });

  // A card longer than the limit is silently cut here, and every caller builds
  // an email that presents what it got as the day's card. getBetOfTheDay asks
  // for one on purpose, so only a real card-sized limit is worth reporting.
  //
  // Measured against the real table on 20 August 2026: 32 days of tips, a mean
  // of 3.4 bets a day and a maximum of 14. The Pro cap was 15, so it had never
  // bitten — and was one busy Saturday from doing so without a word.
  if (limit > 1 && staked.length > limit) {
    console.warn(`⚠️ ${staked.length} bets today but only ${limit} will be sent — `
      + `${staked.length - limit} dropped from the card`);
  }
  return staked.slice(0, limit);
}

async function getBetOfTheDay() {
  const today = ukDateString();
  // An override that cannot be read is not the same as no override, but the
  // fallback to automatic selection is the right behaviour either way — say so
  // rather than failing the dispatch.
  const { data: ov, error: ovErr } = await supabase.from('email_overrides').select('*').eq('date', today).eq('type','daily').maybeSingle();
  if (ovErr) console.error('getBetOfTheDay override read failed:', ovErr.message, '— falling back to automatic selection');
  if (ov?.bet_selection) return { home_team: ov.bet_match?.split(' vs ')[0]||'Home', away_team: ov.bet_match?.split(' vs ')[1]||'Away', selection: ov.bet_selection, odds: ov.bet_odds||1.8, confidence: ov.bet_confidence||80, stake: 1, notes: ov.bet_reasoning||'', league:'', sport:'Football', tip_ref:'OVERRIDE' };
  const tips = await getTodaysTips(1);
  return tips[0] || null;
}

// The Saturday accumulator is emailed but NOT recorded, so its result never
// enters the published record.
//
// generateDailyAcca writes its acca to daily_accas and the settler picks it up
// from there. This one is built on the fly at send time and written nowhere, so
// a bet that goes out to every Saturday subscriber has no outcome anywhere in
// the product — on a site whose copy says every result is logged.
//
// Recording it needs a schema decision rather than a code change: daily_accas
// is keyed one row per date (generateDailyAcca skips if that date already has
// one), so a Saturday acca cannot coexist with that day's daily acca without a
// type column, exactly like the (date, type) pair email_overrides needed. And
// an admin-entered override carries free text with no tip_ref at all, so those
// can never settle automatically whatever the schema does.
//
// Left as a decision. See DEPLOY.md.
async function getSaturdayAcca() {
  const today = ukDateString();
  // No override is the ordinary case, and so is the automatic email that
  // follows. But a failed read looks identical, and then an override the
  // operator set is silently not applied — worth saying, because the send
  // itself must not be held up over it.
  const { data: ov, error: ovErr } = await supabase.from('email_overrides').select('*').eq('date', today).eq('type','saturday').maybeSingle();
  if (ovErr) console.error("Saturday acca: could not read the operator's override "
    + `(${ovErr.message}) — sending the automatic selections instead`);
  if (ov?.acca_selections) return { selections: ov.acca_selections, combinedOdds: ov.acca_combined_odds||0, reasoning: ov.acca_reasoning||'' };
  const dayStart = ukDayStart();
  const s = new Date(dayStart.getTime() + 6 * 3600000);          // 06:00 UK
  const e = new Date(dayStart.getTime() + 24 * 3600000 - 1);     // 23:59:59 UK
  const { data: tips, error: tipsErr } = await supabase.from('tips').select('*').eq('status','pending').eq('sport','Football')
    .gte('event_time', s.toISOString()).lte('event_time', e.toISOString())
    .gte('confidence', 72).order('confidence', { ascending: false }).limit(40);
  // Legs have to be bets, and the fetch has to reach past the informational
  // picks to find them — see generateDailyAcca for why they crowd the top.
  if (tipsErr) console.error('Could not read candidate tips for the Saturday acca:', tipsErr.message);
  const bets = (tips || []).filter(isBet).slice(0, 4);
  if (bets.length < 3) return null;
  // tip_ref and confidence carried through, matching the shape
  // generateDailyAcca writes. The settler resolves an acca's legs by tip_ref
  // and skips any acca without them, so without this the Saturday acca could
  // not be settled even if it were recorded. It still is not recorded — see
  // the note below — but the data no longer blocks it.
  const sels = bets.map(t => ({ match: `${t.home_team} vs ${t.away_team}`, selection: t.selection,
                                odds: advisedPrice(t), tip_ref: t.tip_ref, confidence: t.confidence }));
  return { selections: sels, combinedOdds: sels.reduce((a,s) => a * parseFloat(s.odds), 1), reasoning: `${sels.length} high-confidence selections from today's card.` };
}


// force is passed through to dispatchToSubscribers, where it skips the
// already-sent check. The scheduler never forces — a restart inside its window
// must not re-send to anyone who already has the email, though it must still
// reach anyone who does not. An admin pressing the button in the dashboard
// always forces: that is a person deciding to send, usually because the first
// attempt went wrong, and silently doing nothing while reporting success is
// worse than a duplicate they asked for.
// How many tips the Pro card carries. Comfortably above the busiest day this
// engine has produced (14), so the warning in getTodaysTips stays a warning.
const PRO_CARD_MAX_TIPS = 25;

async function sendProEmails({ force = false } = {}) {
  console.log('📧 Pro dispatch 07:00...');
  const tips = await getTodaysTips(PRO_CARD_MAX_TIPS);
  if (!tips.length) return { skipped: 'no tips to send' };
  const subs = await getSubscribers('daily', 'pro');
  return await dispatchToSubscribers('Pro', subs, (u) => ({
    to: u.email,
    subject: `${u.first_name ? u.first_name + ', ' : ''}Pro Early Access | ${tips.length} tips ready`,
    html: buildProEmail({ tip: tips[0], allTips: tips, userId: u.id, firstName: u.first_name }),
    type: 'pro_daily',
    userId: u.id,
  }), { type: 'pro_daily', force });
}

async function sendDailyEmails({ force = false } = {}) {
  console.log('📧 Free dispatch 08:30...');
  const tip = await getBetOfTheDay();
  if (!tip) return { skipped: 'no bet of the day' };
  const all  = await getTodaysTips(15);
  const subs = await getSubscribers('daily', 'free');
  return await dispatchToSubscribers('Free', subs, (u) => ({
    to: u.email,
    subject: `${u.first_name ? u.first_name + ', ' : ''}Today's Bet of the Day`,
    html: buildFreeEmail({ tip, proTipCount: Math.max(all.length - 1, 0), userId: u.id, firstName: u.first_name }),
    type: 'daily',
    userId: u.id,
  }), { type: 'daily', force });
}

async function sendSaturdayEmails({ force = false } = {}) {
  console.log('📧 Saturday acca dispatch...');
  const acca = await getSaturdayAcca();
  if (!acca) return { skipped: 'no acca available' };
  const subs = await getSubscribers('saturday');
  return await dispatchToSubscribers('Saturday', subs, (u) => ({
    to: u.email,
    subject: `${u.first_name ? u.first_name + ', ' : ''}Saturday's ${acca.selections.length}-Fold | ${parseFloat(acca.combinedOdds).toFixed(2)} combined odds`,
    html: buildSaturdayEmail({ ...acca, userId: u.id }),
    type: 'saturday',
    userId: u.id,
  }), { type: 'saturday', force });
}

async function sendTestEmail(to, type) {
  if (type === 'saturday') {
    const acca = await getSaturdayAcca();
    if (!acca) return { success: false, error: 'No acca' };
    return { success: !!(await sendEmail({ to, subject: '[TEST] Saturday Acca', html: buildSaturdayEmail({ ...acca, userId: 'test' }), type: 'test' })) };
  } else if (type === 'welcome_pro') {
    return { success: !!(await sendEmail({ to, subject: '[TEST] Welcome to Pro', html: buildWelcomeEmail({ userId: 'test', firstName: 'Test' }), type: 'test' })) };
  } else if (type === 'pro_daily') {
    const tips = await getTodaysTips(15);
    if (!tips.length) return { success: false, error: 'No tips' };
    return { success: !!(await sendEmail({ to, subject: '[TEST] Pro Early Access', html: buildProEmail({ tip: tips[0], allTips: tips, userId: 'test', firstName: 'Test' }), type: 'test' })) };
  } else {
    const tip = await getBetOfTheDay();
    if (!tip) return { success: false, error: 'No tip' };
    const all = await getTodaysTips(15);
    return { success: !!(await sendEmail({ to, subject: '[TEST] Bet of the Day', html: buildFreeEmail({ tip, proTipCount: all.length-1, userId: 'test', firstName: 'Test' }), type: 'test' })) };
  }
}

// ═══════════════════════════════════════════════════════════════
// BEST BET TAGGER + DAILY ACCA
// ═══════════════════════════════════════════════════════════════

// How many tips a day are given away. Every surface already assumes three.
const FREE_TIPS_PER_DAY = 3;

// Record which tips are free ON THE ROW.
//
// The free/Pro boundary is currently positional and recomputed independently in
// six places: the engine's /tips route and the four sport pages all take the
// top three by confidence, while index.html takes indices 1-3. Because it is
// derived rather than stored, Row Level Security has no predicate for it — so
// the Pro card cannot be protected at the database level, and index.html
// currently fetches every pending tip with the public anon key and hides the
// Pro ones with a CSS overlay.
//
// This writes the boundary down. It codifies the rule five of the six surfaces
// already use, so nothing about which tips are free changes for them.
//
// Deliberately fault-tolerant: if the is_free column has not been added yet
// this warns once and returns, leaving everything else working. The column and
// the policies that use it are in rls-policies.sql.
let warnedNoIsFree = false;
async function tagFreeTips() {
  try {
    const dayStart = ukDayStart();
    const s = dayStart.toISOString();
    const e = new Date(dayStart.getTime() + 24 * 3600000 - 1).toISOString();

    const { data: tips, error: readErr } = await supabase.from('tips')
      .select('id, confidence, stake')
      .eq('status', 'pending')
      .gte('event_time', s).lte('event_time', e)
      .order('confidence', { ascending: false });
    if (readErr) { console.error('tagFreeTips read:', readErr.message); return; }
    if (!tips || !tips.length) return;

    // The free slots are bets, and this list is ordered by confidence.
    //
    // getTodaysTips already carries this filter and explains the trap: the
    // informational picks applyStrictRules publishes with stake 0 are
    // short-priced, therefore high-confidence, and therefore MORE likely than
    // average to occupy the top of a confidence-ordered list. Unfiltered, a
    // free tier of three could be three things that are explicitly not bets,
    // while every actual bet stayed locked.
    //
    // It also split the free tier in two. The free email sends
    // getBetOfTheDay(), which is the top STAKED tip, while the website locks
    // anything not tagged here — so the one tip a free subscriber was emailed
    // could render locked on the site they clicked through to.
    //
    // stake is now selected for this; the column was not even being read.
    const bets = tips.filter(isBet);
    const freeIds = bets.slice(0, FREE_TIPS_PER_DAY).map(t => t.id);
    const free = new Set(freeIds);
    // Everything else, informational picks included — they are a Pro extra,
    // not one of the three free bets.
    const proIds = tips.filter(t => !free.has(t.id)).map(t => t.id);

    if (freeIds.length) {
      const { error } = await supabase.from('tips').update({ is_free: true }).in('id', freeIds);
      if (error) {
        if (/is_free/i.test(error.message || '')) {
          if (!warnedNoIsFree) {
            warnedNoIsFree = true;
            console.warn('⚠️ tagFreeTips: tips.is_free does not exist yet — skipping. See rls-policies.sql.');
          }
        } else console.error('tagFreeTips update (free):', error.message);
        return;
      }
    }
    if (proIds.length) {
      const { error } = await supabase.from('tips').update({ is_free: false }).in('id', proIds);
      if (error) { console.error('tagFreeTips update (pro):', error.message); return; }
    }
    console.log(`🔓 Tagged ${freeIds.length} free / ${proIds.length} pro tips for today.`);
  } catch(e) { console.error('tagFreeTips error:', e.message); }
}

async function tagDailyBestBet() {
  try {
    // A UK date pasted onto 'T00:00:00Z' is not the UK day. During BST, UK
    // midnight is 23:00Z the day before, so this window started an hour late
    // and a fixture kicking off between 00:00 and 01:00 UK could never be the
    // best bet — the same drift getTodaysTips already documents as fixed.
    const dayStart = new Date(ukDayStart().getTime() + 24 * 3600000);   // tomorrow, UK
    const ukTomorrow = ukDateString(dayStart);
    const s = dayStart.toISOString();
    const e = new Date(dayStart.getTime() + 24 * 3600000 - 1).toISOString();

    // Null means "none tagged yet", and so does a failed read — but only one of
    // them should lead to tagging another. This runs again tomorrow.
    const { data: existing, error: existingErr } = await supabase.from('tips').select('id')
      .eq('is_best_bet', true).gte('event_time', s).lte('event_time', e).maybeSingle();
    if (existingErr) {
      console.error('🚨 Could not check for an existing best bet '
        + `(${existingErr.message}) — not tagging one rather than tagging a second`);
      return;
    }
    if (existing) return;

    // Fetch candidates — select model_edge and quality_score for ranking
    const { data: tips, error: tipsErr } = await supabase.from('tips')
      .select('id, tip_ref, home_team, away_team, confidence, odds, model_edge, quality_score, stake')
      .eq('status', 'pending')
      .gte('event_time', s)
      .lte('event_time', e);

    if (tipsErr) console.error('Could not read candidate tips for the best bet:', tipsErr.message);
    if (!tips?.length) return;

    // The Best Bet is a bet. Informational picks carry stake 0 and were never
    // advised, so promoting one as the single strongest selection of the day
    // is the worst place for this leak to surface. stake is now selected for
    // it; the column was not being read.
    const bets = tips.filter(isBet);
    if (!bets.length) return;

    // Rank by real edge + quality score composite (item 10)
    // 70% model_edge (normalised to 25% max) + 30% quality_score
    const ranked = bets
      .map(t => {
        const edge = parseFloat(t.model_edge || 0);
        const qs   = parseFloat(t.quality_score || 0);
        const normEdge = Math.min(1, Math.max(0, edge) / 25);
        return { ...t, composite: normEdge * 0.70 + qs * 0.30 };
      })
      .sort((a, b) => b.composite - a.composite);

    const best = ranked[0];
    const { data: tagged, error: tagErr } = await supabase.from('tips')
      .update({ is_best_bet: true }).eq('id', best.id).select('id');
    if (tagErr || !tagged || !tagged.length) {
      console.error(`⚠️ Best bet NOT tagged [${ukTomorrow}]: `
        + (tagErr ? tagErr.message : 'the write was refused, no rows changed'));
      return;
    }
    console.log(`🏆 Best bet tagged [${ukTomorrow}]: [${best.tip_ref}] ${best.home_team} vs ${best.away_team} (edge: ${best.model_edge}% qs: ${best.quality_score})`);
  } catch(e) { console.error('tagDailyBestBet error:', e.message); }
}

async function generateDailyAcca() {
  try {
    const today = ukDateString();
    // Same shape as the best bet above: a failed read reads as "none today".
    const { data: existing, error: existingErr } = await supabase.from('daily_accas').select('id').eq('date', today).maybeSingle();
    if (existingErr) {
      console.error(`🚨 Could not check for today's acca (${existingErr.message}) `
        + '— not generating one rather than generating a second');
      return { skipped: true, reason: 'existence_check_failed' };
    }
    if (existing) return { skipped: true };
    // ukDayStart, not the date pasted onto 'T00:00:00Z' — see tagDailyBestBet.
    const dayStart = ukDayStart();
    const s = dayStart.toISOString();
    const e = new Date(dayStart.getTime() + 24 * 3600000 - 1).toISOString();
    // An acca is a bet, so its legs have to be bets.
    //
    // This selects on confidence >= 84, which is exactly where the stake-0
    // informational picks live — they are short-priced, and short prices score
    // high. Unfiltered, the daily acca could be built entirely from selections
    // nobody was ever advised to back, then published with a stake of 1 and
    // settled into the record as though it were a real bet.
    //
    // Over-fetch before filtering, for the same reason getTodaysTips does: at
    // this confidence floor the informational picks come first, so a limit of
    // 5 could be filled by them and leave nothing.
    const { data: tips, error: tipsErr } = await supabase.from('tips').select('*').eq('status', 'pending').gte('event_time', s).lte('event_time', e).gte('confidence', 84).order('confidence', { ascending: false }).limit(50);
    // Not enough tips and no answer both end here. Only one of them is a fact
    // about the card.
    if (tipsErr) console.error('Could not read candidate tips for the acca:', tipsErr.message);
    const bets = (tips || []).filter(isBet);
    if (bets.length < 3) return { generated: false, reason: 'insufficient_tips' };
    const legs = bets.slice(0, 5);
    const sportCounts = legs.reduce((acc, t) => { acc[t.sport] = (acc[t.sport] || 0) + 1; return acc; }, {});
    const dominantSport = Object.entries(sportCounts).sort((a, b) => b[1] - a[1])[0][0];
    const sportLabel = Object.keys(sportCounts).length > 1 ? 'Mixed' : dominantSport;
    // Built from advised prices so the quoted acca matches what settlement pays.
    const combinedOdds = parseFloat(legs.reduce((acc, t) => acc * advisedPrice(t), 1).toFixed(4));
    const selections = legs.map(t => ({ match: `${t.home_team} vs ${t.away_team}`, selection: t.selection, odds: advisedPrice(t), tip_ref: t.tip_ref, confidence: t.confidence }));
    const { error } = await supabase.from('daily_accas').insert({ date: today, sport: sportLabel, legs: legs.length, selections, combined_odds: combinedOdds, stake: 1, result: 'pending', profit_loss: null });
    if (error) return { generated: false, error: error.message };
    console.log(`📋 Daily acca: ${legs.length} legs @ ${combinedOdds}`);
    return { generated: true, legs: legs.length, combinedOdds };
  } catch(e) { return { generated: false, error: e.message }; }
}

// ═══════════════════════════════════════════════════════════════
// ADMIN JOB QUEUE
// ═══════════════════════════════════════════════════════════════

// How long a claimed job may stay claimed before it is offered again.
//
// A job is marked 'processing' before the work starts. If the process goes away
// mid-dispatch — which it does on every deploy, since shutdown() deliberately
// does not wait for an in-flight dispatch — the row stays 'processing' and is
// never picked up again. The admin pressed the button, saw "queued", and
// nothing was ever sent.
//
// Reclaiming is safe because dispatchToSubscribers resumes per recipient:
// anyone already logged 'sent' today is skipped, so a second attempt finishes
// the list rather than mailing it twice. That also makes an early reclaim
// harmless, which matters because the age is measured from created_at — there
// is no claimed_at column to measure from, and adding one is a migration.
const ADMIN_JOB_STALE_MS = 15 * 60 * 1000;

// What a finished dispatch means for the job that asked for it.
//
// Every send job used to be marked 'done' whatever came back, including
// { skipped: 'no bet of the day' } and { sent: 0, failed: 40 }. The comment
// beside it said that recording the real outcome mattered, and then the line
// below it wrote 'done'. This is that comment, implemented.
function jobStatusFor(result) {
  if (!result || typeof result !== 'object') return 'failed';
  if (result.aborted) return 'failed';
  const sent = Number(result.sent) || 0;
  const failed = Number(result.failed) || 0;
  if (result.skipped && !sent && !failed) return 'skipped';
  if (sent && failed) return 'partial';
  if (sent) return 'done';
  if (failed) return 'failed';
  // Nothing sent, nothing failed, nothing skipped: the audience was empty.
  return 'no_recipients';
}

async function processAdminJobs() {
  try {
    // Offer stranded jobs again before looking for new ones.
    //
    // Measured from when the job was CLAIMED, not when it was queued. Keyed
    // on created_at, a job that had waited out the stale window in the queue
    // — which is what happens when the engine is restarting, the same event
    // that strands jobs in the first place — was reclaimed on the next tick,
    // thirty seconds after being picked up and while it was still running.
    // The next tick then claimed it again, and since the manual sends run
    // with force, which skips the already-sent check, the second run emails
    // the whole list a second time.
    //
    // Rows claimed before the column existed have no claimed_at, so those
    // still fall back to created_at and stay recoverable.
    const staleBefore = new Date(Date.now() - ADMIN_JOB_STALE_MS).toISOString();
    const { data: reclaimed, error: reclaimErr } = await supabase.from('admin_jobs')
      .update({ status: 'pending', claimed_at: null })
      .eq('status', 'processing')
      .or(`claimed_at.lt.${staleBefore},and(claimed_at.is.null,created_at.lt.${staleBefore})`)
      .select('id');
    if (reclaimErr) console.error('Stale job reclaim failed:', reclaimErr.message);
    else if (reclaimed && reclaimed.length) {
      console.log(`♻️ Reclaimed ${reclaimed.length} job(s) left 'processing' — retrying`);
    }

    // The error was discarded, so a failed read looked identical to an empty
    // queue and admin actions would silently never run.
    const { data: jobs, error: jobsErr } = await supabase.from('admin_jobs')
      .select('*').eq('status','pending').order('created_at',{ascending:true}).limit(10);
    if (jobsErr) { console.error('Job queue read failed:', jobsErr.message); return; }
    if (!jobs?.length) return;
    for (const job of jobs) {
      // Claim the job before doing the work, and verify the claim landed.
      //
      // This was fire-and-forget: if the update failed, the row stayed
      // 'pending' and the next tick — thirty seconds later — picked it up
      // again. Since manual sends force past the already-sent guard, that is a
      // second dispatch to the entire list. The eq('status','pending') makes
      // the claim atomic, so two overlapping ticks cannot both take it.
      const { data: claimed, error: claimErr } = await supabase.from('admin_jobs')
        .update({ status: 'processing', claimed_at: new Date().toISOString() })
        .eq('id', job.id).eq('status', 'pending').select('id');
      if (claimErr) { console.error(`Job ${job.id}: could not claim — ${claimErr.message}`); continue; }
      if (!claimed || !claimed.length) { console.log(`Job ${job.id}: already claimed elsewhere — skipping`); continue; }
      try {
        const p = JSON.parse(job.payload || '{}');
        if (job.job_type === 'test_email')        { const r = await sendTestEmail(p.to, p.type||'daily'); await supabase.from('admin_jobs').update({ status: r.success?'done':'failed', result: JSON.stringify(r) }).eq('id', job.id); }
        // force: a person pressed the button. Recording the result matters —
        // marking the job 'done' when the dispatch skipped or reached nobody
        // tells the admin it worked.
        else if (job.job_type === 'send_daily')    { const r = await sendDailyEmails({ force: true });    await supabase.from('admin_jobs').update({ status: jobStatusFor(r), result: JSON.stringify(r || {}) }).eq('id', job.id); }
        else if (job.job_type === 'send_saturday') { const r = await sendSaturdayEmails({ force: true }); await supabase.from('admin_jobs').update({ status: jobStatusFor(r), result: JSON.stringify(r || {}) }).eq('id', job.id); }
        else if (job.job_type === 'send_pro')      { const r = await sendProEmails({ force: true });      await supabase.from('admin_jobs').update({ status: jobStatusFor(r), result: JSON.stringify(r || {}) }).eq('id', job.id); }
        else { await supabase.from('admin_jobs').update({ status: 'unknown_type' }).eq('id', job.id); }
      } catch(e) { await supabase.from('admin_jobs').update({ status: 'failed', result: e.message }).eq('id', job.id); }
    }
  } catch(e) { console.error('Job queue error:', e.message); }
}

// ═══════════════════════════════════════════════════════════════
// STRIPE (unchanged from v7)
// ═══════════════════════════════════════════════════════════════

const STRIPE_SECRET_KEY      = process.env.STRIPE_SECRET_KEY      || '';
const STRIPE_WEBHOOK_SECRET  = process.env.STRIPE_WEBHOOK_SECRET  || '';
const STRIPE_PRICE_MONTHLY   = process.env.STRIPE_PRICE_MONTHLY   || 'price_1TBEpeFWJjdJlwwsgLilMcBt';
const STRIPE_PRICE_ANNUAL    = process.env.STRIPE_PRICE_ANNUAL    || 'price_1TBEqbFWJjdJlwwsTH08T82z';
// There is no STRIPE_PUBLISHABLE_KEY here, and no /stripe/config route to serve
// it from. Checkout redirects to the session URL the server creates, so the
// browser never constructs a Stripe object and never needed the key — the route
// answered every caller with an empty string and nothing in either repository
// read it. One fewer unauthenticated endpoint, and one fewer environment
// variable to wonder why is unset.

async function stripeRequest(path, method = 'GET', body = null) {
  if (!STRIPE_SECRET_KEY) return null;
  const opts = { method, headers: { 'Authorization': `Bearer ${STRIPE_SECRET_KEY}`, 'Content-Type': 'application/x-www-form-urlencoded' } };
  if (body) opts.body = new URLSearchParams(body).toString();

  // Node's fetch has no default timeout, so a hanging Stripe endpoint would
  // stall the caller indefinitely — and the callers here are request handlers
  // (/stripe/portal, /stripe/checkout, the webhook, and the /tips fallback),
  // so hung calls hold client connections open with them. sofascoreFetch has
  // had a 10s abort all along; this had none.
  //
  // Deliberately no retry, unlike sofascoreFetch. These calls are not all
  // idempotent — retrying a checkout session creation risks charging twice —
  // and every caller already treats null as "could not determine", so a single
  // clean failure is the safe outcome.
  let timeout;
  try {
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), 10000);
    opts.signal = controller.signal;
    const res = await fetch(`https://api.stripe.com/v1${path}`, opts);
    const data = await res.json();
    if (!res.ok) { console.error('Stripe error:', data.error?.message); return null; }
    return data;
  } catch(e) {
    console.error('Stripe error:', e.name === 'AbortError' ? `timeout after 10s (${path})` : e.message);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function createCheckoutSession(userId, email, priceId, plan) {
  return stripeRequest('/checkout/sessions', 'POST', {
    'mode': 'subscription', 'customer_email': email,
    'line_items[0][price]': priceId, 'line_items[0][quantity]': '1',
    'success_url': `${SITE_URL}/account.html?upgraded=1`,
    'cancel_url': `${SITE_URL}/#pricing`,
    'metadata[user_id]': userId, 'metadata[plan]': plan,
    'subscription_data[metadata][user_id]': userId,
    'allow_promotion_codes': 'true', 'billing_address_collection': 'auto',
  });
}

function verifyStripeWebhook(payload, sig) {
  if (!STRIPE_WEBHOOK_SECRET) return null;
  try {
    const parts = sig.split(',');
    const ts   = parts.find(p => p.startsWith('t=')).split('=')[1];
    const wsig = parts.find(p => p.startsWith('v1=')).split('=').slice(1).join('=');
    const exp  = crypto.createHmac('sha256', STRIPE_WEBHOOK_SECRET).update(`${ts}.${payload}`,'utf8').digest('hex');
    if (!safeEqual(exp, wsig) || Math.abs(Date.now()/1000 - parseInt(ts)) > 300) return null;
    return JSON.parse(payload);
  } catch(e) { return null; }
}

// Stripe subscription status -> the value stored on users.subscription_status.
//
// One mapping, used by every branch below. /verify-pro, /tips and the checkout
// guard all ask Stripe directly and treat trialing as Pro, but this handler
// only ever wrote 'pro' for 'active'. A trialing subscriber therefore had the
// API serving them Pro data while the site — which reads this column — showed
// them the locked free experience.
//
// null means "leave the column alone": 'incomplete' and 'paused' are transient
// and should not downgrade someone mid-flow.
function mapSubStatus(stripeStatus) {
  switch (stripeStatus) {
    case 'active':
    case 'trialing':           return 'pro';
    case 'past_due':           return 'past_due';
    case 'canceled':
    case 'unpaid':
    case 'incomplete_expired': return 'free';
    default:                   return null;
  }
}

// Updates keyed on stripe_customer_id silently match zero rows if that id has
// not been written yet — Stripe does not guarantee checkout.session.completed
// arrives before customer.subscription.updated. Surface it instead of losing
// the event without trace.
// Apply a patch to the user an event is about, trying each identifier in turn.
//
// This used to key on stripe_customer_id alone. That column is written by
// checkout.session.completed, and Stripe does not guarantee that event arrives
// first — the old comment said so and accepted the loss. What was being lost:
// subscription.deleted, which leaves someone holding Pro access they cancelled,
// and payment_failed, which leaves someone Pro on a card that declined.
//
// The other identifiers were already in the events and nothing read them.
// createCheckoutSession writes user_id into subscription_data[metadata], so
// every subscription event carries it; invoices name the subscription, and that
// id is stored at checkout.
//
// A match on a fallback key also writes the customer id, so the row heals and
// the next event for that customer matches on the first key.
// Matchers are tried in order and the first that hits any row wins, so the
// order is the semantics. A third element narrows that matcher's query —
// used to stop a broad customer-wide match from applying an event that
// belongs to a subscription the customer no longer holds.
//
// eventAt is the Stripe timestamp of the event being applied. Stripe
// guarantees delivery, not order, and retries a failed event for days, so an
// event can arrive carrying state that something later has already replaced.
// Every write therefore refuses a row that a newer event has touched, and
// stamps the row with its own time when it succeeds.
async function updateStripeUser(keys, patch, label, eventAt) {
  const tried = [];
  // lte, not lt. Stripe stamps in whole seconds and sibling events for one
  // subscription routinely share a second; strict `lt` would let whichever
  // arrived first block the rest. What this has to refuse is an event from
  // strictly earlier than the state the row already carries — a retry from
  // days ago. A true replay of the same event re-applies, which is harmless:
  // the writes are idempotent and the welcome email is separately deduped.
  const fresh = eventAt ? (q) => q.or(`stripe_event_at.is.null,stripe_event_at.lte.${eventAt}`) : null;
  const body  = eventAt ? { ...patch, stripe_event_at: eventAt } : patch;
  for (const [col, val, refine] of keys) {
    if (!val) continue;
    tried.push(`${col}=${val}` + (refine ? ' (narrowed)' : ''));
    let q = supabase.from('users').update(body).eq(col, val);
    if (refine) q = refine(q);
    if (fresh)  q = fresh(q);
    const { data, error } = await q.select('id');
    if (error) { console.error(`Stripe ${label} update failed (${col}):`, error.message); return false; }
    if (data && data.length) {
      console.log(`Stripe ${label}: matched on ${col}`);
      return true;
    }
  }
  if (!tried.length) { console.error(`Stripe ${label}: event carried nothing to identify a user by`); return false; }
  // Nothing moved. Either there is no such user, or there is one and a newer
  // event already spoke for it — worth telling apart, because the first is a
  // problem and the second is the guard doing its job.
  if (eventAt) {
    for (const [col, val] of keys) {
      if (!val) continue;
      const { data, error: newerErr } = await supabase.from('users').select('id, stripe_event_at').eq(col, val);
      if (newerErr) console.error(`Stripe ${label}: could not check for a newer event:`, newerErr.message);
      const newer = (data || []).find(r => r.stripe_event_at && r.stripe_event_at > eventAt);
      if (newer) {
        console.log(`Stripe ${label}: ignored — event is from ${eventAt}, the row was `
          + `last set by one from ${newer.stripe_event_at}`);
        return true;
      }
    }
  }
  // Retrying will not conjure a row, so report success and let the warning
  // stand rather than making Stripe redeliver for days.
  console.warn(`⚠️ Stripe ${label}: no user matched ${tried.join(' or ')} — not applied`);
  return true;
}

// Returns false when the event could not be PERSISTED. The caller turns that
// into a non-2xx so Stripe redelivers — its retry schedule runs for days and is
// the only safety net for a database that was briefly unavailable.
async function handleStripeWebhook(event) {
  console.log('Stripe:', event.type);
  // Stripe stamps every event with the second it was created. That, not the
  // order it happens to be delivered in, is what says which of two events
  // describes the later state.
  const eventAt = event.created ? new Date(event.created * 1000).toISOString() : null;
  switch(event.type) {
    case 'checkout.session.completed': {
      const s = event.data.object;
      const uid = s.metadata?.user_id;
      if (!uid) {
        // Every session this app creates carries metadata.user_id. One that
        // does not was created elsewhere — a payment link, or the dashboard —
        // and there is nothing here to upgrade. Retrying cannot help, so this
        // still answers 2xx, but it must not pass in silence: money has moved
        // and nobody has been given anything for it.
        console.error(`🚨 Stripe checkout.completed: session ${s.id} has no metadata.user_id `
          + `(customer=${s.customer}) — payment taken, NO account upgraded, needs manual review`);
        break;
      }

      // A completed session is not necessarily a paid one. For card payments
      // this is always 'paid', or 'no_payment_required' for a trial. Enabling a
      // delayed method such as Bacs or SEPA is a Stripe dashboard setting rather
      // than a code change, and under one the session completes as 'unpaid' —
      // which would have granted Pro before any money moved. The
      // invoice.payment_succeeded branch now covers subscription_create, so the
      // upgrade still lands when the payment actually clears.
      if (s.payment_status && s.payment_status !== 'paid'
          && s.payment_status !== 'no_payment_required') {
        // Record who they are, even though they are not being upgraded. The
        // invoice event that later confirms the payment is matched on exactly
        // these two columns, so leaving them null here means it finds nobody —
        // and the customer pays and is never upgraded, which is the outcome
        // this guard exists to prevent.
        // Ids only, no subscription_status — so this deliberately carries no
        // recency stamp either. It records who the customer is without
        // claiming anything about their access.
        const { error: idErr } = await supabase.from('users')
          .update({ stripe_customer_id: s.customer, stripe_subscription_id: s.subscription })
          .eq('id', uid).select('id');
        if (idErr) {
          console.error('Stripe checkout.completed: could not record ids for a pending payment:', idErr.message);
          return false;
        }
        console.log(`Stripe checkout.completed: session ${s.id} is ${s.payment_status} — `
          + `ids recorded, waiting for the invoice to be paid before upgrading`);
        break;
      }

      // Stripe delivers at least once, and answering 500 on a failed write
      // makes redelivery more likely, not less — so this has to be safe to run
      // twice. The upgrade itself is naturally idempotent; the welcome email is
      // not. Read the prior state so a replay does not greet the same customer
      // again.
      // Only used to avoid greeting an existing subscriber again, so a failed
      // read costs at worst a duplicate welcome email.
      const { data: before, error: beforeErr } = await supabase.from('users')
        .select('subscription_status').eq('id', uid).maybeSingle();
      if (beforeErr) console.error('Stripe checkout.completed: could not read the prior state:', beforeErr.message);
      const alreadyPro = before && before.subscription_status === 'pro';

      // The most consequential write in the system: someone has just paid.
      // Its result used to be discarded, and because the route answered 200
      // before running any of this, a failure here meant Stripe never retried
      // and the customer was silently never upgraded.
      // This one writes directly rather than through updateStripeUser, so it
      // carries the same recency guard by hand. A redelivered upgrade must not
      // undo a cancellation that happened in between.
      let up = supabase.from('users')
        .update({ subscription_status:'pro', stripe_customer_id: s.customer,
                  stripe_subscription_id: s.subscription,
                  ...(eventAt ? { stripe_event_at: eventAt } : {}) })
        .eq('id', uid);
      if (eventAt) up = up.or(`stripe_event_at.is.null,stripe_event_at.lte.${eventAt}`);
      const { data: upgraded, error } = await up.select('id');
      if (error) { console.error('Stripe checkout.completed upgrade failed:', error.message); return false; }
      if (!upgraded || !upgraded.length) {
        // No row moved. Distinguish a superseded replay from a missing user:
        // the first is fine and must answer 2xx, the second is money taken for
        // nothing and has to be retried and shouted about.
        const { data: who, error: whoErr } = await supabase.from('users')
          .select('id, stripe_event_at').eq('id', uid).maybeSingle();
        if (whoErr) console.error('Stripe checkout.completed: could not check for a replay:', whoErr.message);
        if (who && eventAt && who.stripe_event_at && who.stripe_event_at > eventAt) {
          console.log(`Stripe checkout.completed: ${uid} already carries a newer event `
            + `(${who.stripe_event_at}) — replay ignored`);
          break;
        }
        console.error(`Stripe checkout.completed: no user row for ${uid} — payment taken, account NOT upgraded`);
        return false;
      }

      // Only greet someone who was not already Pro. A redelivered event now
      // upgrades again harmlessly and stays quiet, instead of sending a second
      // "Welcome to Pro" to an existing subscriber.
      if (alreadyPro) {
        console.log(`Stripe checkout.completed: ${uid} was already pro — replay, no welcome email`);
        break;
      }

      // The welcome email is best-effort and deliberately not awaited into the
      // return value: a mail failure must not make Stripe redeliver the event
      // and upgrade-plus-email the customer twice.
      (async () => {
        const { data: u, error: uErr } = await supabase.from('users').select('email,first_name').eq('id', uid).single();
        if (uErr) console.error('Welcome email: could not read the recipient:', uErr.message);
        if (u) await sendEmail({ to: u.email, subject: `Welcome to The Tipster Pro, ${u.first_name||'there'}`, html: buildWelcomeEmail({ userId: uid, firstName: u.first_name }), type: 'welcome_pro' });
      })().catch(e => console.error('Welcome email failed:', e.message));
      break;
    }
    case 'customer.subscription.updated': {
      const sub = event.data.object;
      const mapped = mapSubStatus(sub.status);
      if (!mapped) break;
      // A subscription cancelled at period end stays 'active' until it lapses,
      // so access correctly continues until subscription.deleted arrives.
      const patch = { subscription_status: mapped };
      if (sub.customer) patch.stripe_customer_id = sub.customer;
      // Heal the row while passing: an event that arrives before
      // checkout.session.completed leaves the subscription id unset, and until
      // it is set every later event has to fall back to matching the customer.
      if (mapped === 'free') patch.stripe_subscription_id = null;
      else if (sub.id)      patch.stripe_subscription_id = sub.id;
      // The subscription this event is about, before the customer who owns it:
      // a customer can hold a newer subscription than the one that fired, and a
      // redelivery days later must not be applied to it. The broader matchers
      // stay for the window before checkout.session.completed has recorded an
      // id at all, and are narrowed to exactly that window.
      if (!await updateStripeUser(
            [['stripe_subscription_id', sub.id],
             ['stripe_customer_id', sub.customer, q => q.is('stripe_subscription_id', null)],
             ['id', sub.metadata?.user_id, q => q.is('stripe_subscription_id', null)]],
            patch, `subscription.updated(${sub.status})`, eventAt)) return false;
      break;
    }
    case 'customer.subscription.deleted': {
      const sub = event.data.object;
      // This one had the exact matcher listed last, which meant it was never
      // reached: the customer-wide match above it always hit first. A
      // redelivered deletion for a cancelled subscription therefore downgraded
      // whatever subscription the customer holds now — and nulled the id, so
      // the live subscription's own invoices could no longer be matched either.
      if (!await updateStripeUser(
            [['stripe_subscription_id', sub.id],
             ['stripe_customer_id', sub.customer, q => q.is('stripe_subscription_id', null)],
             ['id', sub.metadata?.user_id, q => q.is('stripe_subscription_id', null)]],
            { subscription_status:'free', stripe_subscription_id: null },
            'subscription.deleted', eventAt)) return false;
      break;
    }
    case 'invoice.payment_failed': {
      const inv = event.data.object;
      const patch = { subscription_status:'past_due' };
      if (inv.customer)     patch.stripe_customer_id = inv.customer;
      if (inv.subscription) patch.stripe_subscription_id = inv.subscription;
      // An invoice carries no subscription metadata of its own, but it does
      // name the subscription, and that id is stored at checkout.
      if (!await updateStripeUser(
            [['stripe_subscription_id', inv.subscription],
             ['stripe_customer_id', inv.customer, q => q.is('stripe_subscription_id', null)]],
            patch, 'payment_failed', eventAt)) return false;
      break;
    }
    case 'invoice.payment_succeeded': {
      const inv = event.data.object;
      // subscription_create as well as subscription_cycle. The first invoice is
      // normally settled before checkout.session.completed fires, so this is
      // redundant for card payments — but under a delayed payment method the
      // session completes unpaid, and this is the event that confirms the money
      // moved. Upgrading on a paid subscription invoice is right in both cases,
      // and idempotent when it is the second thing to say so.
      if (inv.billing_reason !== 'subscription_cycle'
          && inv.billing_reason !== 'subscription_create') break;
      const patch = { subscription_status:'pro' };
      if (inv.customer)     patch.stripe_customer_id = inv.customer;
      if (inv.subscription) patch.stripe_subscription_id = inv.subscription;
      // Same ordering as the rest: the subscription the invoice names, then the
      // customer only while no subscription id has been recorded. Under a
      // delayed payment method checkout.session.completed has already stored
      // the id, so the exact match is the one that fires.
      if (!await updateStripeUser(
            [['stripe_subscription_id', inv.subscription],
             ['stripe_customer_id', inv.customer, q => q.is('stripe_subscription_id', null)]],
            patch, `payment_succeeded(${inv.billing_reason})`, eventAt)) return false;
      break;
    }
  }
  return true;
}

// ═══════════════════════════════════════════════════════════════
// SCHEDULER
// ═══════════════════════════════════════════════════════════════

// One entry per scheduled job, keyed by the UK date it last ran on.
//
// These were six separate variables, each assigned immediately before its
// await. Taking the guard first is right — a five-minute window is five ticks,
// and without it the job would start five times. But nothing released the
// guard when the work threw, so a single transient failure cost the whole day
// silently, and the rejection escaped the interval callback entirely because
// nothing awaited it.
const lastRun = {};

// Runs fn at most once per UK day. Takes the guard before the work so
// overlapping ticks cannot start it again, and releases it if the work throws,
// so the rest of the window is available to retry rather than the day being
// written off.
async function runDaily(name, today, fn) {
  if (lastRun[name] === today) return;
  lastRun[name] = today;
  try {
    await fn();
  } catch (e) {
    lastRun[name] = '';
    console.error(`❌ Scheduled job "${name}" failed:`, e.message,
                  '— guard released, will retry while the window is open');
  }
}

function startScheduler() {
  setInterval(async () => {
    try {
    const uk    = ukTime();
    const h     = uk.getHours();
    const m     = uk.getMinutes();
    const today = uk.toDateString();
    const isSat = uk.getDay() === 6;

    // Morning fetch: 06:00 UK — full data pull
    if (h === 6 && m < 5) await runDaily('morning', today, async () => {
      await morningFetch();
      await tagDailyBestBet();
    });

    // Pro emails + acca: 07:00
    if (h === 7 && m < 5) await runDaily('pro', today, async () => {
      await tagDailyBestBet();
      await sendProEmails();
      await generateDailyAcca();
    });

    // Free emails: 08:30
    if (h === 8 && m >= 30 && m < 35) await runDaily('free', today, async () => {
      await sendDailyEmails();
    });

    // Saturday acca: 08:00 Sat
    if (isSat && h === 8 && m < 5) await runDaily('saturday', today, async () => {
      await sendSaturdayEmails();
    });

    // Midday odds refresh: 13:00
    if (h === 13 && m < 5) await runDaily('midday', today, async () => {
      await middayOddsRefresh();
    });

    // Evening goalie + lineup refresh: 21:00 UK
    // NHL starters confirmed ~4pm ET = 9pm UK, football lineups confirmed ~1-2hr pre-kickoff
    if (h === 21 && m < 5) await runDaily('evening', today, async () => {
      console.log('🥅 Evening goalie + lineup refresh...');
      Object.keys(nhlGoalieCache).forEach(k => delete nhlGoalieCache[k]);
      nhlGoalieCacheDate = '';
      await fetchNHLGoalieData();
      await fetchLineupsForToday();
    });

    } catch (e) {
      // Nothing awaits this callback, so anything escaping it becomes an
      // unhandled rejection rather than a logged fault.
      console.error('Scheduler tick failed:', e.message);
    }
  }, 60 * 1000);

  setInterval(() => settleResults().catch(e => console.error('Scheduled settle error:', e.message)), 60 * 60 * 1000);

  console.log('⏰ Scheduler active:');
  console.log('   06:00 UK — Morning data fetch (fixtures + odds + form + injuries + H2H)');
  console.log('   07:00 UK — Pro emails + daily acca');
  console.log('   08:00 UK Sat — Saturday acca');
  console.log('   08:30 UK — Free emails');
  console.log('   13:00 UK — Midday odds refresh');
  console.log('   21:00 UK — Goalie + lineup refresh (starters confirmed)');
  console.log('   Every 60 min — Settler');
  console.log('   Every 15 min — Tip generation (cache only)');
}

// ═══════════════════════════════════════════════════════════════
// HTTP SERVER
// ═══════════════════════════════════════════════════════════════

const http = require('http');

const rateLimitMap = new Map();
const RATE_LIMIT   = 60;
const RATE_WINDOW  = 60 * 1000;

// The Stripe webhook gets its own allowance.
//
// The limiter is keyed on client address, and this route's caller is Stripe.
// Its authentication is the HMAC signature, not the address, so limiting it by
// address buys no security — a request without the secret is already refused
// with a 400. What it can do is drop payment events: Stripe replays a backlog
// after an outage, and a replay of more than sixty in a minute would meet a 429.
// Those are retried rather than lost, but a subscription sits in the wrong state
// meanwhile, and the retry schedule runs for days.
//
// Not exempt, because an unlimited endpoint is an unlimited endpoint whatever
// guards its contents — every request still costs a body read and an HMAC.
// Raised instead, which keeps the ceiling and moves it above anything Stripe
// would plausibly send.
const RATE_LIMIT_WEBHOOK = 300;
function limitForPath(pathname) {
  return pathname === '/stripe/webhook' ? RATE_LIMIT_WEBHOOK : RATE_LIMIT;
}

// The last time the expired entries were swept out.
let rateSweptAt = 0;

// Resolve the client address for rate limiting.
//
// x-forwarded-for is a list that each proxy appends its observed peer to, so
// the LEFTMOST entry is whatever the client itself sent. Keying the limiter on
// it — which is what `split(',')[0]` did — meant anyone could bypass the limit
// entirely by varying a header on every request, so the protection was
// decorative.
//
// Count back from the right instead. With one trusted proxy in front, the last
// entry is the address that proxy actually observed, and everything to its left
// is unverifiable. TRUSTED_PROXY_HOPS covers the case of another layer (a CDN,
// say) being added in front later; get it wrong in the low direction and you
// over-limit rather than under-limit, which is the safer way to be wrong.
const TRUSTED_PROXY_HOPS = Math.max(1, parseInt(process.env.TRUSTED_PROXY_HOPS || '1', 10) || 1);

function clientIpFrom(req) {
  const xff = String(req.headers['x-forwarded-for'] || '')
    .split(',').map(v => v.trim()).filter(Boolean);
  if (xff.length) return xff[Math.max(0, xff.length - TRUSTED_PROXY_HOPS)];
  return req.socket?.remoteAddress || '';
}

function isRateLimited(ip, limit = RATE_LIMIT) {
  const now   = Date.now();
  const entry = rateLimitMap.get(ip) || { count: 0, start: now };
  if (now - entry.start > RATE_WINDOW) { entry.count = 1; entry.start = now; }
  else entry.count++;
  rateLimitMap.set(ip, entry);

  // Sweep at most once a window, rather than on every request once the map is
  // over a thousand entries. The old condition swept whenever the map was
  // large, and a spread of more than a thousand active addresses inside one
  // window leaves nothing to delete — so it walked the whole map on every
  // request and deleted nothing, turning the limiter itself into the cost.
  // Once a window is enough: entries are only removable after they expire.
  if (rateLimitMap.size > 1000 && now - rateSweptAt > RATE_WINDOW) {
    rateSweptAt = now;
    for (const [key, val] of rateLimitMap) {
      if (now - val.start > RATE_WINDOW) rateLimitMap.delete(key);
    }
  }
  return entry.count > limit;
}

// Resolve the caller from their Supabase JWT. Returns null if unauthenticated.
// Any endpoint acting on a specific user's data MUST go through this rather
// than trusting an ID supplied in the request body.
async function authedUser(req) {
  const header = req.headers['authorization'] || '';
  if (!header.startsWith('Bearer ')) return null;
  const token = header.slice(7).trim();
  if (!token) return null;
  try {
    const { data, error } = await supabase.auth.getUser(token);
    if (error || !data?.user) return null;
    return data.user;
  } catch(e) { return null; }
}

// Read a request body with a hard size cap. Returns a Buffer so multibyte
// UTF-8 split across chunk boundaries is not corrupted (which would break
// Stripe signature verification non-deterministically).
function readBody(req, maxBytes = 1048576) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', c => {
      size += c.length;
      if (size > maxBytes) {
        // Pause rather than destroy. req.destroy() tore the socket down before
        // the caller could write its 413, so an oversized body got a reset
        // connection and no status line at all — verified by probing the route
        // with a 20KB payload against the 16KB cap. Pausing stops us buffering
        // any more while leaving the response writable.
        req.pause();
        const err = new Error('Body too large');
        err.code = 'BODY_TOO_LARGE';
        reject(err);
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

// The request handler is deliberately a SYNCHRONOUS function wrapping an async
// IIFE with a .catch. It used to be `async (req, res) => {...}` directly, which
// meant any throw inside it became an unhandled promise rejection — and Node
// terminates the process on those by default.
//
// That was not theoretical. `new URL('//', ...)` throws TypeError: Invalid URL,
// and it was the first statement in the handler, so `GET //` killed the engine
// outright. An unauthenticated one-line request took the whole service down,
// and every restart re-ran morningFetch and spent RapidAPI quota.
const server = http.createServer((req, res) => { (async () => {
  // A malformed path is a client error, not a server fault.
  let url;
  try {
    url = new URL(req.url, 'http://localhost');
  } catch (e) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Malformed request URL' }));
    return;
  }
  const origin = req.headers['origin'] || '';
  const allowedOrigins = ['https://www.thetipsteredge.com', 'https://thetipsteredge.com', 'https://the-tipster.vercel.app'];
  const allowOrigin = allowedOrigins.includes(origin) ? origin : allowedOrigins[0];
  const cors = { 'Access-Control-Allow-Origin': allowOrigin, 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type, Authorization', 'Vary': 'Origin' };

  if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return; }

  const clientIp = clientIpFrom(req);
  if (isRateLimited(clientIp, limitForPath(url.pathname))) {
    res.writeHead(429, { ...cors, 'Content-Type': 'application/json', 'Retry-After': '60' });
    res.end(JSON.stringify({ error: 'Too many requests' }));
    return;
  }

  if (url.pathname === '/') {
    res.writeHead(200, { ...cors, 'Content-Type': 'text/plain' });
    res.end(`The Tipster Engine v9.9 | Cache: ${sofascoreCache.fetchedDate || 'not fetched'} | API calls today: ${rapidApiCallCount}`);
    return;
  }

  const adminKey = (req.headers['authorization'] || '').replace('Bearer ', '').trim();

  if (url.pathname === '/admin/morning-fetch') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    morningFetch().catch(e => console.error('Manual morning fetch error:', e.message));
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true })); return;
  }

  if (url.pathname === '/admin/midday-refresh') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    middayOddsRefresh().catch(e => console.error('Manual midday refresh error:', e.message));
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true })); return;
  }

  if (url.pathname === '/admin/generate-acca') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    const result = await generateDailyAcca();
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true, ...result })); return;
  }

  // Set a subscriber's plan.
  //
  // The dashboard used to write users.subscription_status straight from the
  // browser, with the admin's own login. That only worked because every
  // signed-in user could write any column of their own row — and the same
  // grant let ANY user set their own status to 'pro' and take the Pro card for
  // free, since the tips policy decides entitlement by reading exactly that
  // column. rls-policies.sql now caps what a browser may write to first_name,
  // the email preferences and last_login, which closes that and takes this
  // with it. A privileged write belongs behind the service role anyway.
  //
  // Worth knowing before relying on it: Stripe owns this column. A value set
  // here is overwritten the next time /verify-pro runs for that user — which
  // their account page does on load — so this comps an account until they next
  // visit it, not indefinitely.
  if (url.pathname === '/admin/set-plan' && req.method === 'POST') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    (async () => {
      try {
        let body = {};
        try { body = JSON.parse((await readBody(req)).toString('utf8') || '{}'); }
        catch (_) { res.writeHead(400, { ...cors, 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ ok: false, error: 'Malformed body' })); return; }

        const email = String(body.email || '').trim().toLowerCase();
        const plan  = String(body.plan || '').trim();
        if (!email) {
          res.writeHead(400, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'An email address is required' })); return;
        }
        // Only the two values the column is allowed to hold from here. past_due
        // is Stripe's to set, never an operator's.
        if (plan !== 'pro' && plan !== 'free') {
          res.writeHead(400, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: `Plan must be "pro" or "free", not "${plan}"` })); return;
        }

        // updateChecked selects back, so a mistyped address reports "no user"
        // rather than silently succeeding — the same trap the dashboard's own
        // .select() was added for.
        const wrote = await updateChecked('users', { subscription_status: plan },
                                          q => q.eq('email', email));
        if (!wrote.ok) {
          console.error(`Admin set-plan failed for ${email}:`, wrote.why);
          res.writeHead(404, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: wrote.why })); return;
        }
        console.log(`Admin set ${email} to ${plan}`);
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, email, plan, rows: wrote.rows }));
      } catch (e) {
        console.error('Admin set-plan error:', e.message);
        res.writeHead(500, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'Server error' }));
      }
    })();
    return;
  }

  if (url.pathname === '/admin/test-email') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    const r = await sendTestEmail(url.searchParams.get('to'), url.searchParams.get('type')||'daily');
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify(r)); return;
  }

  // The Pro card is the paid product and had no manual trigger at all — no
  // route, no job type, no button. A failed 07:00 dispatch meant paying
  // subscribers got nothing until the following day, with no way to recover.
  if (url.pathname === '/admin/send-pro') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    const r = await sendProEmails({ force: true }).catch(e => ({ error: e.message }));
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true, ...(r || {}) })); return;
  }

  if (url.pathname === '/admin/send-daily') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    // Awaited and reported: this used to be fire-and-forget, so a rejection
    // became an unhandled rejection and the caller was told it started
    // regardless of what happened.
    const r = await sendDailyEmails({ force: true }).catch(e => ({ error: e.message }));
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true, ...(r || {}) })); return;
  }

  if (url.pathname === '/admin/send-saturday') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    const r = await sendSaturdayEmails({ force: true }).catch(e => ({ error: e.message }));
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true, ...(r || {}) })); return;
  }

  if (url.pathname === '/admin/resettle') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    // Runs the settler on demand: grades anything pending and repairs any
    // graded tip missing its results_history row. A full settle can run for
    // minutes behind the Sofascore fallbacks, so it is dispatched in the
    // background — awaiting it here hits the platform request timeout and
    // reports a failure for a job that actually succeeded. Counts go to the
    // logs, matching how the other long-running admin routes behave.
    if (settleInFlight) {
      res.writeHead(409, { ...cors, 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Settle already in progress' })); return;
    }
    settleResults()
      .then(r => console.log(`🔁 Manual resettle finished — settled ${r.settled}, backfilled ${r.backfilled}.`))
      .catch(e => console.error('Manual resettle error:', e.message));
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true })); return;
  }

  if (url.pathname === '/admin/cache-status') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    const status = {};
    for (const sport of SPORTS) {
      status[sport.league] = (sofascoreCache.events[sport.key] || []).length;
    }
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ fetchedDate: sofascoreCache.fetchedDate, oddsFetchedAt: sofascoreCache.oddsFetchedAt, apiCallsToday: rapidApiCallCount, events: status }));
    return;
  }

  if (url.pathname === '/unsubscribe') {
    const token = url.searchParams.get('token');
    const uid   = url.searchParams.get('uid');
    if (!token || !uid) { res.writeHead(400); res.end('Invalid'); return; }
    try {
      if (!verifyUnsubToken(token, uid)) { res.writeHead(403); res.end('Invalid token'); return; }

      // A GET must not change anything. Mail security scanners and link
      // prefetchers — Outlook Safe Links, corporate filters, some mobile
      // clients — fetch every URL in a message, so a GET that opted someone
      // out unsubscribed people who never clicked, and did it silently.
      //
      // GET therefore confirms, POST acts. The POST form posts back to the
      // same address, so it stays on the branded domain, and the same shape
      // serves RFC 8058 one-click: the signed token in the query string is the
      // authentication, so no session or form field is needed.
      if (req.method !== 'POST') {
        const action = `/unsubscribe?token=${encodeURIComponent(token)}&uid=${encodeURIComponent(uid)}`;
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end('<!DOCTYPE html><html lang="en-GB"><head><meta charset="utf-8"><meta name="robots" content="noindex"><meta name="viewport" content="width=device-width, initial-scale=1.0">'
          + '<title>Unsubscribe | The Tipster Edge</title></head>'
          + '<body style="font-family:system-ui,-apple-system,sans-serif;text-align:center;padding:60px 20px;background:#07090d;color:#dde6f0;">'
          + '<h1 style="font-size:22px;font-weight:800;">Unsubscribe</h1>'
          + '<p style="color:#6c83a3;font-size:14px;max-width:420px;margin:0 auto 22px;">Stop receiving tips emails from The Tipster Edge? You can re-enable them any time from your account.</p>'
          + `<form method="POST" action="${esc(action)}" style="margin:0;">`
          + '<button type="submit" style="background:#18e07a;color:#07090d;border:0;font-size:14px;font-weight:700;padding:12px 28px;border-radius:5px;cursor:pointer;">Yes, unsubscribe me</button>'
          + '</form>'
          + '<p style="margin-top:22px;"><a href="https://www.thetipsteredge.com/account.html" style="color:#6c83a3;font-size:13px;">Manage preferences instead</a></p>'
          + '</body></html>');
        return;
      }

      // The result used to be discarded, so a failed write still rendered
      // "You have been removed from all emails." Telling someone they are
      // unsubscribed while they remain opted in is the one failure here that
      // actually matters — they will keep receiving mail they have explicitly
      // refused, and will have no reason to try again.
      const { data: optedOut, error: unsubErr } = await supabase.from('users')
        .update({ email_opt_in: false }).eq('id', uid).select('id');
      if (unsubErr || !optedOut || !optedOut.length) {
        console.error('Unsubscribe failed for', uid, unsubErr ? unsubErr.message : 'no matching user');
        res.writeHead(500, { 'Content-Type': 'text/html' });
        res.end('<!DOCTYPE html><html lang="en-GB"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex"><title>Unsubscribe | The Tipster Edge</title></head><body style="font-family:sans-serif;text-align:center;padding:60px;background:#07090d;color:#dde6f0;"><h1 style="font-size:22px;font-weight:800;">Something went wrong</h1><p>We could not update your preferences just now. Please email <a href="mailto:support@thetipsteredge.com" style="color:#18e07a;">support@thetipsteredge.com</a> and we will remove you manually.</p></body></html>');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end('<!DOCTYPE html><html lang="en-GB"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex"><title>Unsubscribed | The Tipster Edge</title></head><body style="font-family:sans-serif;text-align:center;padding:60px;background:#07090d;color:#dde6f0;"><h1 style="font-size:22px;font-weight:800;">Unsubscribed</h1><p>You have been removed from all emails.</p><a href="https://www.thetipsteredge.com/account.html" style="color:#18e07a;">Manage preferences</a></body></html>');
    } catch(e) { res.writeHead(400); res.end('Invalid token'); }
    return;
  }

  if (url.pathname === '/tips' && req.method === 'GET') {
    try {
      const authHeader = req.headers['authorization'] || '';
      const token = authHeader.replace('Bearer ', '').trim();
      let isPro = false;
      if (token) {
        const { data: { user }, error: authErr } = await supabase.auth.getUser(token);
        if (!authErr && user) {
          // The error was discarded, so a failed profile read left isPro false
          // and served a paying subscriber the free card — the same wrong
          // denial the fallback below exists to prevent, arriving through a
          // different door. There is no way to tell whether they are Pro
          // without this row, so say so rather than presenting the free card
          // as their entitlement. PGRST116 is "no row", which is a real state
          // and means genuinely not Pro.
          const { data: profile, error: profileErr } =
            await supabase.from('users').select('subscription_status, stripe_subscription_id').eq('id', user.id).single();
          if (profileErr && profileErr.code !== 'PGRST116') {
            console.error('/tips profile read failed:', profileErr.message);
            res.writeHead(503, { ...cors, 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Could not confirm your subscription', tips: [], isPro: false }));
            return;
          }

          // subscription_status is what the Stripe webhook maintains — both
          // 'active' and 'trialing' map to 'pro' — so it is the fast path: one
          // indexed read instead of a Stripe round trip on every page load of
          // the busiest endpoint on the site.
          //
          // It was previously selected and then ignored: isPro was only ever
          // set inside the stripe_subscription_id branch, so anyone marked pro
          // without a subscription id on file — set by hand, or after the id
          // was cleared on a lapse and they resubscribed — was served the free
          // card despite the column saying otherwise.
          isPro = profile?.subscription_status === 'pro';

          // Fall back to a live check only when the column says NOT pro but a
          // subscription exists. That covers a missed upgrade webhook, which is
          // the failure that wrongly denies someone who has paid. The opposite
          // staleness costs nothing and self-heals, since Stripe retries
          // webhook delivery for days.
          if (!isPro && profile?.stripe_subscription_id) {
            const sub = await stripeRequest(`/subscriptions/${profile.stripe_subscription_id}`);
            if (sub && (sub.status === 'active' || sub.status === 'trialing')) isPro = true;
          }
        }
      }
      const today = ukDayStart();
      const tom = new Date(today.getTime() + 3 * 24 * 3600000);
      const { data: allTips, error: tipsErr } = await supabase.from('tips').select('*').gte('event_time', today.toISOString()).lte('event_time', tom.toISOString()).eq('status', 'pending').order('confidence', { ascending: false }).limit(50);

      // A failed query is not the same as a quiet day. The error used to be
      // discarded, so `allTips || []` turned a database outage into a cheerful
      // 200 with an empty list — and the site renders that as "No tips yet",
      // identical to a day with no fixtures.
      //
      // That matters most for a misconfigured Row Level Security policy, which
      // presents exactly this way: tips silently vanish and the engine looks
      // broken. Say so instead, so the frontend can distinguish "nothing on"
      // from "we cannot reach the data".
      if (tipsErr) {
        console.error('/tips query failed:', tipsErr.message);
        res.writeHead(503, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Tips temporarily unavailable', tips: [], isPro })); return;
      }

      // Which tips are free is the engine's decision, recorded on the row by
      // tagFreeTips — not "the first three by confidence". Those agree only by
      // coincidence: this route takes a three-day window while tagFreeTips
      // works a single UK day, so a high-confidence fixture tomorrow could
      // take a free slot from one of today's. index.html and the six Vercel
      // pages already gate on the flag; this was the last place deriving it
      // independently.
      //
      // Falls back to position while the column is unmigrated, which is still
      // the case in production.
      const rows = allTips || [];
      const tagged = rows.some(t => t.is_free != null);
      const tips = rows.map((tip, i) => {
        const isLocked = !isPro && (tagged ? tip.is_free !== true : i >= 3);
        if (isLocked) return { tip_ref: tip.tip_ref, sport: tip.sport, league: tip.league, home_team: tip.home_team, away_team: tip.away_team, event_time: tip.event_time, tier: tip.tier, locked: true };
        return { ...tip, locked: false };
      });
      res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ tips, isPro })); return;
    } catch(e) {
      console.error('Tips endpoint error:', e.message);
      res.writeHead(500, cors); res.end('Server error'); return;
    }
  }

  // Nothing in either codebase creates the users row. Signup calls
  // supabase.auth.signUp, which writes to auth.users, and the public.users row
  // is assumed to appear via a database trigger. If that trigger is absent — or
  // ever fails — the subscriber has an auth record and no profile: getSubscribers
  // never sees them, so they get no emails, and subscription_status has nowhere
  // to live, so they can never resolve as Pro even after paying.
  //
  // This closes that by construction rather than by assumption. It runs with the
  // service role, so it works whatever the RLS policy is, and it is idempotent —
  // harmless if a trigger already created the row.
  if (url.pathname === '/ensure-profile' && req.method === 'POST') {
    (async () => {
      try {
        const caller = await authedUser(req);
        if (!caller) { res.writeHead(401, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }

        const { data: existing, error: existingErr } = await supabase.from('users').select('id').eq('id', caller.id).maybeSingle();
        if (existingErr) {
          // Falling through would insert on a primary key that may already
          // exist. Say the read failed; the caller retries on the next visit.
          console.error('ensure-profile: could not check for an existing row:', existingErr.message);
          res.writeHead(503, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Could not check the account, try again shortly' })); return;
        }
        if (existing) {
          res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, created: false })); return;
        }

        const { error } = await supabase.from('users').insert({
          id:                  caller.id,
          email:               caller.email,
          first_name:          caller.user_metadata?.first_name || null,
          subscription_status: 'free',
          email_opt_in:        true,
          // Set explicitly. Left unset these are null, which every reader has
          // to agree to interpret — and for a while they did not.
          email_daily:         true,
          email_saturday:      true,
        });
        // 23505 means a trigger won the race — the row exists, which is the goal.
        if (error && error.code !== '23505') {
          console.error('ensure-profile insert failed:', error.message);
          res.writeHead(500, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Could not create profile' })); return;
        }
        if (!error) console.log(`👤 Created missing profile row for ${caller.email}`);
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, created: !error }));
      } catch(e) {
        console.error('ensure-profile error:', e.message);
        res.writeHead(500, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Server error' }));
      }
    })();
    return;
  }

  if (url.pathname === '/verify-pro' && req.method === 'POST') {
    (async () => {
      try {
        // The caller's identity comes from their JWT, never from the body.
        // Previously any unauthenticated request could probe — and downgrade —
        // an arbitrary account by guessing its user id.
        const caller = await authedUser(req);
        if (!caller) { res.writeHead(401, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
        const userId = caller.id;
        const { data: user, error: userErr } = await supabase.from('users').select('stripe_customer_id, stripe_subscription_id, subscription_status').eq('id', userId).single();
        // A failed read used to fall into the line below and answer
        // { isPro: false, verified: true } — which tells a paying subscriber,
        // with the word "verified" attached, that they are not one. Decline to
        // answer instead; the caller can ask again.
        if (userErr && userErr.code !== 'PGRST116') {
          console.error('verify-subscription: could not read the account:', userErr.message);
          res.writeHead(503, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Could not check the subscription, try again shortly' })); return;
        }
        if (!user?.stripe_subscription_id) { res.writeHead(200, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ isPro: false, verified: true })); return; }

        const sub = await stripeRequest(`/subscriptions/${user.stripe_subscription_id}`);

        // No answer is not the same as a negative answer.
        //
        // stripeRequest returns null for a timeout, a 5xx, a 4xx and a missing
        // API key alike, and this used to read that as "not active" and write
        // the account down to free. A Stripe blip, or a key that had not been
        // set, downgraded a paying subscriber — and /stripe/portal, which calls
        // the same helper, already answers 502 in exactly this case rather than
        // acting on it.
        //
        // Report what is on record, say it was not verified, and change nothing.
        // A genuinely cancelled subscription still reaches us through
        // customer.subscription.deleted, which is the path that should be
        // trusted for it.
        if (!sub || !sub.status) {
          res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ isPro: user.subscription_status === 'pro', verified: false }));
          return;
        }

        // Through the same mapping the webhook uses, rather than a flat 'free'.
        // past_due is not free: the checkout guard counts it as a live
        // subscription and refuses a second one, so writing those accounts down
        // to free here would let a subscriber whose card had failed start a
        // second subscription alongside the first. null means transient —
        // incomplete, paused — and leaves the column alone.
        const mapped = mapSubStatus(sub.status);
        const isPro = mapped === 'pro';
        // An update Supabase REFUSES returns no error and no rows, so the
        // result has to be read rather than assumed — and this answer is not
        // just a status, it is the account page's instruction to overwrite what
        // it is showing. Answering `verified: true` over a write that did not
        // land shows the reader a subscription state that reverts on their next
        // reload, and they cannot tell which of the two visits was lying.
        //
        // `verified: false` is the existing contract for "could not confirm,
        // change nothing", so a failed write uses it. The check against Stripe
        // did succeed; what failed is recording it, and the honest thing is to
        // leave the reader looking at what is on record.
        if (mapped && mapped !== user.subscription_status) {
          const { data: wrote, error: writeErr } = await supabase.from('users')
            .update({ subscription_status: mapped }).eq('id', userId).select('id');
          if (writeErr || !wrote || !wrote.length) {
            console.error(`🚨 /verify-pro could not record ${mapped} for ${userId}: `
              + (writeErr ? writeErr.message : 'the write was refused, no rows changed'));
            res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
              isPro: user.subscription_status === 'pro',
              verified: false,
              subscriptionStatus: sub.status,
            }));
            return;
          }
        }
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ isPro, verified: true, subscriptionStatus: sub.status }));
      } catch(e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: 'Internal error' })); }
    })();
    return;
  }

  if (url.pathname === '/stripe/webhook' && req.method === 'POST') {
    (async () => {
      let raw;
      try { raw = await readBody(req); }
      catch(e) { res.writeHead(413); res.end('Payload too large'); return; }
      // Verify against the exact bytes received — re-encoding breaks the HMAC.
      const body = raw.toString('utf8');
      const sig = req.headers['stripe-signature'];
      if (!sig) { res.writeHead(400); res.end('Missing signature'); return; }
      const event = verifyStripeWebhook(body, sig);
      if (!event) { res.writeHead(400); res.end('Invalid signature'); return; }
      // Process BEFORE answering. This used to reply 200 and then fire the
      // handler off unawaited, so a failed database write happened after Stripe
      // had already been told the event was received — and Stripe never
      // retried. Someone could pay and simply never be upgraded, with nothing
      // but a line in the logs.
      //
      // Answering non-2xx on a persistence failure puts Stripe's redelivery
      // schedule behind it instead. The writes are idempotent, so a retry that
      // succeeds lands in exactly the right state.
      const ok = await handleStripeWebhook(event)
        .catch(e => { console.error('Webhook error:', e && e.message); return false; });
      if (!ok) {
        res.writeHead(500, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Could not persist event — please retry' }));
        return;
      }
      res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ received: true }));
    })();
    return;
  }

  if (url.pathname === '/stripe/checkout' && req.method === 'POST') {
    (async () => {
      try {
        let raw;
        try { raw = await readBody(req, 16384); }
        catch(e) { res.writeHead(413, cors); res.end('Payload too large'); return; }
        // A body that is not parseable JSON — or is valid JSON but not an
        // object, like `null` or `[1,2,3]` — is a client mistake. This used to
        // fall through to the outer catch and answer 500, reporting the
        // caller's error as a server fault and polluting error monitoring.
        let payload;
        try {
          payload = JSON.parse(raw.toString('utf8'));
        } catch (_) {
          res.writeHead(400, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Invalid JSON body' })); return;
        }
        if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
          res.writeHead(400, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Body must be a JSON object' })); return;
        }
        // Resolve the caller from their JWT rather than trusting the body.
        //
        // This route took userId and email as request parameters, which is
        // exactly what authedUser exists to prevent — its own comment says any
        // endpoint acting on a specific user's data must go through it.
        // /verify-pro, /stripe/portal and /ensure-profile all do; this one was
        // missed. Unauthenticated, anyone could mint checkout sessions against
        // arbitrary ids and addresses, and probe which accounts already hold a
        // subscription by reading the 409.
        const caller = await authedUser(req);
        if (!caller) {
          res.writeHead(401, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Not signed in' })); return;
        }
        const userId = caller.id;
        const email  = caller.email;
        const { plan } = payload;
        if (!email) { res.writeHead(400, cors); res.end('Account has no email address'); return; }

        // Anything that was not 'annual' silently fell through to the monthly
        // price, so a typo or a junk value quietly charged a different plan
        // than the one named.
        const PRICES = { monthly: STRIPE_PRICE_MONTHLY, annual: STRIPE_PRICE_ANNUAL };
        if (!plan || !Object.prototype.hasOwnProperty.call(PRICES, plan)) {
          res.writeHead(400, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'plan must be "monthly" or "annual"' })); return;
        }

        // Refuse a second checkout for someone who is already subscribed.
        // createCheckoutSession passes customer_email rather than customer, so
        // Stripe mints a NEW customer each time; the webhook then overwrites
        // stripe_customer_id/stripe_subscription_id with the new pair and the
        // original subscription keeps billing with nothing in the app pointing
        // at it. The user is charged twice and cancelling only stops the second.
        const { data: existing, error: existingErr } = await supabase.from('users')
          .select('stripe_subscription_id').eq('id', userId).maybeSingle();
        if (existingErr) {
          // The whole point of this guard is that it knows whether there is
          // already a subscription. A failed read does not, and waving the
          // checkout through on a guess is precisely the double-charge
          // described above. Refuse; the customer can try again.
          console.error('Checkout: could not check for an existing subscription:', existingErr.message);
          res.writeHead(503, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Could not start checkout, try again shortly' })); return;
        }
        if (existing?.stripe_subscription_id) {
          const current = await stripeRequest(`/subscriptions/${existing.stripe_subscription_id}`);

          // Any subscription that still exists at Stripe blocks a second one,
          // not just an active or trialing one.
          //
          // past_due was the gap. A failed payment maps the account to
          // 'past_due', which every reader treats as not-Pro — so the account
          // page shows an Upgrade button, and this guard used to wave the
          // resulting checkout straight through. The webhook then overwrote
          // stripe_subscription_id with the new subscription and the original
          // kept billing with nothing in the app pointing at it: exactly the
          // double-charge this guard exists to prevent.
          //
          // 'incomplete' is deliberately absent. That is a checkout whose first
          // payment never landed; Stripe expires it within a day, and blocking
          // it would strand someone who simply abandoned a payment and came
          // back to try again.
          const LIVE = ['active', 'trialing', 'past_due', 'unpaid', 'paused'];
          if (current && LIVE.includes(current.status)) {
            const needsPayment = current.status === 'past_due' || current.status === 'unpaid';
            res.writeHead(409, { ...cors, 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
              error: needsPayment
                ? 'Your subscription needs a payment method updating — manage it from your account rather than starting a new one.'
                : 'You already have an active subscription.',
              alreadySubscribed: true,
              subscriptionStatus: current.status,
              needsPayment,
            }));
            return;
          }
        }

        const session = await createCheckoutSession(userId, email, PRICES[plan], plan);
        if (!session) { res.writeHead(500, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Failed' })); return; }
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ url: session.url }));
      } catch(e) { res.writeHead(500, cors); res.end('Server error'); }
    })();
    return;
  }

  if (url.pathname === '/stripe/portal' && req.method === 'POST') {
    (async () => {
      try {
        // The customer id is resolved from the authenticated user's own row.
        // It is NEVER taken from the request body: customer ids are not secret
        // and the old code handed a billing portal session — invoices, card
        // details, cancellation — to anyone who could supply one.
        const caller = await authedUser(req);
        if (!caller) { res.writeHead(401, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }

        const { data: user, error: userErr } = await supabase.from('users').select('stripe_customer_id').eq('id', caller.id).single();
        // 404 "No billing account" is a statement about the account. A read
        // that failed is not in a position to make it.
        if (userErr && userErr.code !== 'PGRST116') {
          console.error('billing-portal: could not read the account:', userErr.message);
          res.writeHead(503, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Could not open billing, try again shortly' })); return;
        }
        if (!user?.stripe_customer_id) { res.writeHead(404, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'No billing account' })); return; }

        const session = await stripeRequest('/billing_portal/sessions', 'POST', { customer: user.stripe_customer_id, return_url: `${SITE_URL}/account.html` });
        if (!session) { res.writeHead(502, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Failed' })); return; }
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ url: session.url }));
      } catch(e) { res.writeHead(500, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Server error' })); }
    })();
    return;
  }

  res.writeHead(404); res.end('Not found');

})().catch(e => {
  // Anything that escapes a route's own error handling lands here instead of
  // taking the process down. Reply if we still can, so the client gets an
  // answer rather than a reset connection.
  console.error('Unhandled request error:', e && e.message);
  try {
    if (!res.headersSent) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Server error' }));
    } else {
      res.end();
    }
  } catch (_) { /* response already torn down */ }
}); }).listen(process.env.PORT || 3000, () => {
  console.log(`🟢 HTTP server on port ${process.env.PORT || 3000}`);
});

// Render sends SIGTERM before replacing the process, on every deploy. With no
// handler the listening socket is cut mid-request, so whoever happened to be
// checking out at that moment gets a connection reset instead of an answer.
// Stop accepting new connections, let the in-flight ones finish, then exit.
//
// An in-flight email dispatch is deliberately NOT waited for. It can run for
// minutes — longer than the platform's grace period — and dispatchToSubscribers
// now resumes per recipient on the next start, so being cut short costs nothing
// except the waiting. Blocking here would just get the process force-killed at
// the same point, having also delayed the deploy.
let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) return;            // SIGTERM then SIGKILL is normal
  shuttingDown = true;
  console.log(`
${signal} received — finishing in-flight requests`);
  server.close(() => { console.log('🔴 HTTP server closed'); process.exit(0); });
  // A keep-alive connection that never sends another request would otherwise
  // hold close() open until the platform kills us anyway. unref so this timer
  // cannot by itself keep the process alive.
  setTimeout(() => { console.log('🔴 Grace period elapsed'); process.exit(0); }, 10000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));

// ═══════════════════════════════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════════════════════════════

// Last-resort backstop. Every known async path is guarded individually above,
// but this is a single-process service where an unhandled rejection means a
// total outage until the platform restarts it — and each restart re-runs the
// morning fetch against a metered API. Log loudly and stay up rather than
// dying silently on something nobody anticipated.
process.on('unhandledRejection', (reason) => {
  console.error('⚠️ UNHANDLED REJECTION (service kept alive):', reason && (reason.stack || reason.message || reason));
});

// The synchronous half of the same problem. A throw from a timer callback or an
// event emitter has no try/catch to land in, and Node's default is to terminate.
//
// The standard advice is to log and exit, on the grounds that a process which
// has thrown from an unknown place is in an unknown state. That argument is
// weaker here than usual: this service keeps its state in Supabase rather than
// in memory, so there is little in-process state to be corrupted — and the
// alternative is a total outage plus another metered morningFetch on restart.
// Same trade as the handler above, made deliberately.
process.on('uncaughtException', (err) => {
  console.error('⚠️ UNCAUGHT EXCEPTION (service kept alive):', err && (err.stack || err.message || err));
});

(async () => {
  console.log(`\n🟢 The Tipster Engine v9.9 starting...`);
  console.log(`   Season: ${currentSeason()}/${currentSeason()+1}`);
  console.log(`   Data source: Sofascore (RapidAPI Pro)`);
  console.log(`   Schedule: Morning fetch 06:00 | Midday refresh 13:00 | Tips every 15min`);

  logConfiguration();

  // Awaited, unlike the unsubscribe probe below: if the schema is short a
  // column, every line after this is going to fail in a way that is much
  // harder to read than one message at the top of the log.
  await checkSchema().catch(e => console.error('Schema check error:', e && e.message));

  // Not awaited: these report on things outside this process, and startup does
  // not depend on either answer.
  checkStripePrices().catch(e => console.error('Stripe price check error:', e && e.message));
  checkUnsubscribeLink().catch(e => console.error('Unsubscribe link check error:', e && e.message));

  // Every async call below is guarded. A rejection from any of them would
  // otherwise be unhandled, and Node terminates the process on those — one
  // transient Sofascore or Supabase failure would take the service down, and
  // each restart re-runs morningFetch and spends RapidAPI quota.
  const guard = (label) => (e) => console.error(`${label} error:`, e && e.message);

  // On startup, run morning fetch immediately to populate cache
  await morningFetch().catch(guard('Startup morning fetch'));
  await settleResults().catch(guard('Startup settle'));

  // Start 15-min tip generation cycle
  setInterval(() => runEngine().catch(guard('Scheduled runEngine')), 15 * 60 * 1000);
  await runEngine().catch(guard('Startup runEngine'));

  startScheduler();
  setInterval(() => processAdminJobs().catch(guard('Scheduled admin jobs')), 30 * 1000);
  processAdminJobs().catch(guard('Startup admin jobs'));
})();

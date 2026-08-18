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
//   Odds:    1.40–2.20 core | 2.21–2.50 elite only | reject outside
//   Edge:    H2H ≥ 8% | Overs ≥ 12% | Elite grade requires ≥ 10% (H2H) / 14% (Overs)
//   Grades:  A+ (2–2.5u) | A (1.5u) | below A = no bet
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
const NHL_MIN_EDGE      = 0;
const NBA_MIN_EDGE      = 0;
const OVERS_MIN_EDGE    = 0;
const ELITE_H2H_EDGE    = 10;
const ELITE_OVERS_EDGE  = 14;

// Odds tiers:
//   INSIGHT_ODDS_MIN  — show as "Short Price Watch" (informational, stake 0)
//   BET_ODDS_MIN      — minimum odds for a real bet recommendation
//   ODDS_ELITE_MAX    — maximum odds published
const INSIGHT_ODDS_MIN  = 1.05; // Show from here — heavy favs shown as insight
const BET_ODDS_MIN      = 1.35; // Minimum odds for a real bet
const ODDS_CORE_MAX     = 5.00;
const ODDS_ELITE_MAX    = 10.0;

const LINE_MOVE_REJECT  = 0.10;
const MATRIX_MAX_GOALS  = 8;
const FOOTBALL_EDGE_PREMIUM = 0;
const MIN_QUALITY_SCORE = 0.10;
const NHL_HOME_ADVANTAGE  = 0.20;
const NHL_LEAGUE_AVG_GF   = 3.10;
const NHL_MATRIX_MAX      = 7;
const NBA_HOME_ADVANTAGE  = 3.5;
const NBA_LEAGUE_AVG_PTS  = 113;
const NBA_SCORE_STD_DEV   = 12.0;
const DC_RHO = 0.10;

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
};

// Sofascore API wrapper — rate limited to 5 req/sec
let rapidApiCallCount = 0;
let rapidApiCallDate  = '';

function trackApiCall() {
  const today = new Date().toISOString().split('T')[0];
  if (rapidApiCallDate !== today) { rapidApiCallDate = today; rapidApiCallCount = 0; }
  rapidApiCallCount++;
  if (rapidApiCallCount % 10 === 0) console.log(`📡 RapidAPI calls today: ${rapidApiCallCount}`);
}

// How many times a single Sofascore request is attempted before giving up.
const SOFASCORE_MAX_ATTEMPTS = 3;

async function sofascoreFetch(path, params = {}, attempt = 1) {
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
      if (res.status === 429 || res.status >= 500) return retry(`HTTP ${res.status}`);
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
    return parseFloat((1 + num / den).toFixed(4));
  }
  // Decimal format
  const d = parseFloat(s);
  return isFinite(d) && d > 0 ? d : 0;
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
async function fetchTeamRecentMatches(teamId) {
  await new Promise(r => setTimeout(r, 250));
  const data = await sofascoreFetch(`/teams/get-last-matches`, { id: teamId, page: 0 });
  return data?.events || [];
}

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
        if      (label === '1' || nameMatch(label, homeTeam)) h2hOutcomes.push({ name: homeTeam, price });
        else if (label === '2' || nameMatch(label, awayTeam)) h2hOutcomes.push({ name: awayTeam, price });
        else if (idx === 0)                                   h2hOutcomes.push({ name: homeTeam, price });
        else if (idx === 1)                                   h2hOutcomes.push({ name: awayTeam, price });
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
  const leagueAvg = getLeagueAvg(tournamentId);
  const totalAvg  = leagueAvg.homeGoals + leagueAvg.awayGoals;
  const homeRatio = leagueAvg.homeGoals / totalAvg;
  const awayRatio = leagueAvg.awayGoals / totalAvg;

  for (const row of rows) {
    const name    = row.team?.name || row.team?.shortName;
    const teamId  = row.team?.id;
    if (!name) continue;

    const played = row.matches || row.played || 0;
    const gf     = row.scoresFor || row.goalsScored || 0;
    const ga     = row.scoresAgainst || row.goalsConceded || 0;

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
  const today = new Date().toISOString().split('T')[0];

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

  matchContextCache[eventId] = ctx;
  return ctx;
}

// ─── FETCH LINEUPS (called at 21:00 UK) ──────────────────────
async function fetchLineupsForToday() {
  const today = new Date().toLocaleDateString('en-CA', { timeZone: 'Europe/London' });
  let fetched = 0;

  for (const sport of SPORTS) {
    const events = sofascoreCache.events[sport.key] || [];
    for (const event of events) {
      const eventDate = event.commence_time?.split('T')[0];
      if (eventDate !== today && eventDate !== new Date(Date.now() + 86400000).toLocaleDateString('en-CA', { timeZone: 'Europe/London' })) continue;
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
          matchContextCache[event.id] = ctx;
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
function getContextModifiers(teamSide, ctx, isHome) {
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
    if (form.restDays <= 2) {
      attackMult  *= 0.93; // fatigue reduces attacking output
      defenceMult *= 1.05; // and weakens defence
      notes.push(`Rest: ${form.restDays}d ⚠️`);
    } else if (form.restDays <= 3) {
      attackMult  *= 0.97;
      notes.push(`Rest: ${form.restDays}d`);
    } else {
      notes.push(`Rest: ${form.restDays}d ✓`);
    }

    // ── Recent goals adjustment ────────────────────────────
    // If team's recent form goals are very different from season avg,
    // blend them in slightly
    if (form.gamesPlayed >= 3) {
      const recentAttAdj = (form.goalsFor - 1.3) * 0.10; // small nudge
      attackMult = Math.max(0.7, Math.min(1.4, attackMult + recentAttAdj));
    }
  } else {
    dataQuality = 0.90;
    notes.push('No form data');
  }

  // ── Missing players (from lineups) ────────────────────────
  // Position-weighted impact: forwards matter more for attack,
  // defenders/GK for defence
  if (missing.length > 0) {
    const attackImpact  = missing.length * 0.03; // ~3% per missing player
    const defenceImpact = missing.length * 0.02;
    attackMult  = Math.max(0.75, attackMult  - attackImpact);
    defenceMult = Math.max(0.80, defenceMult + defenceImpact);
    notes.push(`Missing: ${missing.map(p => p.name).join(', ')}`);
    dataQuality = Math.min(dataQuality, 0.95); // slight quality boost — we HAVE lineup data
  } else if (ctx.lineups?.homeConfirmed) {
    notes.push('Full squad ✓');
  }

  return {
    attackMult:  parseFloat(attackMult.toFixed(3)),
    defenceMult: parseFloat(defenceMult.toFixed(3)),
    dataQuality: parseFloat(dataQuality.toFixed(3)),
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
      for (const [name, stats] of Object.entries(teams)) {
        teamStatsCache[`${name}_${sport.tournamentId}`] = stats;
      }

      console.log(`  ⚽ ${sport.league}: ${Object.keys(teams).length} teams loaded`);
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
  }

  sofascoreCache.oddsFetchedAt = new Date();
  console.log('✅ Midday refresh complete.');
}

// ═══════════════════════════════════════════════════════════════
// NHL STATS — Free NHL API (api.nhle.com) — unchanged from v7
// ═══════════════════════════════════════════════════════════════

const nhlTeamCache = {};
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
function goalieQualityMultiplier(goalieName, teamName) {
  // Try exact team name, then fuzzy
  const data = nhlGoalieCache[teamName] ||
    Object.entries(nhlGoalieCache).find(([k]) => nameMatch(k, teamName))?.[1];

  if (!data || !data.savePercent) return { multiplier: 1.0, label: null };

  const sv = data.savePercent;
  let multiplier;
  if      (sv >= 0.930) multiplier = 0.82;  // elite — saves 93%+
  else if (sv >= 0.920) multiplier = 0.90;  // very good
  else if (sv >= 0.910) multiplier = 0.96;  // average
  else if (sv >= 0.900) multiplier = 1.03;  // below average
  else if (sv >= 0.890) multiplier = 1.10;  // weak
  else                  multiplier = 1.18;  // very weak

  return {
    multiplier,
    label: `${data.starter} (SV%: ${sv.toFixed(3)}, GAA: ${data.gaa.toFixed(2)})`,
    savePercent: sv,
    gaa: data.gaa,
    isConfirmed: data.isConfirmed,
  };
}

function currentSeason() {
  const now = new Date();
  const month = now.getMonth() + 1;
  return month >= 7 ? now.getFullYear() : now.getFullYear() - 1;
}

async function fetchNHLAllTeams() {
  const today = new Date().toISOString().split('T')[0];
  if (nhlAllTeamsCache && nhlAllTeamsCacheDate === today) return nhlAllTeamsCache;
  const year = currentSeason();
  const seasonId = `${year}${year + 1}`;
  try {
    const res = await fetch(
      `https://api.nhle.com/stats/rest/en/team/summary?cayenneExp=seasonId=${seasonId}%20and%20gameTypeId=2`
    );
    if (!res.ok) return null;
    const data = await res.json();
    nhlAllTeamsCache = data.data || [];
    nhlAllTeamsCacheDate = today;
    console.log(`🏒 NHL team stats loaded: ${nhlAllTeamsCache.length} teams`);
    return nhlAllTeamsCache;
  } catch(e) { console.error('NHL API error:', e.message); return null; }
}

async function fetchNHLTeamStats(teamName) {
  const cacheKey = `${teamName}_${currentSeason()}`;
  if (nhlTeamCache[cacheKey]) return nhlTeamCache[cacheKey];
  const allTeams = await fetchNHLAllTeams();
  if (!allTeams) return null;
  const team = allTeams.find(t =>
    nameMatch(t.teamFullName, teamName) || nameMatch(t.teamName, teamName)
  );
  if (!team || (team.gamesPlayed || 0) < 5) return null;
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

async function fetchNBAAllTeamStats() {
  const today = new Date().toISOString().split('T')[0];
  if (nbaAllTeamsCache && nbaAllTeamsCacheDate === today) return nbaAllTeamsCache;

  const season = currentSeason();
  const seasonStr = `${season}-${String(season + 1).slice(2)}`; // e.g. "2024-25"

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
      console.log(`  🏀 NBA stats API HTTP ${res.status}`);
      return null;
    }

    const data = await res.json();
    const headers = data.resultSets?.[0]?.headers || [];
    const rows    = data.resultSets?.[0]?.rowSet   || [];

    if (!rows.length) { console.log('  🏀 NBA stats: no rows returned'); return null; }

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
      if (name && gp >= 5) {
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

    nbaAllTeamsCache     = result;
    nbaAllTeamsCacheDate = today;
    console.log(`  🏀 NBA team stats loaded: ${Object.keys(result).length} teams`);
    return result;
  } catch(e) {
    console.log(`  🏀 NBA stats fetch error: ${e.message}`);
    return null;
  }
}

async function fetchNBATeamStats(teamName) {
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

function nameMatch(a, b) {
  if (!a || !b) return false;
  const clean = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  const ac = clean(a), bc = clean(b);
  return ac === bc || ac.includes(bc) || bc.includes(ac);
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

function poisson(lambda, k) {
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
      matrix[h][a] = Math.max(0, raw * tau);
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
  if (prob <= 0.001) return 999.0;
  return parseFloat((1 / prob).toFixed(2));
}

function calcEdge(modelProb, trueImpliedProb) {
  return parseFloat(((modelProb - trueImpliedProb) * 100).toFixed(2));
}

function kellyStake(modelProb, decimalOdds, fraction = 0.25) {
  const b = decimalOdds - 1;
  if (b <= 0 || modelProb <= 0 || modelProb >= 1) return 1.0;
  const q    = 1 - modelProb;
  const full = (b * modelProb - q) / b;
  if (full <= 0) return 0;
  const sized = full * fraction;
  if (sized >= 0.40) return 3.0;
  if (sized >= 0.28) return 2.5;
  if (sized >= 0.18) return 2.0;
  if (sized >= 0.11) return 1.5;
  if (sized >= 0.06) return 1.0;
  return 0.5;
}

function normalCDF(x) {
  const t = 1 / (1 + 0.2315419 * Math.abs(x));
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
function extractMarketData(event) {
  const books1x2  = [];
  const books2way = [];
  let bestOver25 = 0, bestOver25Book = '';

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
    const totals = book.markets?.find(m => m.key === 'totals');
    if (totals) {
      for (const o of totals.outcomes) {
        if (o.name === 'Over' && (o.price || 0) > bestOver25) {
          bestOver25 = o.price; bestOver25Book = book.title;
        }
      }
    }
  }

  // 3-way (football)
  if (books1x2.length > 0) {
    const b       = books1x2[0];
    const totalIP = (1/b.home) + (1/b.draw) + (1/b.away);
    return {
      trueHome:   (1/b.home) / totalIP,
      trueDraw:   (1/b.draw) / totalIP,
      trueAway:   (1/b.away) / totalIP,
      homeOdds:   b.home, drawOdds: b.draw, awayOdds: b.away,
      over25Odds: bestOver25,
      homeBook:   b.title, drawBook: b.title, awayBook: b.title, over25Book: bestOver25Book,
      bookCount:  books1x2.length,
      avgMargin:  parseFloat(((totalIP - 1) * 100).toFixed(2)),
      isTwoWay:   false,
    };
  }

  // 2-way (NBA/NHL)
  if (books2way.length > 0) {
    const b       = books2way[0];
    const totalIP = (1/b.home) + (1/b.away);
    return {
      trueHome:   (1/b.home) / totalIP,
      trueDraw:   0,
      trueAway:   (1/b.away) / totalIP,
      homeOdds:   b.home, drawOdds: 0, awayOdds: b.away,
      over25Odds: bestOver25,
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

  // Reject if edge > 20% without strong multi-book confirmation
  if (edge > 20 && market.bookCount < 3) return null;

  // Reject if model probability diverges from market by > 20 percentage points
  const divergence = Math.abs(modelProb - trueImplied);
  if (divergence > 0.20) return null;

  return candidate;
}

// ─── CANDIDATE SCORING ────────────────────────────────────────

const NBA_CONFIDENCE_CEILING = 80; // cap until injury/rest data available
const NHL_CONFIDENCE_CEILING = 80; // cap until goalie/rest data available

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
    const cap = (dataQualityTier >= 1.0) ? 88 : 80;
    conf = Math.min(conf, cap);
  }
  if (sport === 'Basketball') conf = Math.min(conf, 80);

  // Hard floor — don't publish tips below 50% win probability
  conf = Math.max(50, Math.min(95, conf));

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
      lH = Math.max(0.3, Math.min(4.0, hAtt * aDef * leagueAvg.homeGoals));
      lA = Math.max(0.3, Math.min(4.0, aAtt * hDef * leagueAvg.awayGoals));
    } else {
      // No stats — skip, don't fall back to market consensus for football
      return null;
    }

    const matrix = buildScoreMatrix(lH, lA);
    // Apply match context modifiers (form, rest, injuries, lineups)
    const ctx      = matchContextCache[event.id] || null;
    const homeMod  = getContextModifiers('home', ctx, true);
    const awayMod  = getContextModifiers('away', ctx, false);

    // Apply multipliers to expected goals
    let lHmod = Math.max(0.3, Math.min(4.0, lH * homeMod.attackMult * awayMod.defenceMult));
    let lAmod = Math.max(0.3, Math.min(4.0, lA * awayMod.attackMult * homeMod.defenceMult));

    // H2H adjustment — if one team dominates historically, nudge lambda slightly
    if (ctx?.h2h && ctx.h2h.total >= 5) {
      const h2hHomeRate = ctx.h2h.homeWins / ctx.h2h.total;
      const h2hAwayRate = ctx.h2h.awayWins / ctx.h2h.total;
      const h2hAdj = (h2hHomeRate - h2hAwayRate) * 0.08; // max ±8% nudge
      lHmod = Math.max(0.3, Math.min(4.0, lHmod * (1 + h2hAdj)));
      lAmod = Math.max(0.3, Math.min(4.0, lAmod * (1 - h2hAdj)));
    }

    const dataQuality = Math.min(homeMod.dataQuality, awayMod.dataQuality);
    const modMatrix = buildScoreMatrix(lHmod, lAmod);
    const { homeWin, draw, awayWin, over25 } = calcOutcomes(modMatrix);
    const over25IP = market.over25Odds > 0 ? 1 / market.over25Odds : 0;
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
      market:        'h2h',
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
    let lH = Math.max(0.5, Math.min(6.0,
      homeAttack * awayDef * NHL_LEAGUE_AVG_GF * awayGoalie.multiplier + NHL_HOME_ADVANTAGE
    ));
    let lA = Math.max(0.5, Math.min(6.0,
      awayAttack * homeDef * NHL_LEAGUE_AVG_GF * homeGoalie.multiplier
    ));

    // Data quality tier — lower if goalie data absent (less confident model)
    const hasGoalieData = homeGoalie.label && awayGoalie.label;
    let dataQualityTier = hasGoalieData ? 1.0 : 0.85;

    const goalieNote = hasGoalieData
      ? `H: ${homeGoalie.label} | A: ${awayGoalie.label}`
      : 'No goalie data';

    // Apply match context (form, rest, H2H) to NHL lambda
    const ctx     = matchContextCache[event.id] || null;
    const homeMod = getContextModifiers('home', ctx, true);
    const awayMod = getContextModifiers('away', ctx, false);

    lH = Math.max(0.5, Math.min(6.0, lH * homeMod.attackMult * awayMod.defenceMult));
    lA = Math.max(0.5, Math.min(6.0, lA * awayMod.attackMult * homeMod.defenceMult));

    // H2H adjustment for NHL
    if (ctx?.h2h && ctx.h2h.total >= 5) {
      const h2hAdj = ((ctx.h2h.homeWins - ctx.h2h.awayWins) / ctx.h2h.total) * 0.06;
      lH = Math.max(0.5, Math.min(6.0, lH * (1 + h2hAdj)));
      lA = Math.max(0.5, Math.min(6.0, lA * (1 - h2hAdj)));
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

    if (market.over25Odds >= INSIGHT_ODDS_MIN && market.over25Odds <= ODDS_ELITE_MAX) {
      const over55IP = 1 / market.over25Odds;
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
    const pick = candidates.reduce((a, b) => {
      const scoreA = a.edge * 0.7 + (a.qualityScore || 0) * 0.3;
      const scoreB = b.edge * 0.7 + (b.qualityScore || 0) * 0.3;
      return scoreA >= scoreB ? a : b;
    });

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
  if (isFootball && isTotals)  return null;

  const odds = parseFloat(tip.odds || 0);
  if (odds < INSIGHT_ODDS_MIN) return null; // below 1.05 — not worth showing
  if (odds > ODDS_ELITE_MAX)   return null;

  // Line movement check
  if (existingBestOdds && existingBestOdds > 0) {
    const lineMove = existingBestOdds - odds;
    if (lineMove >= LINE_MOVE_REJECT) return null;
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
  const conf = parseFloat(tip.confidence || 0);
  let stake;
  if      (conf >= 78) stake = 2.0;
  else if (conf >= 72) stake = 1.5;
  else                 stake = 1.0;

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
  for (const event of events) {
    if (!event.bookmakers || !event.bookmakers.length) continue;
    const hours = (new Date(event.commence_time) - new Date()) / 3600000;
    if (hours < 0 || hours > 48) continue;

    let tip = null;

    if (sport.name === 'Football') {
      tip = await analyseFootballFixture(event, sport);
    } else if (sport.name === 'Basketball') {
      tip = await analyseNBAFixture(event, sport);
    } else if (sport.name === 'Ice Hockey') {
      tip = await analyseNHLFixture(event, sport);
    }

    if (!tip) continue;
    const approved = applyStrictRules(tip);
    if (approved) tips.push(approved);
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
      if (tip.event_id) {
        const { data: byId } = await supabase.from('tips').select('id, tip_ref, confidence, odds, best_odds, bookmaker, status')
          .eq('event_id', tip.event_id).eq('selection', tip.selection)
          .in('status', ['pending', 'won', 'lost', 'void']).maybeSingle();
        existing = byId;
      }
      if (!existing) {
        // Fuzzy name match — catches "Inter" vs "Inter Milan", "Marseille" vs "Olympique de Marseille"
        const date = new Date(tip.event_time).toISOString().split('T')[0];
        const { data: candidates } = await supabase.from('tips').select('id, tip_ref, confidence, odds, best_odds, bookmaker, status, home_team, away_team')
          .gte('event_time', `${date}T00:00:00Z`).lte('event_time', `${date}T23:59:59Z`)
          .eq('selection', tip.selection)
          .in('status', ['pending', 'won', 'lost', 'void']);
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
          await supabase.from('tips').update({
            odds: tip.odds, best_odds: newBest, bookmaker: tip.bookmaker,
            confidence: tip.confidence, stake: tip.stake,
            selection: tip.selection, market: tip.market, notes: tip.notes,
            model_edge: tip.model_edge, model_prob: tip.model_prob,
            implied_prob: tip.implied_prob, fair_odds: tip.fair_odds,
            quality_score: tip.quality_score, book_count: tip.book_count,
            event_id: tip.event_id,
            ...(existing.tip_ref ? {} : { tip_ref: tip.tip_ref }),
          }).eq(updateKey, updateVal);
          updated++;
        } else { skipped++; }
        continue;
      }

      // advised_odds is written once at publication and NEVER updated.
      // It is the price we told subscribers to take, and the only honest
      // basis for settlement. `odds` tracks the live price; `best_odds`
      // tracks the peak seen — neither is what we actually advised.
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

const scoreCache = {};

async function fetchSofascoreResult(eventId) {
  if (scoreCache[eventId]) return scoreCache[eventId];

  // Primary: matches/detail
  const data = await sofascoreFetch(`/matches/detail`, { id: eventId });
  if (data?.event) {
    const e = data.event;
    const isFinished = e.status?.type === 'finished' || e.status?.description === 'Ended' || e.status?.code === 100;
    if (!isFinished) {
      console.log(`  ⏳ Not finished yet [${eventId}]: ${e.status?.description} (${e.status?.type})`);
      return null;
    }
    const result = {
      homeScore: e.homeScore?.current ?? e.homeScore?.normaltime ?? null,
      awayScore: e.awayScore?.current ?? e.awayScore?.normaltime ?? null,
      finished:  true,
    };
    if (result.homeScore !== null) {
      scoreCache[eventId] = result;
      return result;
    }
  }

  // Fallback: matches/get-h2h-events or tournaments/get-last-matches
  // Try getting score via the match graph endpoint which often has final scores
  await new Promise(r => setTimeout(r, 300));
  const graphData = await sofascoreFetch(`/matches/get-graph`, { matchId: eventId });
  if (graphData) {
    // Graph data has homeScore/awayScore on parent
    // Try extracting from the last data point
    const points = graphData.graphPoints || [];
    if (points.length > 0) {
      const last = points[points.length - 1];
      if (last.homeScore !== undefined && last.awayScore !== undefined) {
        const result = {
          homeScore: last.homeScore,
          awayScore: last.awayScore,
          finished:  true,
        };
        console.log(`  ✅ Score from graph [${eventId}]: ${result.homeScore}-${result.awayScore}`);
        scoreCache[eventId] = result;
        return result;
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
  rows.sort((a, b) => {
    const ta = new Date(a.settled_at).getTime();
    const tb = new Date(b.settled_at).getTime();
    if (ta !== tb) return ta - tb;
    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;   // stable tiebreak on PK
  });

  let acc = 0, fixed = 0;
  for (const r of rows) {
    acc = parseFloat((acc + parseFloat(r.profit_loss || 0)).toFixed(2));
    if (parseFloat(r.running_pl) === acc) continue;   // NaN on NULL -> always rewrites
    const { error } = await supabase.from('results_history').update({ running_pl: acc }).eq('id', r.id);
    if (error) { console.error(`  ❌ running_pl renumber [id ${r.id}]:`, error.message); continue; }
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

  const alreadyGraded = await selectAll('tips', '*',
    q => q.in('status', ['won', 'lost']).lt('event_time', nowIso));

  // Assign a tip_ref to ANY tip lacking one — graded tips included. tip_ref is
  // the only key linking a tip to its ledger row, so a graded tip with a NULL
  // ref cannot be matched against results_history: it was filtered out of the
  // backfill with no log line and stayed invisible permanently.
  for (const t of [...pendingTips, ...alreadyGraded].filter(t => !t.tip_ref)) {
    const prefixes = { 'Football':'FB','Basketball':'BB','Ice Hockey':'IH' };
    const ref = `${prefixes[t.sport]||'TT'}-${Date.now().toString(36).toUpperCase().slice(-4)}${Math.random().toString(36).toUpperCase().slice(-4)}`;
    const { error } = await supabase.from('tips').update({ tip_ref: ref }).eq('id', t.id);
    // Leave t.tip_ref NULL on failure — a ref we did not persist must not be
    // treated as backfillable, or the ledger row would reference nothing.
    if (error) { console.error(`  ❌ tip_ref assign [id ${t.id}]:`, error.message); continue; }
    t.tip_ref = ref;
    if (t.status !== 'pending') console.log(`  🏷️ Assigned ${ref} to graded tip id ${t.id} (was NULL) — now eligible for backfill`);
  }

  // Computed AFTER ref assignment, so graded tips that just received one are
  // included rather than silently skipped.
  const missing = alreadyGraded.filter(t => t.tip_ref && !inHistory.has(t.tip_ref));
  const pending = pendingTips;

  // Same shape on every return path — the caller logs these counts, and
  // `ok: !!result` on /admin/resettle read a bare return as a failure.
  if (!pending.length && !missing.length) { console.log('🏁 Nothing to settle.'); return { settled: 0, backfilled: 0 }; }

  const { data: lastRow } = await supabase.from('results_history').select('running_pl').order('settled_at', { ascending: false }).limit(1).maybeSingle();
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
        if (!oddsUsed || !stake) {
          console.log(`  ⚠️ Skipping backfill [${tip.tip_ref}] — missing odds/stake`);
          continue;
        }

        let pl = tip.profit_loss;
        if (pl === null || pl === undefined) {
          pl = tip.status === 'won'
            ? parseFloat(((oddsUsed - 1) * stake).toFixed(2))
            : parseFloat((-stake).toFixed(2));
          await supabase.from('tips').update({ profit_loss: pl }).eq('tip_ref', tip.tip_ref);
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
      const hoursOld = (now - new Date(tip.event_time).getTime()) / 3600000;
      if (hoursOld > 72) {
        await supabase.from('tips').update({ status: 'void' }).eq('tip_ref', tip.tip_ref);
        console.log(`⚪ VOID (no result after 72h): [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team}`);
        continue;
      }

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
          if (homeScore === null) { console.log(`⏳ No result yet: ${tip.home_team} vs ${tip.away_team}`); continue; }
        }
      } else {
        // Fallback: name match from cache for tips created before event_id was stored
        const sport = SPORTS.find(s => s.league === tip.league);
        if (!sport) continue;

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
                  if (!result) { console.log(`⏳ No result yet: ${tip.home_team} vs ${tip.away_team}`); continue; }
                  homeScore = result.homeScore;
                  awayScore = result.awayScore;
                }
              } else {
                console.log(`⏳ No cached event for: ${tip.home_team} vs ${tip.away_team}`);
                continue;
              }
            } else {
              console.log(`⏳ No cached event for: ${tip.home_team} vs ${tip.away_team}`);
              continue;
            }
          } catch (e) {
            console.log(`⚠️ Last matches fetch failed for ${tip.league}: ${e.message}`);
            continue;
          }
        } else {
          const result = await fetchSofascoreResult(cachedEvent.id);
          if (!result) { console.log(`⏳ No result yet: ${tip.home_team} vs ${tip.away_team}`); continue; }
          homeScore = result.homeScore;
          awayScore = result.awayScore;
        }
      }

      if (homeScore === null || awayScore === null) continue;

      // Grade the selection. Resolve the side against BOTH teams — the old
      // ternary treated "not a home-name match" as "must be away", so a single
      // fuzzy-match miss silently graded the opposite team.
      let won = false, graded = true;
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
        if (Number.isFinite(line)) won = (homeScore + awayScore) > line; else graded = false;
      } else if (sel.startsWith('under')) {
        const line = parseFloat(sel.replace('under ', ''));
        if (Number.isFinite(line)) won = (homeScore + awayScore) < line; else graded = false;
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
      const pl = won
        ? parseFloat(((settlementOdds - 1) * tip.stake).toFixed(2))
        : parseFloat((-tip.stake).toFixed(2));

      const { data: already } = await supabase.from('results_history').select('id').eq('tip_ref', tip.tip_ref).maybeSingle();

      await supabase.from('tips').update({
        status: won ? 'won' : 'lost', profit_loss: pl,
        result_updated_at: new Date().toISOString()
      }).eq('tip_ref', tip.tip_ref);

      if (already) continue;

      currentRunningPL = parseFloat((currentRunningPL + pl).toFixed(2));

      await supabase.from('results_history').insert({
        tip_ref:    tip.tip_ref,
        sport:      tip.sport,
        event:      `${tip.home_team} vs ${tip.away_team}`,
        selection:  tip.selection,
        odds:       settlementOdds,
        stake:      tip.stake,
        tier:       tip.tier || 'pro',
        result:     won ? 'WON' : 'LOST',
        profit_loss: pl,
        running_pl:  currentRunningPL,
        settled_at:  new Date().toISOString(),
        confidence: tip.confidence || 0,
      });

      console.log(`${won ? '✅ WON' : '❌ LOST'}: [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team} — ${tip.selection} @ ${settlementOdds} (${pl >= 0 ? '+' : ''}${pl}u)`);
      count++; dirty = true;

    } catch(e) { console.error(`Settle error [${tip.tip_ref}]:`, e.message); }
  }

  // Recompute stats ONCE, not once per settled tip.
  if (dirty) await updateStatsCache();

  // Settle daily accas
  try {
    const { data: pendingAccas } = await supabase.from('daily_accas').select('*').eq('result', 'pending');
    for (const acca of (pendingAccas || [])) {
      const tipRefs = (acca.selections || []).map(s => s.tip_ref).filter(Boolean);
      if (!tipRefs.length) continue;
      const { data: legTips } = await supabase.from('tips').select('tip_ref, status, odds, best_odds, advised_odds').in('tip_ref', tipRefs);
      if (!legTips || legTips.length < tipRefs.length) {
        console.warn(`⚠️ Acca ${acca.date}: ${tipRefs.length - (legTips?.length || 0)} leg tip(s) missing from tips table — cannot settle, needs manual review`);
        continue;
      }
      const allSettled = legTips.every(t => ['won','lost','void'].includes(t.status));
      if (!allSettled) continue;
      const activeLegs = legTips.filter(t => t.status !== 'void');
      if (!activeLegs.length) {
        await supabase.from('daily_accas').update({ result: 'VOID', profit_loss: 0 }).eq('id', acca.id);
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
      await supabase.from('daily_accas').update({ result, profit_loss: pl }).eq('id', acca.id);
      console.log(`📋 Acca ${acca.date} settled: ${result} (${pl >= 0 ? '+' : ''}${pl}u)`);
    }
  } catch(e) { console.error('Acca settlement error:', e.message); }

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
    const data = await selectAll('results_history', 'result, profit_loss, stake');
    if (!data.length) return;
    const won   = data.filter(r => r.result === 'WON').length;
    const lost  = data.filter(r => r.result === 'LOST').length;
    const total = won + lost;
    const pl    = data.reduce((s,r) => s + parseFloat(r.profit_loss || 0), 0);
    const stk   = data.reduce((s,r) => s + parseFloat(r.stake ?? 1), 0);
    await supabase.from('stats_cache').update({
      total_tips:   total, total_won: won, total_lost: lost,
      win_rate:     total > 0 ? parseFloat((won/total*100).toFixed(1)) : 0,
      total_pl:     parseFloat(pl.toFixed(2)),
      total_staked: parseFloat(stk.toFixed(2)),
      roi:          stk > 0 ? parseFloat((pl/stk*100).toFixed(1)) : 0,
    }).eq('id', 1);
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

  let all = [];
  for (const sport of SPORTS) {
    const events = sofascoreCache.events[sport.key] || [];
    if (!events.length) continue;
    console.log(`Analysing ${sport.league} (${events.length} events from cache)...`);
    const tips = await generateTips(events, sport);
    console.log(`  → ${tips.length} tips`);
    all = all.concat(tips);
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

async function sendEmail({ to, subject, html, type = 'general' }) {
  if (!RESEND_API_KEY) { console.log(`📧 No RESEND key — skipping email to ${to}`); return false; }
  let ok = false;
  try {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: `${FROM_NAME} <${FROM_EMAIL}>`, to, subject, html }),
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

function emailBase(content, userId) {
  const unsubUrl = `${SITE_URL.replace('www.','')}/unsubscribe?token=${generateUnsubToken(userId)}&uid=${userId}`;
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>The Tipster Edge</title></head>
<body style="margin:0;padding:0;background:#07090d;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#07090d;padding:32px 16px;">
<tr><td align="center">
<table width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;">
<tr><td style="padding-bottom:24px;">
<p style="font-family:monospace;font-size:11px;text-transform:uppercase;letter-spacing:4px;color:#18e07a;margin:0;">The Tipster Edge</p>
</td></tr>
<tr><td style="background:#0c0f15;border-radius:10px;padding:28px 24px;">${content}</td></tr>
<tr><td style="padding-top:20px;text-align:center;">
<p style="font-size:11px;color:#2a3a50;margin:0;">© The Tipster Edge · <a href="${unsubUrl}" style="color:#2a3a50;">Unsubscribe</a></p>
</td></tr>
</table>
</td></tr>
</table></body></html>`;
}

function buildWelcomeEmail({ userId, firstName }) {
  const g = firstName || 'there';
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
    : ` <span style="color:#4a5a70;">· now ${live.toFixed(2)}</span>`;
}

// Stated once per email so the headline number needs no per-tip label.
const PRICE_FOOTNOTE = '<p style="font-size:10px;color:#4a5a70;margin:18px 0 0;text-align:center;line-height:1.5;">Odds shown are the prices advised at publication — the same prices our published results are settled at.</p>';

function buildProEmail({ tip, allTips, userId, firstName }) {
  const g    = firstName || 'there';
  const edge = parseFloat(tip.model_edge != null ? tip.model_edge : 0).toFixed(1);
  const ec   = parseFloat(edge) >= 0 ? '#18e07a' : '#ff3d5a';
  const es   = parseFloat(edge) >= 0 ? `+${edge}%` : `${edge}%`;
  const extras = allTips.slice(1, 9).map(t => {
    const te  = parseFloat(t.model_edge != null ? t.model_edge : 0).toFixed(1);
    const tec = parseFloat(te) >= 0 ? '#18e07a' : '#ff3d5a';
    return `<tr style="border-top:1px solid #1c2535;"><td style="padding:10px 14px;">
<p style="font-size:10px;color:#4a5a70;margin:0 0 2px;font-family:monospace;text-transform:uppercase;">${t.sport} · ${t.league} · [${t.tip_ref}]</p>
<p style="font-size:13px;font-weight:700;color:#dde6f0;margin:0 0 2px;">${t.home_team} vs ${t.away_team}</p>
<p style="font-size:12px;color:#18e07a;margin:0;">${t.selection} <span style="color:#4a5a70;">@</span> <span style="color:#f0b429;font-family:monospace;">${advisedPrice(t).toFixed(2)}</span>${livePriceNote(t)}</p>
</td><td style="padding:10px 14px;text-align:right;white-space:nowrap;">
<p style="font-family:monospace;font-size:11px;color:${tec};margin:0;">${parseFloat(te)>=0?'+':''}${te}% edge</p>
<p style="font-family:monospace;font-size:11px;color:#4a5a70;margin:2px 0;">${t.confidence}% conf · ${t.stake}u</p>
</td></tr>`;
  }).join('');
  const content = `
<p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#f0b429;margin:0 0 10px;">Pro Early Access · 07:00</p>
<h1 style="font-size:20px;font-weight:800;color:#dde6f0;margin:0 0 4px;">Morning, ${g}. Here's your full card.</h1>
<p style="font-size:12px;color:#4a5a70;margin:0 0 22px;">${allTips.length} tips ready.</p>
<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#18e07a;margin:0 0 8px;">Best Pick · [${tip.tip_ref}]</p>
<table width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:16px;">
<tr><td style="padding:14px 16px;">
<p style="font-size:11px;color:#4a5a70;margin:0 0 3px;font-family:monospace;text-transform:uppercase;">${tip.sport} · ${tip.league}</p>
<p style="font-size:17px;font-weight:800;color:#dde6f0;margin:0 0 2px;">${tip.home_team} vs ${tip.away_team}</p>
<p style="font-size:14px;color:#18e07a;margin:0;">${tip.selection}</p>
</td><td style="padding:14px 16px;text-align:right;">
<p style="font-family:monospace;font-size:26px;font-weight:700;color:#f0b429;margin:0;line-height:1;">${advisedPrice(tip).toFixed(2)}</p>
${livePriceNote(tip, { block: true })}
<p style="font-family:monospace;font-size:10px;color:${ec};margin:4px 0 0;">${es} edge · ${tip.stake}u stake</p>
</td></tr></table>
${extras ? `<table width="100%" cellpadding="0" cellspacing="0" style="background:#0c0f15;border:1px solid #1c2535;border-radius:7px;margin-bottom:20px;">${extras}</table>` : ''}
<div style="text-align:center;"><a href="${SITE_URL}/#tips" style="display:inline-block;background:#f0b429;color:#07090d;font-size:13px;font-weight:700;padding:12px 28px;border-radius:5px;text-decoration:none;">View Full Card</a></div>${PRICE_FOOTNOTE}`;
  return emailBase(content, userId);
}

function buildFreeEmail({ tip, proTipCount, userId, firstName }) {
  const g    = firstName || 'there';
  const edge = parseFloat(tip.model_edge != null ? tip.model_edge : 0).toFixed(1);
  const ec   = parseFloat(edge) >= 0 ? '#18e07a' : '#ff3d5a';
  const es   = parseFloat(edge) >= 0 ? `+${edge}%` : `${edge}%`;
  const content = `
<p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#18e07a;margin:0 0 10px;">Bet of the Day</p>
<h1 style="font-size:20px;font-weight:800;color:#dde6f0;margin:0 0 4px;">Morning, ${g}.</h1>
<p style="font-size:12px;color:#4a5a70;margin:0 0 22px;">Your daily pick · Ref: <span style="font-family:monospace;">[${tip.tip_ref || '-'}]</span></p>
<table width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:16px;">
<tr><td style="padding:14px 16px;">
<p style="font-size:11px;color:#4a5a70;margin:0 0 3px;font-family:monospace;text-transform:uppercase;">${tip.sport} · ${tip.league}</p>
<p style="font-size:17px;font-weight:800;color:#dde6f0;margin:0 0 2px;">${tip.home_team} vs ${tip.away_team}</p>
<p style="font-size:14px;color:#18e07a;margin:0;">${tip.selection}</p>
</td><td style="padding:14px 16px;text-align:right;">
<p style="font-family:monospace;font-size:26px;font-weight:700;color:#f0b429;margin:0;line-height:1;">${advisedPrice(tip).toFixed(2)}</p>
${livePriceNote(tip, { block: true })}
<p style="font-family:monospace;font-size:10px;color:${ec};margin:4px 0 0;">${es} edge</p>
</td></tr></table>
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0c0f15;border:1px solid rgba(240,180,41,0.25);border-radius:7px;margin-bottom:20px;">
<tr><td style="padding:16px 18px;">
<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#f0b429;margin:0 0 8px;">Pro members got ${proTipCount} more tips at 07:00</p>
<a href="${SITE_URL}/#pricing" style="display:inline-block;background:#f0b429;color:#07090d;font-size:12px;font-weight:700;padding:9px 20px;border-radius:4px;text-decoration:none;">Go Pro — £9.99/mo</a>
</td></tr></table>
<div style="text-align:center;"><a href="${SITE_URL}/#tips" style="display:inline-block;background:#18e07a;color:#07090d;font-size:13px;font-weight:700;padding:12px 28px;border-radius:5px;text-decoration:none;">View Today's Tips</a></div>${PRICE_FOOTNOTE}`;
  return emailBase(content, userId);
}

function buildSaturdayEmail({ selections, combinedOdds, reasoning, userId }) {
  const rows = selections.map((s, i) => `<tr style="${i>0?'border-top:1px solid #1c2535;':''}"><td style="padding:11px 14px;">
<p style="font-size:12px;font-weight:700;color:#dde6f0;margin:0 0 2px;">${s.match}</p>
<p style="font-family:monospace;font-size:11px;color:#18e07a;margin:0;">${s.selection} <span style="color:#4a5a70;">@ ${parseFloat(s.odds).toFixed(2)}</span></p>
</td></tr>`).join('');
  const content = `
<p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#18e07a;margin:0 0 10px;">Weekend Accumulator</p>
<h1 style="font-size:22px;font-weight:800;color:#dde6f0;margin:0 0 6px;">Saturday's Best ${selections.length}-Fold</h1>
<p style="font-size:12px;color:#4a5a70;margin:0 0 24px;">Combined odds: <span style="font-family:monospace;font-weight:700;color:#f0b429;font-size:15px;">${parseFloat(combinedOdds).toFixed(2)}</span></p>
<table width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:20px;">${rows}</table>
<div style="text-align:center;"><a href="${SITE_URL}/#tips" style="display:inline-block;background:#18e07a;color:#07090d;font-size:13px;font-weight:700;padding:13px 28px;border-radius:5px;text-decoration:none;">View Full Weekend Card</a></div>`;
  return emailBase(content, userId);
}

async function getSubscribers(type = 'daily', tier = 'all') {
  const col = type === 'saturday' ? 'email_saturday' : 'email_daily';
  let q = supabase.from('users').select('id, email, first_name, subscription_status').eq('email_opt_in', true).eq(col, true);
  if (tier === 'pro')  q = q.eq('subscription_status', 'pro');
  if (tier === 'free') q = q.neq('subscription_status', 'pro');
  const { data } = await q;
  return data || [];
}

async function getTodaysTips(limit = 15) {
  // Render runs in UTC; setHours anchors to the server's day, which is an hour
  // off the UK day during BST. A fixture kicking off between 00:00 and 01:00 UK
  // fell outside "today" and never made the card.
  const s = ukDayStart();
  const e = new Date(s.getTime() + 24 * 3600000 - 1);

  // Over-fetch, because the informational picks filtered out below would
  // otherwise eat into the limit.
  const { data } = await supabase.from('tips').select('*').eq('status','pending')
    .gte('event_time', s.toISOString()).lte('event_time', e.toISOString())
    .order('confidence', { ascending: false }).limit(limit * 3);

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
  const staked = (data || []).filter(t => parseFloat(t.stake ?? 1) > 0);
  return staked.slice(0, limit);
}

async function getBetOfTheDay() {
  const today = new Date().toISOString().split('T')[0];
  const { data: ov } = await supabase.from('email_overrides').select('*').eq('date', today).eq('type','daily').maybeSingle();
  if (ov?.bet_selection) return { home_team: ov.bet_match?.split(' vs ')[0]||'Home', away_team: ov.bet_match?.split(' vs ')[1]||'Away', selection: ov.bet_selection, odds: ov.bet_odds||1.8, confidence: ov.bet_confidence||80, stake: 1, notes: ov.bet_reasoning||'', league:'', sport:'Football', tip_ref:'OVERRIDE' };
  const tips = await getTodaysTips(1);
  return tips[0] || null;
}

async function getSaturdayAcca() {
  const today = new Date().toISOString().split('T')[0];
  const { data: ov } = await supabase.from('email_overrides').select('*').eq('date', today).eq('type','saturday').maybeSingle();
  if (ov?.acca_selections) return { selections: ov.acca_selections, combinedOdds: ov.acca_combined_odds||0, reasoning: ov.acca_reasoning||'' };
  const dayStart = ukDayStart();
  const s = new Date(dayStart.getTime() + 6 * 3600000);          // 06:00 UK
  const e = new Date(dayStart.getTime() + 24 * 3600000 - 1);     // 23:59:59 UK
  const { data: tips } = await supabase.from('tips').select('*').eq('status','pending').eq('sport','Football')
    .gte('event_time', s.toISOString()).lte('event_time', e.toISOString())
    .gte('confidence', 72).order('confidence', { ascending: false }).limit(4);
  if (!tips || tips.length < 3) return null;
  const sels = tips.map(t => ({ match: `${t.home_team} vs ${t.away_team}`, selection: t.selection, odds: advisedPrice(t) }));
  return { selections: sels, combinedOdds: sels.reduce((a,s) => a * parseFloat(s.odds), 1), reasoning: `${sels.length} high-confidence selections from today's card.` };
}

async function sendProEmails() {
  console.log('📧 Pro dispatch 07:00...');
  const tips = await getTodaysTips(15);
  if (!tips.length) return;
  const subs = await getSubscribers('daily', 'pro');
  let sent = 0;
  for (const u of subs) {
    const html = buildProEmail({ tip: tips[0], allTips: tips, userId: u.id, firstName: u.first_name });
    if (await sendEmail({ to: u.email, subject: `${u.first_name?u.first_name+', ':''}Pro Early Access | ${tips.length} tips ready`, html, type: 'pro_daily' })) sent++;
    await new Promise(r => setTimeout(r, 100));
  }
  console.log(`📧 Pro: ${sent}/${subs.length}`);
}

async function sendDailyEmails() {
  console.log('📧 Free dispatch 08:30...');
  const tip = await getBetOfTheDay();
  if (!tip) return;
  const all  = await getTodaysTips(15);
  const subs = await getSubscribers('daily', 'free');
  let sent = 0;
  for (const u of subs) {
    const html = buildFreeEmail({ tip, proTipCount: Math.max(all.length - 1, 0), userId: u.id, firstName: u.first_name });
    if (await sendEmail({ to: u.email, subject: `${u.first_name?u.first_name+', ':''}Today's Bet of the Day`, html, type: 'daily' })) sent++;
    await new Promise(r => setTimeout(r, 100));
  }
  console.log(`📧 Free: ${sent}/${subs.length}`);
}

async function sendSaturdayEmails() {
  console.log('📧 Saturday acca dispatch...');
  const acca = await getSaturdayAcca();
  if (!acca) return;
  const subs = await getSubscribers('saturday');
  let sent = 0;
  for (const u of subs) {
    const html = buildSaturdayEmail({ ...acca, userId: u.id });
    if (await sendEmail({ to: u.email, subject: `${u.first_name?u.first_name+', ':''}Saturday's ${acca.selections.length}-Fold | ${parseFloat(acca.combinedOdds).toFixed(2)} combined odds`, html, type: 'saturday' })) sent++;
    await new Promise(r => setTimeout(r, 100));
  }
  console.log(`📧 Saturday: ${sent}/${subs.length}`);
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
      .select('id, confidence')
      .eq('status', 'pending')
      .gte('event_time', s).lte('event_time', e)
      .order('confidence', { ascending: false });
    if (readErr) { console.error('tagFreeTips read:', readErr.message); return; }
    if (!tips || !tips.length) return;

    const freeIds = tips.slice(0, FREE_TIPS_PER_DAY).map(t => t.id);
    const proIds  = tips.slice(FREE_TIPS_PER_DAY).map(t => t.id);

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
    const now = new Date();
    const ukTomorrow = new Date(now.getTime() + 86400000).toLocaleDateString('en-CA', { timeZone: 'Europe/London' });
    const s = ukTomorrow + 'T00:00:00Z';
    const e = ukTomorrow + 'T23:59:59Z';

    const { data: existing } = await supabase.from('tips').select('id')
      .eq('is_best_bet', true).gte('event_time', s).lte('event_time', e).maybeSingle();
    if (existing) return;

    // Fetch candidates — select model_edge and quality_score for ranking
    const { data: tips } = await supabase.from('tips')
      .select('id, tip_ref, home_team, away_team, confidence, odds, model_edge, quality_score')
      .eq('status', 'pending')
      .gte('event_time', s)
      .lte('event_time', e);

    if (!tips?.length) return;

    // Rank by real edge + quality score composite (item 10)
    // 70% model_edge (normalised to 25% max) + 30% quality_score
    const ranked = tips
      .map(t => {
        const edge = parseFloat(t.model_edge || 0);
        const qs   = parseFloat(t.quality_score || 0);
        const normEdge = Math.min(1, Math.max(0, edge) / 25);
        return { ...t, composite: normEdge * 0.70 + qs * 0.30 };
      })
      .sort((a, b) => b.composite - a.composite);

    const best = ranked[0];
    await supabase.from('tips').update({ is_best_bet: true }).eq('id', best.id);
    console.log(`🏆 Best bet tagged [${ukTomorrow}]: [${best.tip_ref}] ${best.home_team} vs ${best.away_team} (edge: ${best.model_edge}% qs: ${best.quality_score})`);
  } catch(e) { console.error('tagDailyBestBet error:', e.message); }
}

async function generateDailyAcca() {
  try {
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'Europe/London' });
    const { data: existing } = await supabase.from('daily_accas').select('id').eq('date', today).maybeSingle();
    if (existing) return { skipped: true };
    const s = today + 'T00:00:00Z';
    const e = today + 'T23:59:59Z';
    const { data: tips } = await supabase.from('tips').select('*').eq('status', 'pending').gte('event_time', s).lte('event_time', e).gte('confidence', 84).order('confidence', { ascending: false }).limit(5);
    if (!tips || tips.length < 3) return { generated: false, reason: 'insufficient_tips' };
    const legs = tips.slice(0, 5);
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

async function processAdminJobs() {
  try {
    const { data: jobs } = await supabase.from('admin_jobs').select('*').eq('status','pending').order('created_at',{ascending:true}).limit(10);
    if (!jobs?.length) return;
    for (const job of jobs) {
      await supabase.from('admin_jobs').update({ status: 'processing' }).eq('id', job.id);
      try {
        const p = JSON.parse(job.payload || '{}');
        if (job.job_type === 'test_email')        { const r = await sendTestEmail(p.to, p.type||'daily'); await supabase.from('admin_jobs').update({ status: r.success?'done':'failed', result: JSON.stringify(r) }).eq('id', job.id); }
        else if (job.job_type === 'send_daily')    { await sendDailyEmails();    await supabase.from('admin_jobs').update({ status: 'done' }).eq('id', job.id); }
        else if (job.job_type === 'send_saturday') { await sendSaturdayEmails(); await supabase.from('admin_jobs').update({ status: 'done' }).eq('id', job.id); }
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
const STRIPE_PUBLISHABLE_KEY = process.env.STRIPE_PUBLISHABLE_KEY || '';

async function stripeRequest(path, method = 'GET', body = null) {
  if (!STRIPE_SECRET_KEY) return null;
  const opts = { method, headers: { 'Authorization': `Bearer ${STRIPE_SECRET_KEY}`, 'Content-Type': 'application/x-www-form-urlencoded' } };
  if (body) opts.body = new URLSearchParams(body).toString();
  try {
    const res = await fetch(`https://api.stripe.com/v1${path}`, opts);
    const data = await res.json();
    if (!res.ok) { console.error('Stripe error:', data.error?.message); return null; }
    return data;
  } catch(e) { console.error('Stripe error:', e.message); return null; }
}

async function createCheckoutSession(userId, email, priceId, plan) {
  return stripeRequest('/checkout/sessions', 'POST', {
    'mode': 'subscription', 'customer_email': email,
    'line_items[0][price]': priceId, 'line_items[0][quantity]': '1',
    'success_url': 'https://thetipsteredge.com/account.html?upgraded=1',
    'cancel_url': 'https://thetipsteredge.com/#pricing',
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
async function updateUserByCustomer(customerId, patch, label) {
  if (!customerId) { console.error(`Stripe ${label}: event carried no customer id`); return; }
  const { data, error } = await supabase.from('users').update(patch).eq('stripe_customer_id', customerId).select('id');
  if (error) { console.error(`Stripe ${label} update failed:`, error.message); return; }
  if (!data || !data.length) console.warn(`⚠️ Stripe ${label}: no user with stripe_customer_id=${customerId} — not applied`);
}

async function handleStripeWebhook(event) {
  console.log('Stripe:', event.type);
  switch(event.type) {
    case 'checkout.session.completed': {
      const s = event.data.object;
      const uid = s.metadata?.user_id;
      if (!uid) break;
      await supabase.from('users').update({ subscription_status:'pro', stripe_customer_id: s.customer, stripe_subscription_id: s.subscription }).eq('id', uid);
      const { data: u } = await supabase.from('users').select('email,first_name').eq('id', uid).single();
      if (u) await sendEmail({ to: u.email, subject: `Welcome to The Tipster Pro, ${u.first_name||'there'}`, html: buildWelcomeEmail({ userId: uid, firstName: u.first_name }), type: 'welcome_pro' });
      break;
    }
    case 'customer.subscription.updated': {
      const sub = event.data.object;
      const mapped = mapSubStatus(sub.status);
      if (!mapped) break;
      // A subscription cancelled at period end stays 'active' until it lapses,
      // so access correctly continues until subscription.deleted arrives.
      const patch = { subscription_status: mapped };
      if (mapped === 'free') patch.stripe_subscription_id = null;
      await updateUserByCustomer(sub.customer, patch, `subscription.updated(${sub.status})`);
      break;
    }
    case 'customer.subscription.deleted':
      await updateUserByCustomer(event.data.object.customer, { subscription_status:'free', stripe_subscription_id: null }, 'subscription.deleted');
      break;
    case 'invoice.payment_failed':
      await updateUserByCustomer(event.data.object.customer, { subscription_status:'past_due' }, 'payment_failed');
      break;
    case 'invoice.payment_succeeded':
      if (event.data.object.billing_reason === 'subscription_cycle')
        await updateUserByCustomer(event.data.object.customer, { subscription_status:'pro' }, 'payment_succeeded');
      break;
  }
}

// ═══════════════════════════════════════════════════════════════
// SCHEDULER
// ═══════════════════════════════════════════════════════════════

let lastProDate = '', lastFreeDate = '', lastSatDate = '', lastMorningDate = '', lastMiddayDate = '', lastEveningDate = '';

function startScheduler() {
  setInterval(async () => {
    const uk    = ukTime();
    const h     = uk.getHours();
    const m     = uk.getMinutes();
    const today = uk.toDateString();
    const isSat = uk.getDay() === 6;

    // Morning fetch: 06:00 UK — full data pull
    if (h === 6 && m < 5 && lastMorningDate !== today) {
      lastMorningDate = today;
      await morningFetch();
      await tagDailyBestBet();
    }

    // Pro emails + acca: 07:00
    if (h === 7 && m < 5 && lastProDate !== today) {
      lastProDate = today;
      await tagDailyBestBet();
      await sendProEmails();
      await generateDailyAcca();
    }

    // Free emails: 08:30
    if (h === 8 && m >= 30 && m < 35 && lastFreeDate !== today) {
      lastFreeDate = today;
      await sendDailyEmails();
    }

    // Saturday acca: 08:00 Sat
    if (isSat && h === 8 && m < 5 && lastSatDate !== today) {
      lastSatDate = today;
      await sendSaturdayEmails();
    }

    // Midday odds refresh: 13:00
    if (h === 13 && m < 5 && lastMiddayDate !== today) {
      lastMiddayDate = today;
      await middayOddsRefresh();
    }

    // Evening goalie + lineup refresh: 21:00 UK
    // NHL starters confirmed ~4pm ET = 9pm UK, football lineups confirmed ~1-2hr pre-kickoff
    if (h === 21 && m < 5 && lastEveningDate !== today) {
      lastEveningDate = today;
      console.log('🥅 Evening goalie + lineup refresh...');
      Object.keys(nhlGoalieCache).forEach(k => delete nhlGoalieCache[k]);
      nhlGoalieCacheDate = '';
      await fetchNHLGoalieData();
      await fetchLineupsForToday();
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

function isRateLimited(ip) {
  const now   = Date.now();
  const entry = rateLimitMap.get(ip) || { count: 0, start: now };
  if (now - entry.start > RATE_WINDOW) { entry.count = 1; entry.start = now; }
  else entry.count++;
  rateLimitMap.set(ip, entry);
  if (rateLimitMap.size > 1000) {
    for (const [key, val] of rateLimitMap) {
      if (now - val.start > RATE_WINDOW) rateLimitMap.delete(key);
    }
  }
  return entry.count > RATE_LIMIT;
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
      if (size > maxBytes) { reject(new Error('Body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

http.createServer(async (req, res) => {
  const url    = new URL(req.url, 'http://localhost');
  const origin = req.headers['origin'] || '';
  const allowedOrigins = ['https://www.thetipsteredge.com', 'https://thetipsteredge.com', 'https://the-tipster.vercel.app'];
  const allowOrigin = allowedOrigins.includes(origin) ? origin : allowedOrigins[0];
  const cors = { 'Access-Control-Allow-Origin': allowOrigin, 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type, Authorization', 'Vary': 'Origin' };

  if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return; }

  const clientIp = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress || '';
  if (isRateLimited(clientIp)) { res.writeHead(429, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Too many requests' })); return; }

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

  if (url.pathname === '/admin/test-email') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    const r = await sendTestEmail(url.searchParams.get('to'), url.searchParams.get('type')||'daily');
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify(r)); return;
  }

  if (url.pathname === '/admin/send-daily') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    sendDailyEmails();
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true })); return;
  }

  if (url.pathname === '/admin/send-saturday') {
    if (!safeEqual(adminKey, ADMIN_KEY)) { res.writeHead(403); res.end('Forbidden'); return; }
    sendSaturdayEmails();
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true })); return;
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
      await supabase.from('users').update({ email_opt_in: false }).eq('id', uid);
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end('<html><body style="font-family:sans-serif;text-align:center;padding:60px;background:#07090d;color:#dde6f0;"><h2>Unsubscribed</h2><p>You have been removed from all emails.</p><a href="https://www.thetipsteredge.com/account.html" style="color:#18e07a;">Manage preferences</a></body></html>');
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
          const { data: profile } = await supabase.from('users').select('subscription_status, stripe_subscription_id').eq('id', user.id).single();

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
      const { data: allTips } = await supabase.from('tips').select('*').gte('event_time', today.toISOString()).lte('event_time', tom.toISOString()).eq('status', 'pending').order('confidence', { ascending: false }).limit(50);
      const tips = (allTips || []).map((tip, i) => {
        const isLocked = !isPro && i >= 3;
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

        const { data: existing } = await supabase.from('users').select('id').eq('id', caller.id).maybeSingle();
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
        const { data: user } = await supabase.from('users').select('stripe_customer_id, stripe_subscription_id, subscription_status').eq('id', userId).single();
        if (!user?.stripe_subscription_id) { res.writeHead(200, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ isPro: false })); return; }
        const sub = await stripeRequest(`/subscriptions/${user.stripe_subscription_id}`);
        const isPro = sub && (sub.status === 'active' || sub.status === 'trialing');
        if (!isPro && user.subscription_status === 'pro') await supabase.from('users').update({ subscription_status: 'free' }).eq('id', userId);
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ isPro: !!isPro }));
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
      res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ received: true }));
      handleStripeWebhook(event).catch(e => console.error('Webhook error:', e.message));
    })();
    return;
  }

  if (url.pathname === '/stripe/checkout' && req.method === 'POST') {
    (async () => {
      try {
        let raw;
        try { raw = await readBody(req, 16384); }
        catch(e) { res.writeHead(413, cors); res.end('Payload too large'); return; }
        const { userId, email, plan } = JSON.parse(raw.toString('utf8'));
        if (!userId || !email || !plan) { res.writeHead(400, cors); res.end('Missing params'); return; }

        // Refuse a second checkout for someone who is already subscribed.
        // createCheckoutSession passes customer_email rather than customer, so
        // Stripe mints a NEW customer each time; the webhook then overwrites
        // stripe_customer_id/stripe_subscription_id with the new pair and the
        // original subscription keeps billing with nothing in the app pointing
        // at it. The user is charged twice and cancelling only stops the second.
        const { data: existing } = await supabase.from('users')
          .select('stripe_subscription_id').eq('id', userId).maybeSingle();
        if (existing?.stripe_subscription_id) {
          const current = await stripeRequest(`/subscriptions/${existing.stripe_subscription_id}`);
          if (current && (current.status === 'active' || current.status === 'trialing')) {
            res.writeHead(409, { ...cors, 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'You already have an active subscription.', alreadySubscribed: true }));
            return;
          }
        }

        const session = await createCheckoutSession(userId, email, plan === 'annual' ? STRIPE_PRICE_ANNUAL : STRIPE_PRICE_MONTHLY, plan);
        if (!session) { res.writeHead(500, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Failed' })); return; }
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ url: session.url }));
      } catch(e) { res.writeHead(500, cors); res.end('Server error'); }
    })();
    return;
  }

  if (url.pathname === '/stripe/config') {
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ publishableKey: STRIPE_PUBLISHABLE_KEY })); return;
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

        const { data: user } = await supabase.from('users').select('stripe_customer_id').eq('id', caller.id).single();
        if (!user?.stripe_customer_id) { res.writeHead(404, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'No billing account' })); return; }

        const session = await stripeRequest('/billing_portal/sessions', 'POST', { customer: user.stripe_customer_id, return_url: 'https://thetipsteredge.com/account.html' });
        if (!session) { res.writeHead(502, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Failed' })); return; }
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ url: session.url }));
      } catch(e) { res.writeHead(500, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Server error' })); }
    })();
    return;
  }

  res.writeHead(404); res.end('Not found');

}).listen(process.env.PORT || 3000, () => {
  console.log(`🟢 HTTP server on port ${process.env.PORT || 3000}`);
});

// ═══════════════════════════════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════════════════════════════

(async () => {
  console.log(`\n🟢 The Tipster Engine v9.9 starting...`);
  console.log(`   Season: ${currentSeason()}/${currentSeason()+1}`);
  console.log(`   Data source: Sofascore (RapidAPI Pro)`);
  console.log(`   Schedule: Morning fetch 06:00 | Midday refresh 13:00 | Tips every 15min`);

  // On startup, run morning fetch immediately to populate cache
  await morningFetch();
  await settleResults().catch(e => console.error('Startup settle error:', e.message));

  // Start 15-min tip generation cycle
  setInterval(runEngine, 15 * 60 * 1000);
  await runEngine();

  startScheduler();
  setInterval(processAdminJobs, 30 * 1000);
  processAdminJobs();
})();

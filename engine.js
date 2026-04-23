// ============================================================
// THE TIPSTER EDGE — Engine v7 (Strict Rules Edition)
// ============================================================
// Schedule:
//   Odds fetch     → every 15 minutes
//   Results settle → every 60 minutes
//   Form data      → once daily at 06:00 UK
//   Stats cache    → recalculates on each settlement
//   Emails         → Pro 07:00, Free 08:30, Saturday 08:00 UK
//
// STRICT RULES ENGINE — target ROI: 30%+
//   Sports:  Ice Hockey (primary), Basketball (primary), Football (secondary)
//   Markets: H2H win markets + Overs (secondary, stricter rules)
//   Removed: Baseball, NFL, Unders, all props
//   Odds:    1.40–2.20 core | 2.21–2.50 elite only | reject outside
//   Edge:    H2H ≥ 8% | Overs ≥ 12% | Elite grade requires ≥ 10% (H2H) / 14% (Overs)
//   Grades:  A+ (2–2.5u) | A (1.5u) | below A = no bet
//   Line:    Reject if odds moved ≥ 0.10 against; allow if improved
// ============================================================

const { createClient } = require('@supabase/supabase-js');
const { runFootballLab, settleFootballLab } = require('./football_lab');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
const crypto = require('crypto');

// ─── CREDENTIALS ─────────────────────────────────────────────
const SUPABASE_URL         = process.env.SUPABASE_URL || 'https://eyhlzzaaxrwisrtwyoyh.supabase.co';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
if (!SUPABASE_SERVICE_KEY) throw new Error('FATAL: SUPABASE_SERVICE_KEY env var is not set');
const ODDS_API_KEY         = process.env.ODDS_API_KEY || '';
if (!ODDS_API_KEY) throw new Error('FATAL: ODDS_API_KEY env var is not set');
const ODDS_BASE            = 'https://api.the-odds-api.com/v4';
const API_FOOTBALL_KEY     = process.env.API_FOOTBALL_KEY || '';
const API_FOOTBALL_BASE    = 'https://v3.football.api-sports.io';
// BallDontLie: free tier for NBA/NHL injuries + NBA stats
const BDL_API_KEY          = process.env.BDL_API_KEY || ''; // add to Render env vars
// football-data.org: free tier for football standings (Premier League, La Liga etc)
const FOOTBALL_DATA_TOKEN  = process.env.FOOTBALL_DATA_TOKEN || '315a4b3479ba41d0bd6def1a02d2f425';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ─── SPORTS CONFIG ────────────────────────────────────────────
const SPORTS = [
  { key: 'soccer_epl',               name: 'Football',   league: 'Premier League',   leagueId: 39  },
  { key: 'soccer_spain_la_liga',      name: 'Football',   league: 'La Liga',          leagueId: 140 },
  { key: 'soccer_germany_bundesliga', name: 'Football',   league: 'Bundesliga',       leagueId: 78  },
  { key: 'soccer_italy_serie_a',      name: 'Football',   league: 'Serie A',          leagueId: 135 },
  { key: 'soccer_france_ligue_one',   name: 'Football',   league: 'Ligue 1',          leagueId: 61  },
  { key: 'soccer_uefa_champs_league', name: 'Football',   league: 'Champions League', leagueId: 2   },
  { key: 'basketball_nba',            name: 'Basketball', league: 'NBA' },
  // REMOVED: americanfootball_nfl — insufficient model data
  // REMOVED: baseball_mlb — permanently excluded, no edge
  { key: 'icehockey_nhl',             name: 'Ice Hockey', league: 'NHL' },
];

// ── STRICT RULES ENGINE CONSTANTS ────────────────────────────
// These constants define the hard-rule filter applied to ALL tips
// before they are saved. If a tip fails any rule, it is silently dropped.
const MIN_CONFIDENCE    = 78;   // minimum confidence to publish
const MIN_EDGE_PCT      = 8;    // H2H minimum edge %
const OVERS_MIN_EDGE    = 12;   // Overs minimum edge % (stricter than H2H)
const ELITE_H2H_EDGE    = 10;   // H2H edge required for A+ grade
const ELITE_OVERS_EDGE  = 14;   // Overs edge required for A+ grade
const ODDS_MIN          = 1.40; // reject below this
const ODDS_CORE_MAX     = 2.20; // core range ceiling
const ODDS_ELITE_MAX    = 2.50; // absolute ceiling (elite selections only)
const LINE_MOVE_REJECT  = 0.10; // reject if odds moved this much AGAINST selection
const MATRIX_MAX_GOALS  = 8;   // score matrix built 0..8 each side
// Football is SECONDARY — applies stricter edge (+2%) relative to H2H minimum
const FOOTBALL_EDGE_PREMIUM = 2;

// ─── UNIQUE TIP ID ────────────────────────────────────────────
function generateTipRef(sport) {
  const prefixes = { 'Football': 'FB', 'Basketball': 'BB', 'NFL': 'NF', 'Baseball': 'BA', 'Ice Hockey': 'IH' };
  const prefix = prefixes[sport] || 'TT';
  const ts   = Date.now().toString(36).toUpperCase().slice(-4);
  const rand = crypto.randomBytes(3).toString('hex').toUpperCase();
  return `${prefix}-${ts}${rand}`;
}

// ─── HELPERS ─────────────────────────────────────────────────
function nameMatch(a, b) {
  if (!a || !b) return false;
  const clean = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  const ac = clean(a), bc = clean(b);
  return ac === bc || ac.includes(bc) || bc.includes(ac);
}

function ukTime() {
  return new Date(new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' }));
}

// ═══════════════════════════════════════════════════════════════
// [CHANGED] DYNAMIC SEASON HELPER
// Centralises season logic in one place.
// Football seasons start in July/August and run to May/June.
// Leagues that span two calendar years use the year they started.
// e.g. 2024/25 season = season=2024 in API-Football
// Champions League follows the same convention.
// ═══════════════════════════════════════════════════════════════

function currentSeason() {
  const now   = new Date();
  const year  = now.getFullYear();
  const month = now.getMonth() + 1; // 1-12
  // Season rolls over in July (month 7).
  // Before July → still in season that started previous year.
  // July onwards → new season has started.
  return month >= 7 ? year : year - 1;
}

// Single place for all API-Football season usage.
// Pass leagueId if you ever need league-specific overrides in future.
function seasonFor(/* leagueId */) {
  return currentSeason();
}

// ═══════════════════════════════════════════════════════════════
// FORM DATA SYSTEM
// Runs once at 06:00 UK. Fetches last 5 results + goals per team.
// Cached for the day. Max 40 API calls reserved for form.
// After fetching, updates confidence on existing pending tips.
// ═══════════════════════════════════════════════════════════════

const formCache = {};
let formFetchedDate = '';
let formApiCalls = 0;
const MAX_FORM_CALLS = 40;

async function fetchAllFormData() {
  const today = new Date().toISOString().split('T')[0];
  if (formFetchedDate === today) { console.log('📊 Form cache current.'); return; }

  console.log('📊 Fetching form data at 06:00...');
  formApiCalls = 0;

  // Seed already-fetched teams from existing formCache to avoid duplicate calls
  const alreadyFetchedTeams = new Set(Object.keys(formCache).map(k => k.split('_')[0]));

  // Only fetch form for tomorrow's football fixtures that pass bookmaker sanity check
  // getEligibleFootballFormFixtures enforces the MAX_FOOTBALL_FORM_CALLS hard cap
  // and deduplicates teams to control actual call count precisely
  const { data: pendingTips } = await supabase
    .from('tips').select('home_team, away_team, league').eq('status', 'pending').eq('sport', 'Football');

  if (!pendingTips?.length) { formFetchedDate = today; return; }

  // Build mock event objects for the eligible fixture checker
  // We only have team names from pending tips, so sanity check is simplified here
  const pairs = new Set();
  for (const t of pendingTips) {
    const sport = SPORTS.find(s => s.league === t.league);
    if (!sport?.leagueId) continue;
    // Only add if not already cached
    if (!alreadyFetchedTeams.has(t.home_team)) pairs.add(`${t.home_team}||${sport.leagueId}`);
    if (!alreadyFetchedTeams.has(t.away_team)) pairs.add(`${t.away_team}||${sport.leagueId}`);
  }

  let formCallsUsed = 0;
  for (const entry of pairs) {
    if (formCallsUsed >= MAX_FOOTBALL_FORM_CALLS) break;
    const [team, lid] = entry.split('||');
    await fetchTeamForm(team, parseInt(lid));
    formCallsUsed++;
    await new Promise(r => setTimeout(r, 350));
  }

  formFetchedDate = today;
  console.log(`📊 Form fetched. ${formApiCalls} calls used. ${Object.keys(formCache).length} teams cached.`);
  await applyFormToPendingTips();

  // Fetch NBA + NHL injuries alongside form data (same 06:00 run)
  await fetchNBAInjuries(BDL_API_KEY);
  await fetchNHLInjuries(BDL_API_KEY);
  console.log(`🏥 Injury caches updated. ${budgetStatus()}`);
}

// [CHANGED] Uses seasonFor() instead of hardcoded 2024
async function fetchTeamForm(teamName, leagueId) {
  const key = `${teamName}_${leagueId}`;
  if (formCache[key]) return formCache[key];
  if (formApiCalls >= MAX_FORM_CALLS) return null;

  const season = seasonFor(leagueId);

  try {
    formApiCalls++;
    const sr = await fetch(
      `${API_FOOTBALL_BASE}/teams?name=${encodeURIComponent(teamName)}&league=${leagueId}&season=${season}`,
      { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
    );
    if (!sr.ok) return null;
    const sd = await sr.json();
    const team = sd.response?.[0]?.team;
    if (!team) return null;

    formApiCalls++;
    const fr = await fetch(
      `${API_FOOTBALL_BASE}/fixtures?team=${team.id}&league=${leagueId}&season=${season}&last=5&status=FT`,
      { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
    );
    if (!fr.ok) return null;
    const fd = await fr.json();
    const fixtures = fd.response || [];
    if (!fixtures.length) return null;

    let wins = 0, draws = 0, losses = 0, gf = 0, ga = 0;
    const chars = [];
    for (const f of fixtures) {
      const isHome = f.teams.home.id === team.id;
      const tg     = isHome ? f.goals.home : f.goals.away;
      const og     = isHome ? f.goals.away : f.goals.home;
      const winner = isHome ? f.teams.home.winner : f.teams.away.winner;
      gf += tg || 0; ga += og || 0;
      if (winner === true)       { wins++;   chars.push('W'); }
      else if (winner === false) { losses++; chars.push('L'); }
      else                       { draws++;  chars.push('D'); }
    }
    const played = fixtures.length;
    const result = {
      formScore:       (wins * 3 + draws) / (played * 3),
      avgGoalsFor:     gf / played,
      avgGoalsAgainst: ga / played,
      formString:      chars.join(''),
      wins, draws, losses, played,
      teamId: team.id,  // store for reuse in team stats fetch
    };
    formCache[key] = result;
    console.log(`  📊 ${teamName} [${season}]: ${result.formString} | ${result.avgGoalsFor.toFixed(1)} scored, ${result.avgGoalsAgainst.toFixed(1)} conceded`);
    return result;
  } catch(e) { console.error(`Form error ${teamName}:`, e.message); return null; }
}

// [CHANGED] applyFormToPendingTips now passes market type so form
// adjustment is only applied where it makes sense.
async function applyFormToPendingTips() {
  const { data: tips } = await supabase.from('tips').select('*').eq('status','pending').eq('sport','Football');
  if (!tips?.length) return;
  let updated = 0;
  for (const tip of tips) {
    const sport = SPORTS.find(s => s.league === tip.league);
    if (!sport?.leagueId) continue;
    const hf = formCache[`${tip.home_team}_${sport.leagueId}`];
    const af = formCache[`${tip.away_team}_${sport.leagueId}`];
    if (!hf && !af) continue;

    // Determine market type from selection text
    const sel = (tip.selection || '').toLowerCase();
    const isOver  = sel.startsWith('over');
    const isUnder = sel.startsWith('under');
    const isDraw  = sel === 'draw';
    const isHome  = !isDraw && !isOver && !isUnder && nameMatch(tip.selection.replace(/ win$/i,'').trim(), tip.home_team);

    const newConf  = footballFormAdj(tip.confidence, hf, af, { isHome, isDraw, isOver, isUnder });
    const newNotes = appendFormToNotes(hf, af, tip.notes);
    if (newConf !== tip.confidence) {
      await supabase.from('tips').update({ confidence: newConf, notes: newNotes }).eq('tip_ref', tip.tip_ref);
      updated++;
    }
  }
  if (updated) console.log(`📊 Updated ${updated} pending tips with form data.`);
}

// ─── HELPERS: LEGACY (non-football) ──────────────────────────
// Used only for non-football blendForm calls from analyseH2H.
function blendForm(baseConf, homeForm, awayForm, isHomeTeam) {
  const tf = isHomeTeam ? homeForm : awayForm;
  const of = isHomeTeam ? awayForm : homeForm;
  if (!tf) return baseConf;
  const formAdj = (tf.formScore - 0.5) * 16;
  let goalsAdj = 0;
  if (tf.avgGoalsFor      > 1.8) goalsAdj += 3;
  if (tf.avgGoalsFor      < 0.8) goalsAdj -= 3;
  if (tf.avgGoalsAgainst  < 0.8) goalsAdj += 2;
  if (tf.avgGoalsAgainst  > 2.0) goalsAdj -= 2;
  let oppAdj = 0;
  if (of) {
    if (of.formScore < 0.33) oppAdj += 2;
    if (of.formScore > 0.67) oppAdj -= 2;
  }
  return Math.min(95, Math.max(50, Math.round(baseConf + formAdj + goalsAdj + oppAdj)));
}

function buildNotes(hf, af, base = '') {
  const parts = [base.split('|')[0].trim()];
  if (hf) parts.push(`Home: ${hf.formString} (${hf.avgGoalsFor.toFixed(1)} scored)`);
  if (af) parts.push(`Away: ${af.formString} (${af.avgGoalsFor.toFixed(1)} scored)`);
  return parts.filter(Boolean).join(' | ');
}

// ═══════════════════════════════════════════════════════════════
// FOOTBALL MODEL v5 — Dixon-Coles Poisson + H2H + Weighted Form
// ═══════════════════════════════════════════════════════════════
//
// Architecture:
//   A. League averages (static, centralised)
//   B. API call budget manager (shared across all football fetches)
//   C. Team season stats cache + fetcher (attack/defence strengths)
//   D. H2H record fetcher (last 10 head-to-head results)
//   E. Poisson + Dixon-Coles correction (0..8 goal matrix)
//   F. Bookmaker margin stripping (true implied probability)
//   G. Edge calculation on margin-stripped prices
//   H. Fractional Kelly stake sizing (0.25 Kelly, 0.5-3u range)
//   I. Confidence from edge + form quality signal
//   J. Market-aware form adjustment
//   K. Notes builder (full model diagnostics)
//   L. Main entry: analyseFootballFixture()
//
// API call budget per day (free plan = 100 total):
//   Form data (06:00):    ~40 calls (fetchTeamForm)
//   Season stats:         ~24 calls (2 per fixture team pair, cached)
//   H2H:                  ~12 calls (1 per fixture, cached)
//   Settler:              ~12 calls
//   Buffer:                12 calls
//   Total:               ~100 calls ← within free plan
// ═══════════════════════════════════════════════════════════════

// ── A. LEAGUE AVERAGES ────────────────────────────────────────
// Long-run home/away scoring averages 2020–2024 per league.
// Used as league baseline in attack/defence strength formula.
// Centralised here — review at season start each year.
const LEAGUE_AVERAGES = {
  39:  { homeGoals: 1.53, awayGoals: 1.21 }, // Premier League
  140: { homeGoals: 1.57, awayGoals: 1.14 }, // La Liga
  78:  { homeGoals: 1.68, awayGoals: 1.25 }, // Bundesliga
  135: { homeGoals: 1.49, awayGoals: 1.12 }, // Serie A
  61:  { homeGoals: 1.51, awayGoals: 1.18 }, // Ligue 1
  2:   { homeGoals: 1.64, awayGoals: 1.22 }, // Champions League
};
const LEAGUE_AVG_FALLBACK = { homeGoals: 1.55, awayGoals: 1.18 };
function getLeagueAvg(leagueId) {
  return LEAGUE_AVERAGES[leagueId] || LEAGUE_AVG_FALLBACK;
}

// ── B. API CALL BUDGET MANAGER ────────────────────────────────
// Single counter for all API-Football calls made per day.
// Shared by form fetcher, season stats, H2H, and settler.
// Prevents overrun on free plan (100/day).
// Resets automatically when date changes.
// ─── API CALL BUDGET ─────────────────────────────────────────
// Football = 55/day, NHL = 30/day, NBA = 0 (disabled)
// Total = 85/day — one shared counter across all sports
const API_BUDGET_LIMITS = { football: 55, nhl: 30, nba: 0, total: 85 };
const MAX_FOOTBALL_FORM_CALLS = 10; // hard cap — enforced by call counter not fixture count
const ENABLE_NBA_TIPS = true;       // enabled: stats now sourced from BallDontLie games cache

let apiBudget = {
  date:  '',
  used:  0,
  limit: 85, // total daily ceiling across all sports
};

function budgetCheck(n = 1) {
  const today = new Date().toISOString().split('T')[0];
  if (apiBudget.date !== today) { apiBudget.date = today; apiBudget.used = 0; }
  if (apiBudget.used + n > apiBudget.limit) return false;
  apiBudget.used += n;
  return true;
}

function budgetStatus() {
  return `API budget: ${apiBudget.used}/${apiBudget.limit} used today`;
}

// ── C. TEAM SEASON STATS ──────────────────────────────────────
// Uses football-data.org free tier — no call limits that matter for our use.
// One standings call per league fetches ALL teams at once.
// Returns goalsFor, goalsAgainst, playedGames per team from standings.
// Home/away splits not available on free tier — we use overall averages.
// Guards against < 4 games played (stats unreliable early season).

const FOOTBALL_DATA_BASE  = 'https://api.football-data.org/v4';

// Map API-Football leagueId → football-data.org competition code
const LEAGUE_FD_CODE = {
  39:  'PL',  // Premier League
  140: 'PD',  // La Liga
  78:  'BL1', // Bundesliga
  135: 'SA',  // Serie A
  61:  'FL1', // Ligue 1
  2:   'CL',  // Champions League
};

// Cache: leagueId → { date, teams: { teamName → stats } }
const fdStandingsCache = {};
const teamStatsCache = {};

// Name aliases: Odds API name → football-data.org name
// Add entries here whenever a team name mismatch appears in logs
const FD_NAME_ALIASES = {
  'RC Lens':               'Lens',
  'Paris Saint-Germain':   'Paris Saint-Germain FC',
  'Atletico Madrid':       'Atlético de Madrid',
  'Athletic Club':         'Athletic Club',
  'Manchester City':       'Manchester City FC',
  'Manchester United':     'Manchester United FC',
  'Tottenham Hotspur':     'Tottenham Hotspur FC',
  'Newcastle United':      'Newcastle United FC',
  'West Ham United':       'West Ham United FC',
  'Wolverhampton Wanderers': 'Wolverhampton Wanderers FC',
  'Nottingham Forest':     "Nottingham Forest FC",
  'Brighton and Hove Albion': 'Brighton & Hove Albion FC',
  'Bayer Leverkusen':      'Bayer 04 Leverkusen',
  'Borussia Dortmund':     'Borussia Dortmund',
  'Eintracht Frankfurt':   'Eintracht Frankfurt',
  'AC Milan':              'AC Milan',
  'Inter Milan':           'FC Internazionale Milano',
  'AS Roma':               'AS Roma',
  'Napoli':                'SSC Napoli',
};

async function fetchFDStandings(leagueId) {
  const today = new Date().toISOString().split('T')[0];
  if (fdStandingsCache[leagueId]?.date === today) return fdStandingsCache[leagueId].teams;

  const code = LEAGUE_FD_CODE[leagueId];
  if (!code) return null;

  try {
    const res = await fetch(
      `${FOOTBALL_DATA_BASE}/competitions/${code}/standings`,
      { headers: { 'X-Auth-Token': FOOTBALL_DATA_TOKEN } }
    );
    if (!res.ok) { console.log(`⚠️ FD standings ${code}: ${res.status}`); return null; }
    const data = await res.json();

    const standings = data.standings?.[0]?.table || [];
    const teams = {};
    for (const row of standings) {
      const name = row.team?.name || '';
      if (!name) continue;
      teams[name] = {
        playedGames:  row.playedGames || 0,
        goalsFor:     row.goalsFor    || 0,
        goalsAgainst: row.goalsAgainst || 0,
        // Derive per-game averages
        goalsForPG:     row.playedGames > 0 ? row.goalsFor    / row.playedGames : 0,
        goalsAgainstPG: row.playedGames > 0 ? row.goalsAgainst / row.playedGames : 0,
      };
    }

    fdStandingsCache[leagueId] = { date: today, teams };
    console.log(`⚽ FD standings ${code}: ${standings.length} teams loaded`);
    return teams;
  } catch(e) { console.error(`FD standings error (${code}):`, e.message); return null; }
}

async function fetchTeamSeasonStats(teamName, leagueId) {
  const season   = seasonFor(leagueId);
  const cacheKey = `stats_${teamName}_${leagueId}_${season}`;
  if (teamStatsCache[cacheKey]) return teamStatsCache[cacheKey];

  const teams = await fetchFDStandings(leagueId);
  if (!teams) return null;

  // Match by name — check alias map first, then fuzzy match
  const aliasedName = FD_NAME_ALIASES[teamName] || teamName;
  let match = null;
  for (const [fdName, stats] of Object.entries(teams)) {
    if (nameMatch(fdName, aliasedName) || nameMatch(fdName, teamName)) { match = stats; break; }
  }

  if (!match) {
    console.log(`  ⚠️ FD: no standings match for "${teamName}"`);
    return null;
  }

  if (match.playedGames < 4) {
    console.log(`  ⚠️ ${teamName}: only ${match.playedGames} games — insufficient`);
    return null;
  }

  // football-data.org free tier doesn't split home/away goals.
  // We approximate home/away using overall averages — sufficient for Dixon-Coles.
  const hg = Math.floor(match.playedGames / 2);
  const ag = match.playedGames - hg;
  const result = {
    homeScored:   Math.round(match.goalsForPG     * hg),
    homeConceded: Math.round(match.goalsAgainstPG * hg),
    awayScored:   Math.round(match.goalsForPG     * ag),
    awayConceded: Math.round(match.goalsAgainstPG * ag),
    homeGames:    hg,
    awayGames:    ag,
    teamId:       null, // not needed downstream
  };

  teamStatsCache[cacheKey] = result;
  console.log(`  ⚽ ${teamName}: ${match.goalsForPG.toFixed(2)} GF/gm, ${match.goalsAgainstPG.toFixed(2)} GA/gm [FD]`);
  return result;
}

// ── D. H2H RECORD ─────────────────────────────────────────────
// Fetches last 10 head-to-head results between two teams.
// Returns a modifier (-0.08 to +0.08) applied to lambdaHome/Away.
// Strong historical home dominance slightly lifts home λ.
// Cached per pair per day — 1 API call per fixture pair.
const h2hCache = {};

async function fetchH2HModifier(homeTeamId, awayTeamId) {
  if (!homeTeamId || !awayTeamId) return { homeMod: 0, awayMod: 0 };
  const cacheKey = `h2h_${Math.min(homeTeamId,awayTeamId)}_${Math.max(homeTeamId,awayTeamId)}`;
  if (h2hCache[cacheKey]) return h2hCache[cacheKey];

  if (!budgetCheck(1)) return { homeMod: 0, awayMod: 0 };

  try {
    const res = await fetch(
      `${API_FOOTBALL_BASE}/fixtures/headtohead?h2h=${homeTeamId}-${awayTeamId}&last=10`,
      { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
    );
    if (!res.ok) { h2hCache[cacheKey] = { homeMod: 0, awayMod: 0 }; return h2hCache[cacheKey]; }
    const data     = await res.json();
    const fixtures = data.response || [];
    if (fixtures.length < 3) { h2hCache[cacheKey] = { homeMod: 0, awayMod: 0 }; return h2hCache[cacheKey]; }

    let homeWins = 0, awayWins = 0, draws = 0;
    let homeGF = 0, awayGF = 0;

    for (const f of fixtures) {
      const fHomeId = f.teams?.home?.id;
      // Normalise: treat the current home team as "home" regardless of venue
      const isCurrentHomeActuallyHome = fHomeId === homeTeamId;
      const fHomeGoals = f.goals?.home || 0;
      const fAwayGoals = f.goals?.away || 0;
      const hg = isCurrentHomeActuallyHome ? fHomeGoals : fAwayGoals;
      const ag = isCurrentHomeActuallyHome ? fAwayGoals : fHomeGoals;
      homeGF += hg; awayGF += ag;
      if (hg > ag) homeWins++;
      else if (hg < ag) awayWins++;
      else draws++;
    }

    const total = fixtures.length;
    const homeWinRate = homeWins / total;
    const awayWinRate = awayWins / total;

    // Modifier: if home team wins >60% of H2H → slight λ boost
    // If home team wins <30% of H2H → slight λ reduction
    // Scale: ±0.08 maximum — meaningful but not overwhelming
    const homeMod = Math.max(-0.08, Math.min(0.08, (homeWinRate - 0.40) * 0.25));
    const awayMod = Math.max(-0.08, Math.min(0.08, (awayWinRate - 0.30) * 0.20));

    const modifier = { homeMod, awayMod, homeWins, awayWins, draws, total };
    h2hCache[cacheKey] = modifier;
    console.log(`  H2H ${homeTeamId} vs ${awayTeamId}: ${homeWins}W ${draws}D ${awayWins}L (mod: ${homeMod.toFixed(3)} / ${awayMod.toFixed(3)})`);
    return modifier;
  } catch(e) {
    console.error('H2H error:', e.message);
    h2hCache[cacheKey] = { homeMod: 0, awayMod: 0 };
    return h2hCache[cacheKey];
  }
}

// ── E. POISSON + DIXON-COLES CORRECTION ──────────────────────
// Standard Poisson overestimates probability of low-scoring outcomes
// (0-0, 1-0, 0-1, 1-1). Dixon-Coles (1997) applies a correction
// factor τ (tau) to these four cells only.
//
// τ(h,a,λH,λA,ρ):
//   h=0,a=0: 1 - λH*λA*ρ
//   h=1,a=0: 1 + λA*ρ
//   h=0,a=1: 1 + λH*ρ
//   h=1,a=1: 1 - ρ
//   all other cells: 1 (no correction)
//
// ρ (rho) = correlation parameter. Empirically ~0.10 for football.
// Higher ρ = more correction on low scores.
// Positive ρ reduces 0-0 and 1-1, increases 1-0 and 0-1 slightly.
const DC_RHO = 0.10;

function dixonColesTau(h, a, lambdaH, lambdaA, rho) {
  if (h === 0 && a === 0) return 1 - lambdaH * lambdaA * rho;
  if (h === 1 && a === 0) return 1 + lambdaA * rho;
  if (h === 0 && a === 1) return 1 + lambdaH * rho;
  if (h === 1 && a === 1) return 1 - rho;
  return 1; // no correction for all other scores
}

function poisson(lambda, k) {
  if (lambda <= 0) return k === 0 ? 1 : 0;
  // Log-space calculation to avoid underflow at higher k values
  let logP = -lambda + k * Math.log(lambda);
  for (let i = 1; i <= k; i++) logP -= Math.log(i);
  return Math.exp(logP);
}

// Build score probability matrix with Dixon-Coles correction.
// Matrix is built 0..MATRIX_MAX_GOALS then renormalised to sum=1.
// DC correction applies to the 4 low-score cells only.
function buildScoreMatrix(lambdaHome, lambdaAway) {
  const N = MATRIX_MAX_GOALS;
  const matrix = [];
  let total = 0;

  for (let h = 0; h <= N; h++) {
    matrix[h] = [];
    for (let a = 0; a <= N; a++) {
      // Raw Poisson probability
      const raw = poisson(lambdaHome, h) * poisson(lambdaAway, a);
      // Apply Dixon-Coles τ correction to low-score cells
      const tau = dixonColesTau(h, a, lambdaHome, lambdaAway, DC_RHO);
      const p   = raw * tau;
      matrix[h][a] = Math.max(0, p); // guard against tiny negatives from τ
      total += matrix[h][a];
    }
  }

  // Renormalise: DC correction shifts some mass between cells, total
  // will not be exactly 1. Renormalise so outcomes sum to exactly 1.
  if (total > 0) {
    for (let h = 0; h <= N; h++)
      for (let a = 0; a <= N; a++)
        matrix[h][a] /= total;
  }

  return matrix;
}

// Derive win/draw/loss and market probabilities from matrix
function calcOutcomes(matrix) {
  const N = MATRIX_MAX_GOALS;
  let homeWin = 0, draw = 0, awayWin = 0, over25 = 0, btts = 0;
  for (let h = 0; h <= N; h++) {
    for (let a = 0; a <= N; a++) {
      const p = matrix[h][a];
      if (h > a)          homeWin += p;
      else if (h === a)   draw    += p;
      else                awayWin += p;
      if (h + a > 2.5)    over25  += p;
      if (h > 0 && a > 0) btts    += p;
    }
  }
  // Sanity check: probabilities must sum to ~1
  const total = homeWin + draw + awayWin;
  if (Math.abs(total - 1) > 0.01) {
    console.warn(`⚠️ Matrix probability sum off: ${total.toFixed(4)}`);
  }
  return { homeWin, draw, awayWin, over25, btts };
}

// Fair decimal odds from model probability (no margin)
function fairOdds(prob) {
  if (prob <= 0.001) return 999.0;
  return parseFloat((1 / prob).toFixed(2));
}

// ── F. BOOKMAKER MARGIN STRIPPING ─────────────────────────────
// The odds on a three-way market (1X2) always sum to more than 1
// when converted to implied probabilities. The excess is the margin
// (overround). Without stripping it, our edge calculation compares
// model probability to inflated bookmaker implied probability —
// making every bet look like it has less edge than it really does.
//
// Method: for each bookmaker, calculate total implied probability
// across home/draw/away. True implied probability = raw / total.
// We then take the BEST true implied probability per outcome across
// all bookmakers (not the best raw odds, which can be misleading).
//
// This function returns:
//   { home, draw, away, over25 }  — best margin-stripped prices
//   { homeBook, drawBook, awayBook, over25Book } — source bookmakers
//   { homeOdds, drawOdds, awayOdds, over25Odds } — best raw decimal odds
//   { bookCount } — number of books with data
function extractMarketData(event) {
  // Collect raw 1X2 odds per bookmaker for margin stripping
  const books1x2 = [];
  const bestRaw  = { home: 0, draw: 0, away: 0, over25: 0,
                     homeBook: '', drawBook: '', awayBook: '', over25Book: '' };

  for (const book of (event.bookmakers || [])) {
    // H2H market
    const h2h = book.markets?.find(m => m.key === 'h2h');
    if (h2h) {
      let bHome = 0, bDraw = 0, bAway = 0;
      for (const o of h2h.outcomes) {
        if (nameMatch(o.name, event.home_team))  bHome = o.price;
        else if (nameMatch(o.name, event.away_team)) bAway = o.price;
        else if (o.name === 'Draw')              bDraw = o.price;
      }
      if (bHome > 0 && bDraw > 0 && bAway > 0) {
        books1x2.push({ title: book.title, home: bHome, draw: bDraw, away: bAway });
      }
      // Also track best raw odds for display
      if (bHome > bestRaw.home) { bestRaw.home = bHome; bestRaw.homeBook = book.title; }
      if (bDraw > bestRaw.draw) { bestRaw.draw = bDraw; bestRaw.drawBook = book.title; }
      if (bAway > bestRaw.away) { bestRaw.away = bAway; bestRaw.awayBook = book.title; }
    }
    // Over 2.5
    const totals = book.markets?.find(m => m.key === 'totals');
    if (totals) {
      for (const o of totals.outcomes) {
        if (o.name === 'Over' && Math.abs((o.point || 0) - 2.5) < 0.01 && o.price > bestRaw.over25) {
          bestRaw.over25 = o.price; bestRaw.over25Book = book.title;
        }
      }
    }
  }

  if (!books1x2.length) return null;

  // For each bookmaker, strip margin and get true implied probabilities
  // Then take the best (highest) margin-stripped implied prob per outcome
  // across all bookmakers — this is the tightest price the market offers.
  let bestTrueHome = 0, bestTrueDraw = 0, bestTrueAway = 0;
  let bestTrueHomeBook = '', bestTrueDrawBook = '', bestTrueAwayBook = '';

  for (const b of books1x2) {
    // Total implied probability (includes margin)
    const totalIP = (1/b.home) + (1/b.draw) + (1/b.away);
    // True (margin-stripped) probability
    const trueHome = (1/b.home) / totalIP;
    const trueDraw = (1/b.draw) / totalIP;
    const trueAway = (1/b.away) / totalIP;

    if (trueHome > bestTrueHome) { bestTrueHome = trueHome; bestTrueHomeBook = b.title; }
    if (trueDraw > bestTrueDraw) { bestTrueDraw = trueDraw; bestTrueDrawBook = b.title; }
    if (trueAway > bestTrueAway) { bestTrueAway = trueAway; bestTrueAwayBook = b.title; }
  }

  // Average margin across all books (for logging/diagnostics)
  const avgMargin = books1x2.reduce((s, b) =>
    s + ((1/b.home) + (1/b.draw) + (1/b.away) - 1), 0) / books1x2.length;

  return {
    // Margin-stripped true probabilities (used for edge calculation)
    trueHome: bestTrueHome,
    trueDraw: bestTrueDraw,
    trueAway: bestTrueAway,
    // Best raw decimal odds (used for stake calculation and display)
    homeOdds: bestRaw.home,
    drawOdds: bestRaw.draw,
    awayOdds: bestRaw.away,
    over25Odds: bestRaw.over25,
    // Best bookmaker per outcome
    homeBook:   bestRaw.homeBook,
    drawBook:   bestRaw.drawBook,
    awayBook:   bestRaw.awayBook,
    over25Book: bestRaw.over25Book,
    // Diagnostics
    bookCount:  books1x2.length,
    avgMargin:  parseFloat((avgMargin * 100).toFixed(2)),
  };
}

// ── G. EDGE CALCULATION ───────────────────────────────────────
// Edge = model probability - margin-stripped bookmaker probability.
// Positive edge = model thinks outcome more likely than market does.
// This is a real edge calculation — not inflated by bookmaker margin.
function calcEdge(modelProb, trueImpliedProb) {
  return parseFloat(((modelProb - trueImpliedProb) * 100).toFixed(2));
}

// ── H. FRACTIONAL KELLY STAKE ─────────────────────────────────
// Full Kelly: f = (bp - q) / b
//   b = decimal odds - 1 (net odds)
//   p = model probability of winning
//   q = 1 - p (probability of losing)
//
// Full Kelly is too aggressive for single-game bets due to model
// uncertainty. We use 0.25 Kelly (quarter-Kelly) which is standard
// practice for sports betting with uncertain probability estimates.
//
// Result is clamped to 0.5u–3.0u to match the site's stake display
// and protect the bank from edge-case over-sizing.
function kellyStake(modelProb, decimalOdds, fraction = 0.25) {
  const b = decimalOdds - 1;
  if (b <= 0 || modelProb <= 0 || modelProb >= 1) return 1.0;
  const q    = 1 - modelProb;
  const full = (b * modelProb - q) / b;
  if (full <= 0) return 0; // negative Kelly = no bet
  const sized = full * fraction;
  // Spread thresholds to produce genuine variation across tip quality
  if (sized >= 0.40) return 3.0;
  if (sized >= 0.28) return 2.5;
  if (sized >= 0.18) return 2.0;
  if (sized >= 0.11) return 1.5;
  if (sized >= 0.06) return 1.0;
  return 0.5;
}

// ── I. CONFIDENCE SCORE ───────────────────────────────────────
// Confidence reflects how much we trust this tip, combining:
//   1. Edge size (primary) — how much the model disagrees with market
//   2. Form quality signal (secondary) — is recent form consistent?
//   3. Data quality — full season stats vs form-only fallback
//
// Explicit bands, no modulo, linear interpolation within each band.
// Clamped 75–95 (never claim certainty, never publish below threshold).
function confidenceFromEdge(edgePct, formQuality, hasFullStats) {
  // Edge bands: edge is the main signal
  let base, bandFloor, bandCeil;
  if      (edgePct >= 18) { base = 91; bandFloor = 18; bandCeil = 25; }
  else if (edgePct >= 14) { base = 87; bandFloor = 14; bandCeil = 18; }
  else if (edgePct >= 10) { base = 83; bandFloor = 10; bandCeil = 14; }
  else if (edgePct >= 7)  { base = 79; bandFloor = 7;  bandCeil = 10; }
  else                    { base = 75; bandFloor = 5;  bandCeil = 7;  }

  // Interpolate within band
  const progress = Math.min(1, (edgePct - bandFloor) / (bandCeil - bandFloor));
  const fromEdge = base + Math.round(progress * 3);

  // Form quality adjustment: -3 to +3
  // formQuality is 0–1 (1 = all recent wins, 0 = all losses)
  const formAdj = Math.round((formQuality - 0.5) * 6);

  // Data quality: if we're using form-only fallback (no season stats), reduce confidence
  const dataAdj = hasFullStats ? 0 : -3;

  return Math.min(95, Math.max(75, fromEdge + formAdj + dataAdj));
}

// ═══════════════════════════════════════════════════════════════
// NEW SELECTION SYSTEM
// Replaces highest-edge selection with quality-scored system.
// All candidates pass through veto → score → threshold check.
// ═══════════════════════════════════════════════════════════════

// ── TOMORROW CHECK ────────────────────────────────────────────
// Returns true if event falls on tomorrow's date in Europe/London timezone.
function isTomorrowFixture(commenceTime) {
  if (!commenceTime) return false;
  const eventDate = new Date(commenceTime);
  if (isNaN(eventDate.getTime())) return false;
  const londonNow = new Date(new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' }));
  const londonTomorrow = new Date(londonNow);
  londonTomorrow.setDate(londonTomorrow.getDate() + 1);
  const eventStr    = new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit' }).format(eventDate);
  const tomorrowStr = new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit' }).format(londonTomorrow);
  return eventStr === tomorrowStr;
}

// ── BOOKMAKER SANITY CHECK ────────────────────────────────────
// Football fixture passes only if:
// - At least 2 bookmakers provide full 1X2 market
// - Home and away odds both within valid odds range
function passedBookmakerSanityCheck(event, sport) {
  if (sport !== 'Football') return false;
  let complete1x2Books = 0;
  let bestHomeOdds = 0;
  let bestAwayOdds = 0;
  for (const book of (event.bookmakers || [])) {
    const h2h = book.markets?.find(m => m.key === 'h2h');
    if (!h2h) continue;
    let hasHome = false, hasAway = false, hasDraw = false;
    for (const o of (h2h.outcomes || [])) {
      if (nameMatch(o.name, event.home_team))       { hasHome = true; if (o.price > bestHomeOdds) bestHomeOdds = o.price; }
      else if (nameMatch(o.name, event.away_team))  { hasAway = true; if (o.price > bestAwayOdds) bestAwayOdds = o.price; }
      else if (o.name === 'Draw')                   { hasDraw = true; }
    }
    if (hasHome && hasAway && hasDraw) complete1x2Books++;
  }
  return (
    complete1x2Books >= 2 &&
    bestHomeOdds >= ODDS_MIN && bestHomeOdds <= ODDS_ELITE_MAX &&
    bestAwayOdds >= ODDS_MIN && bestAwayOdds <= ODDS_ELITE_MAX
  );
}

// ── ELIGIBLE FORM FIXTURES ────────────────────────────────────
// Returns tomorrow's football fixtures that pass sanity check,
// sorted by bookmaker depth, respecting MAX_FOOTBALL_FORM_CALLS.
// Deduplicates teams using alreadyFetchedTeams (seeded from formCache).
function getEligibleFootballFormFixtures(events, sport, alreadyFetchedTeams = new Set()) {
  if (sport !== 'Football') return [];
  const eligible = events
    .filter(event => isTomorrowFixture(event.commence_time) && passedBookmakerSanityCheck(event, sport))
    .map(event => {
      let depth = 0;
      for (const book of (event.bookmakers || [])) {
        const h2h = book.markets?.find(m => m.key === 'h2h');
        if (!h2h) continue;
        let hasHome = false, hasAway = false, hasDraw = false;
        for (const o of (h2h.outcomes || [])) {
          if (nameMatch(o.name, event.home_team)) hasHome = true;
          else if (nameMatch(o.name, event.away_team)) hasAway = true;
          else if (o.name === 'Draw') hasDraw = true;
        }
        if (hasHome && hasAway && hasDraw) depth++;
      }
      return { event, bookmakerDepth: depth };
    })
    .sort((a, b) => b.bookmakerDepth - a.bookmakerDepth);

  const selected = [];
  let callsUsed = 0;
  for (const { event } of eligible) {
    let needed = 0;
    if (!alreadyFetchedTeams.has(event.home_team)) needed++;
    if (!alreadyFetchedTeams.has(event.away_team)) needed++;
    if (callsUsed + needed > MAX_FOOTBALL_FORM_CALLS) break;
    selected.push(event);
    if (!alreadyFetchedTeams.has(event.home_team)) { alreadyFetchedTeams.add(event.home_team); callsUsed++; }
    if (!alreadyFetchedTeams.has(event.away_team)) { alreadyFetchedTeams.add(event.away_team); callsUsed++; }
  }
  return selected;
}

// ── VETO LAYER ────────────────────────────────────────────────
// Hard reject before scoring. Any true = reject.
// Football probGap = top - second (draw included).
// NHL probGap = abs(home - away) (2-way market).
function vetoCandidate({ sport, hasCoreData, hasCompleteMarket, homeWinProb, drawProb = 0, awayWinProb, adverseLineMove }) {
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
  if (!hasCoreData)          return true;
  if (!hasCompleteMarket)    return true;
  if (topOutcomeProb < minTopOutcomeProb) return true;
  if (probGap < 0.10)        return true;
  if (adverseLineMove >= 0.10) return true;
  // Veto if model is implausibly confident — likely a data error (inflated lambda)
  const topModelProb = sport === 'Football'
    ? Math.max(homeWinProb, drawProb, awayWinProb)
    : Math.max(homeWinProb, awayWinProb);
  if (topModelProb > 0.82)   return true;
  return false;
}

// ── DATA QUALITY TIER ─────────────────────────────────────────
function getDataQualityTier({ hasFullStats, hasFormFallback }) {
  if (hasFullStats)    return 1.0;
  if (hasFormFallback) return 0.7;
  return 0.0;
}

// ── PROBABILITY STRENGTH ──────────────────────────────────────
// How far the model probability is from 50% — range 0 to 1.
function getProbabilityStrength(modelProb) {
  return Math.min(1, Math.abs(modelProb - 0.5) / 0.5);
}

// ── CANDIDATE QUALITY SCORE ───────────────────────────────────
// 3-signal score: edge (primary 60%), probability strength (25%), data quality (15%).
// Edge dominates but is tempered by model conviction and data reliability.
function scoreCandidate({ edgePct, modelProb, dataQualityTier }) {
  const edgeScore   = Math.min(1, Math.max(0, edgePct) / 20); // capped at 1.0 — prevents fake large edges dominating
  const probStrength = getProbabilityStrength(modelProb);
  return edgeScore * 0.60 + probStrength * 0.25 + dataQualityTier * 0.15;
}

// ── PICK BEST CANDIDATE ───────────────────────────────────────
// Scores all candidates, returns highest scorer above threshold.
// Returns null if no candidate meets MIN_QUALITY_SCORE.
const MIN_QUALITY_SCORE = 0.42;

function pickBestCandidate(candidates) {
  if (!candidates.length) return null;
  const scored = candidates.map(c => ({
    ...c,
    qualityScore: scoreCandidate({
      edgePct:         c.edge,
      modelProb:       c.modelProb,
      dataQualityTier: c.dataQualityTier,
    }),
  }));
  const best = scored.reduce((a, b) => a.qualityScore >= b.qualityScore ? a : b);
  if (best.qualityScore < MIN_QUALITY_SCORE) return null;
  return best;
}

// ── UPDATED CONFIDENCE FUNCTION ───────────────────────────────
// Edge-band base + 2 additional signals: probability strength and data quality.
// NHL tips capped at 82 due to missing goalie data.
function confidenceFromSignals({ edgePct, modelProb, dataQualityTier, sport }) {
  // Base from edge bands
  let conf;
  if      (edgePct >= 18) conf = 88;
  else if (edgePct >= 14) conf = 84;
  else if (edgePct >= 10) conf = 80;
  else if (edgePct >= 8)  conf = 77;
  else                    conf = 74;

  // Signal 1: probability strength
  const probStrength = Math.min(1, Math.abs(modelProb - 0.5) / 0.5);
  if (probStrength >= 0.30) conf += 2;
  else if (probStrength >= 0.20) conf += 1;
  else if (probStrength < 0.10)  conf -= 1;

  // Signal 2: data quality penalty
  if (dataQualityTier === 0.7) conf -= 3;

  conf = Math.max(70, Math.min(92, Math.round(conf)));

  // NHL cap: permanently 82 due to missing goalie starter data
  if (sport === 'Ice Hockey') conf = applyNHLGoalieConfidenceCap(conf);

  return conf;
}

// ── J. MARKET-AWARE FORM ADJUSTMENT ──────────────────────────
// Returns a form quality signal (0–1) for use in confidence calculation.
// This is NOT added directly to confidence — it feeds confidenceFromEdge
// as the formQuality parameter so the relationship is transparent.
//
// For win bets: uses the tipping team's recent form score.
// For draw:     uses average form of both teams (lower = more volatile = draw more likely, but we penalise).
// For over 2.5: uses combined recent scoring rate.
function getFormQuality(hf, af, market) {
  if (!hf && !af) return 0.5; // neutral if no data

  if (market === 'home') {
    return hf ? hf.formScore : 0.5;
  }
  if (market === 'away') {
    return af ? af.formScore : 0.5;
  }
  if (market === 'draw') {
    // Draw quality: both teams in mid-table form suggests draws
    // We return 0.5 as neutral — draws are model-driven not form-driven
    return 0.5;
  }
  if (market === 'over25') {
    // Use combined recent scoring rate normalised to 0–1
    const hScore = hf ? Math.min(1, hf.avgGoalsFor / 2.5) : 0.5;
    const aScore = af ? Math.min(1, af.avgGoalsFor / 2.5) : 0.5;
    return (hScore + aScore) / 2;
  }
  return 0.5;
}

// ── K. NOTES BUILDER ─────────────────────────────────────────
// Full model diagnostics for Supabase notes field.
// Format: xG: H vs A | Fair odds: X.XX | Book: X.XX (Bookie) | Edge: +X.X% | Model: XX.X% <market> probability
function buildFootballNotes({ market, modelProb, fairPrice, bookOdds, bookmaker, edgePct, lambdaHome, lambdaAway, bookMargin, h2hRecord }) {
  if (lambdaHome == null || lambdaAway == null || modelProb == null) {
    return `Book: ${bookOdds} (${bookmaker || 'Multiple'}) | Edge: +${(edgePct || 0).toFixed(1)}%`;
  }
  const probSuffix = {
    home:   'home win probability',
    away:   'away win probability',
    draw:   'draw probability',
    over25: 'over 2.5 probability',
  }[market] || 'win probability';

  const parts = [
    `xG: ${lambdaHome.toFixed(2)} vs ${lambdaAway.toFixed(2)}`,
    `Fair odds: ${fairPrice}`,
    `Book: ${bookOdds} (${bookmaker})`,
    `Edge: +${edgePct.toFixed(1)}%`,
    `Model: ${(modelProb * 100).toFixed(1)}% ${probSuffix}`,
  ];
  if (bookMargin != null) parts.push(`Mkt margin: ${bookMargin}%`);
  if (h2hRecord)          parts.push(`H2H: ${h2hRecord}`);
  return parts.join(' | ');
}

// Appends form strings to existing notes (used by applyFormToPendingTips)
function appendFormToNotes(hf, af, existingNotes = '') {
  if (existingNotes.includes('Home form:')) return existingNotes;
  const extras = [];
  if (hf) extras.push(`Home form: ${hf.formString}`);
  if (af) extras.push(`Away form: ${af.formString}`);
  return extras.length ? existingNotes + ' | ' + extras.join(' | ') : existingNotes;
}

// ── L. MAIN FOOTBALL FIXTURE ANALYSER ────────────────────────
// Entry point called from generateTips() for every football fixture.
// Returns a tip object (same shape as the rest of the engine) or null.
//
// Decision flow:
//   1. Fetch season stats → calculate attack/defence strengths
//   2. Apply H2H modifier to λ values
//   3. Build Dixon-Coles corrected score matrix
//   4. Derive outcome probabilities
//   5. Strip bookmaker margin → get true implied probabilities
//   6. Calculate real edge per market
//   7. Filter: only tip if edge ≥ MIN_EDGE_PCT
//   8. Size stake with fractional Kelly
//   9. Build confidence score
//  10. Return tip or null
async function analyseFootballFixture(event, sport) {
  try {
    const leagueId = sport.leagueId;
    if (!leagueId) return null;

    const leagueAvg = getLeagueAvg(leagueId);
    const { homeGoals: lgHome, awayGoals: lgAway } = leagueAvg;

    // ── 1. Season stats ─────────────────────────────────────
    const [homeStats, awayStats] = await Promise.all([
      fetchTeamSeasonStats(event.home_team, leagueId),
      fetchTeamSeasonStats(event.away_team, leagueId),
    ]);

    // Form data from morning fetch (may be null if budget expired)
    const hf = formCache[`${event.home_team}_${leagueId}`] || null;
    const af = formCache[`${event.away_team}_${leagueId}`] || null;

    let lambdaHome, lambdaAway;
    let hasFullStats = false;

    if (homeStats && awayStats) {
      hasFullStats = true;
      // Attack strength = team avg scored ÷ league avg (home or away context)
      const homeAvgScoredH  = homeStats.homeScored   / homeStats.homeGames;
      const homeAvgConcH    = homeStats.homeConceded  / homeStats.homeGames;
      const awayAvgScoredA  = awayStats.awayScored    / awayStats.awayGames;
      const awayAvgConcA    = awayStats.awayConceded  / awayStats.awayGames;

      const homeAttack = homeAvgScoredH / lgHome;
      const homeDef    = homeAvgConcH   / lgAway;
      const awayAttack = awayAvgScoredA / lgAway;
      const awayDef    = awayAvgConcA   / lgHome;

      lambdaHome = homeAttack * awayDef   * lgHome;
      lambdaAway = awayAttack * homeDef   * lgAway;

    } else if (hf && af) {
      // Fallback: use last-5 form averages as λ proxies, normalised via league averages.
      // Mirrors the full-stats attack/defence formula so lambdas stay calibrated.
      // Confidence penalised via hasFullStats=false.
      const hfAttack = hf.avgGoalsFor     / lgHome;
      const hfDef    = hf.avgGoalsAgainst / lgAway;
      const afAttack = af.avgGoalsFor     / lgAway;
      const afDef    = af.avgGoalsAgainst / lgHome;
      lambdaHome = hfAttack * afDef  * lgHome;
      lambdaAway = afAttack * hfDef  * lgAway;
      console.log(`  ⚠️ Form-fallback λ (normalised): ${event.home_team} vs ${event.away_team}`);
    } else {
      // No data available — refuse to price
      return null;
    }

    // ── 2. H2H modifier ─────────────────────────────────────
    // Get team IDs for H2H lookup — prefer stats cache, fall back to formCache
    const homeId = homeStats?.teamId || formCache[`${event.home_team}_${leagueId}`]?.teamId || null;
    const awayId = awayStats?.teamId || formCache[`${event.away_team}_${leagueId}`]?.teamId || null;
    const h2h    = await fetchH2HModifier(homeId, awayId);

    // Apply H2H modifier to lambdas
    lambdaHome = lambdaHome * (1 + h2h.homeMod);
    lambdaAway = lambdaAway * (1 + h2h.awayMod);

    // Clamp to realistic range — football xG rarely exceeds 3.0 per team per match.
    // Ceiling of 2.8 prevents runaway fallback values corrupting the matrix.
    const lH = Math.max(0.30, Math.min(2.8, lambdaHome));
    const lA = Math.max(0.30, Math.min(2.8, lambdaAway));

    // ── 3. Score matrix with Dixon-Coles correction ─────────
    const matrix = buildScoreMatrix(lH, lA);

    // ── 4. Outcome probabilities ─────────────────────────────
    const { homeWin, draw, awayWin, over25 } = calcOutcomes(matrix);

    // ── 5. Margin-stripped market data ───────────────────────
    const market = extractMarketData(event);
    if (!market) return null; // need at least one complete 1X2 book

    // ── 6. Edge calculation (against margin-stripped prices) ──
    // For over 2.5 we use raw implied probability (only one side priced)
    const over25IP = market.over25Odds > 0 ? 1 / market.over25Odds : 0;

    const candidates = [];

    // Home win
    if (market.homeOdds >= 1.25 && market.homeOdds <= 6.5 && market.trueHome > 0) {
      const edge = calcEdge(homeWin, market.trueHome);
      if (edge >= MIN_EDGE_PCT) {
        const kelly  = kellyStake(homeWin, market.homeOdds);
        if (kelly > 0) {
          const tier_ = getDataQualityTier({ hasFullStats, hasFormFallback: !hasFullStats && (hf !== null || af !== null) });
          const conf = confidenceFromSignals({ edgePct: edge, modelProb: homeWin, dataQualityTier: tier_, sport: 'Football' });
          if (conf >= MIN_CONFIDENCE) candidates.push({
            market: 'home', edge, modelProb: homeWin,
            fairPrice: fairOdds(homeWin), bookOdds: market.homeOdds,
            bookmaker: market.homeBook, stake: kelly, conf,
            selection: `${event.home_team} Win`,
          });
        }
      }
    }

    // Away win
    if (market.awayOdds >= 1.25 && market.awayOdds <= 6.5 && market.trueAway > 0) {
      const edge = calcEdge(awayWin, market.trueAway);
      if (edge >= MIN_EDGE_PCT) {
        const kelly = kellyStake(awayWin, market.awayOdds);
        if (kelly > 0) {
          const tier_ = getDataQualityTier({ hasFullStats, hasFormFallback: !hasFullStats && (hf !== null || af !== null) });
          const conf = confidenceFromSignals({ edgePct: edge, modelProb: awayWin, dataQualityTier: tier_, sport: 'Football' });
          if (conf >= MIN_CONFIDENCE) candidates.push({
            market: 'away', edge, modelProb: awayWin,
            fairPrice: fairOdds(awayWin), bookOdds: market.awayOdds,
            bookmaker: market.awayBook, stake: kelly, conf,
            selection: `${event.away_team} Win`,
          });
        }
      }
    }

    // Draw — extra hurdle (+3%): draws are genuinely harder to price
    if (market.drawOdds >= 2.50 && draw > 0.22 && market.trueDraw > 0) {
      const edge = calcEdge(draw, market.trueDraw);
      if (edge >= MIN_EDGE_PCT + 5) { // +5% extra hurdle — draws are noisiest market
        const kelly = kellyStake(draw, market.drawOdds);
        if (kelly > 0) {
          const tier_ = getDataQualityTier({ hasFullStats, hasFormFallback: !hasFullStats && (hf !== null || af !== null) });
          const conf = confidenceFromSignals({ edgePct: edge, modelProb: draw, dataQualityTier: tier_, sport: 'Football' });
          if (conf >= MIN_CONFIDENCE) candidates.push({
            market: 'draw', edge, modelProb: draw,
            fairPrice: fairOdds(draw), bookOdds: market.drawOdds,
            bookmaker: market.drawBook, stake: kelly, conf,
            selection: 'Draw',
          });
        }
      }
    }

    // Over 2.5 — use raw implied probability (single-side market, no 3-way margin)
    if (market.over25Odds >= 1.45 && market.over25Odds <= 2.50 && over25IP > 0) {
      const edge = calcEdge(over25, over25IP);
      if (edge >= MIN_EDGE_PCT) {
        const kelly = kellyStake(over25, market.over25Odds);
        if (kelly > 0) {
          const tier_ = getDataQualityTier({ hasFullStats, hasFormFallback: !hasFullStats && (hf !== null || af !== null) });
          const conf = confidenceFromSignals({ edgePct: edge, modelProb: over25, dataQualityTier: tier_, sport: 'Football' });
          if (conf >= MIN_CONFIDENCE) candidates.push({
            market: 'over25', edge, modelProb: over25,
            fairPrice: fairOdds(over25), bookOdds: market.over25Odds,
            bookmaker: market.over25Book, stake: kelly, conf,
            selection: 'Over 2.5',
          });
        }
      }
    }

    if (!candidates.length) return null;

    // ── Veto + quality score selection ─────────────────────────
    // Apply veto layer first, then score remaining candidates.
    const vetoed = candidates.filter(c => !vetoCandidate({
      sport:             'Football',
      hasCoreData:       hasFullStats || (hf !== null && af !== null),
      hasCompleteMarket: !!market,
      homeWinProb:       homeWin,
      drawProb:          draw,
      awayWinProb:       awayWin,
      adverseLineMove:   0, // line movement tracking not yet implemented
    }));

    // Attach data quality tier to each candidate
    const tier = getDataQualityTier({ hasFullStats, hasFormFallback: !hasFullStats && (hf !== null || af !== null) });
    const withTier = vetoed.map(c => ({ ...c, dataQualityTier: tier }));

    // Pick best by quality score — returns null if none pass threshold
    const pick = pickBestCandidate(withTier);
    if (!pick) return null;

    // H2H record string for notes
    const h2hStr = null; // H2H removed to preserve API budget

    const notes = buildFootballNotes({
      market:      pick.market,
      modelProb:   pick.modelProb,
      fairPrice:   pick.fairPrice,
      bookOdds:    pick.bookOdds,
      bookmaker:   pick.bookmaker,
      edgePct:     pick.edge,
      lambdaHome:  lH,
      lambdaAway:  lA,
      bookMargin:  market.avgMargin,
      h2hRecord:   h2hStr,
    });

    return {
      tip_ref:    generateTipRef('Football'),
      sport:      'Football',
      league:     sport.league,
      home_team:  event.home_team,
      away_team:  event.away_team,
      event_time: event.commence_time,
      selection:  pick.selection,
      market:     pick.market === 'over25' ? 'totals' : 'h2h',
      odds:       parseFloat(pick.bookOdds.toFixed(2)),
      stake:      pick.stake,
      confidence: pick.conf,
      tier:       'pro',
      status:     'pending',
      bookmaker:  pick.bookmaker || 'Multiple',
      notes,
    };

  } catch(e) {
    console.error(`Football model error [${event.home_team} vs ${event.away_team}]:`, e.message);
    return null;
  }
}


// ═══════════════════════════════════════════════════════════════
// US SPORTS MODELS — NBA + NHL
// ═══════════════════════════════════════════════════════════════
// Both models use the same key as API-Football (API-Sports family).
// Each API has its own independent 100 req/day budget.
// NFL and MLB continue using market consensus — their data
// structures don't lend themselves to a simple scoring model
// without player-level data, which costs too many API calls.
//
// NBA MODEL:
//   Uses offensive/defensive rating from team season stats.
//   Pace-adjusted expected score per team.
//   Margin-stripped edge calculation + fractional Kelly staking.
//
// NHL MODEL:
//   Uses goals-for and goals-against per game (season average).
//   Simple Poisson model (hockey scores < 10, 0..7 matrix sufficient).
//   Margin-stripped edge + fractional Kelly.
//
// INJURY CACHE:
//   Fetches NBA + NHL injuries once per day from BallDontLie.
//   If key player (starter) is injured, a confidence penalty applies.
//   Fetched at 06:00 alongside form data.
// ═══════════════════════════════════════════════════════════════

const API_BASKETBALL_BASE = 'https://v2.nba.api-sports.io';
const API_HOCKEY_BASE     = 'https://v1.hockey.api-sports.io';
const BDL_BASE            = 'https://api.balldontlie.io';

// BDL budget counter — generous free tier, ~5 req/min rate limit
// We use it for injuries + team stats (games endpoint), ~10-20 calls/day total
let bdlBudget  = { date: '', used: 0, limit: 200 };

function checkBDLBudget(n = 1) {
  const today = new Date().toISOString().split('T')[0];
  if (bdlBudget.date !== today) { bdlBudget.date = today; bdlBudget.used = 0; }
  if (bdlBudget.used + n > bdlBudget.limit) return false;
  bdlBudget.used += n; return true;
}

// ── INJURY CACHE ──────────────────────────────────────────────
// Stores injured players per team: { 'TeamName': ['Player1', 'Player2'] }
// Fetched once at 06:00 UK. Used to apply confidence penalties.
const injuryCache = { nba: {}, nhl: {} };
let injuryCacheFetched = { nba: '', nhl: '' };

// BallDontLie NBA injuries — free tier, no auth required for basic endpoints
// Returns list of current game-time-decision or out players
async function fetchNBAInjuries(BDL_API_KEY) {
  const today = new Date().toISOString().split('T')[0];
  if (injuryCacheFetched.nba === today) return;
  if (!checkBDLBudget(1)) return;
  try {
    const res = await fetch(`${BDL_BASE}/v1/player_injuries`, {
      headers: BDL_API_KEY ? { 'Authorization': BDL_API_KEY } : {}
    });
    if (!res.ok) return;
    const data = await res.json();
    const injuries = data.data || [];
    const cache = {};
    for (const inj of injuries) {
      const team = inj.team?.full_name || inj.team?.name;
      const player = inj.player ? `${inj.player.first_name} ${inj.player.last_name}` : null;
      if (team && player) {
        if (!cache[team]) cache[team] = [];
        cache[team].push(player);
      }
    }
    injuryCache.nba = cache;
    injuryCacheFetched.nba = today;
    const total = Object.values(cache).reduce((s,a) => s + a.length, 0);
    console.log(`🏥 NBA injuries fetched: ${total} players across ${Object.keys(cache).length} teams`);
  } catch(e) { console.error('NBA injury fetch error:', e.message); }
}

async function fetchNHLInjuries(BDL_API_KEY) {
  const today = new Date().toISOString().split('T')[0];
  if (injuryCacheFetched.nhl === today) return;
  if (!checkBDLBudget(1)) return;
  try {
    const res = await fetch(`${BDL_BASE}/nhl/v1/player_injuries`, {
      headers: BDL_API_KEY ? { 'Authorization': BDL_API_KEY } : {}
    });
    if (!res.ok) return;
    const data = await res.json();
    const injuries = data.data || [];
    const cache = {};
    for (const inj of injuries) {
      const team = inj.team?.full_name;
      const player = inj.player?.full_name;
      if (team && player) {
        if (!cache[team]) cache[team] = [];
        cache[team].push(player);
      }
    }
    injuryCache.nhl = cache;
    injuryCacheFetched.nhl = today;
    const total = Object.values(cache).reduce((s,a) => s + a.length, 0);
    console.log(`🏥 NHL injuries fetched: ${total} players across ${Object.keys(cache).length} teams`);
  } catch(e) { console.error('NHL injury fetch error:', e.message); }
}

// Returns how many known injured players a team has (0 = clean bill of health)
function injuryCount(sport, teamName) {
  const cache = injuryCache[sport.toLowerCase()] || {};
  // Try exact match then partial
  for (const [team, players] of Object.entries(cache)) {
    if (nameMatch(team, teamName)) return players.length;
  }
  return 0;
}

// Injury confidence penalty: each injured player reduces confidence slightly
// Capped at -5 total to avoid over-weighting uncertainty
function injuryPenalty(homeTeam, awayTeam, sport) {
  const hInj = injuryCount(sport, homeTeam);
  const aInj = injuryCount(sport, awayTeam);
  return Math.min(5, (hInj + aInj));
}

// ── NBA TEAM STATS CACHE ──────────────────────────────────────
// Derived from last 15 BallDontLie game results — no API-Sports needed.
// Computes pts/gm scored and allowed from actual game box scores.
const nbaTeamCache = {};
// BDL team name → BDL team ID (fetched once, cached for process lifetime)
const bdlNBATeamIds = {};

async function fetchBDLNBATeamId(teamName) {
  if (bdlNBATeamIds[teamName]) return bdlNBATeamIds[teamName];
  if (!checkBDLBudget(1)) return null;
  try {
    const res = await fetch(
      `${BDL_BASE}/nba/v1/teams?search=${encodeURIComponent(teamName)}`,
      { headers: { 'Authorization': BDL_API_KEY } }
    );
    if (!res.ok) return null;
    const data = await res.json();
    const team = (data.data || []).find(t =>
      nameMatch(t.full_name, teamName) || nameMatch(t.name, teamName)
    );
    if (!team) return null;
    bdlNBATeamIds[teamName] = team.id;
    return team.id;
  } catch(e) { console.error(`BDL NBA team ID error (${teamName}):`, e.message); return null; }
}

async function fetchNBATeamStats(teamName) {
  const season = seasonFor();
  const cacheKey = `${teamName}_${season}`;
  if (nbaTeamCache[cacheKey]) return nbaTeamCache[cacheKey];
  if (!checkBDLBudget(1)) { console.log(`⚠️ BDL NBA budget: skipping ${teamName}`); return null; }

  try {
    const teamId = await fetchBDLNBATeamId(teamName);
    if (!teamId) return null;

    // Fetch last 15 completed games for this team this season
    const res = await fetch(
      `${BDL_BASE}/nba/v1/games?team_ids[]=${teamId}&seasons[]=${season}&per_page=15`,
      { headers: { 'Authorization': BDL_API_KEY } }
    );
    if (!res.ok) return null;
    const data = await res.json();
    const games = (data.data || []).filter(g => g.status === 'Final');

    if (games.length < 5) {
      console.log(`  ⚠️ NBA ${teamName}: only ${games.length} completed games — skipping`);
      return null;
    }

    let ptsFor = 0, ptsAgainst = 0;
    for (const g of games) {
      const isHome = g.home_team?.id === teamId;
      ptsFor     += isHome ? (g.home_team_score || 0) : (g.visitor_team_score || 0);
      ptsAgainst += isHome ? (g.visitor_team_score || 0) : (g.home_team_score || 0);
    }
    const n = games.length;
    const result = {
      teamId,
      gamesPlayed: n,
      ptsFor:      ptsFor / n,
      ptsAgainst:  ptsAgainst / n,
    };

    nbaTeamCache[cacheKey] = result;
    console.log(`  🏀 NBA ${teamName} [BDL ${n}gm]: ${result.ptsFor.toFixed(1)} pts/gm, ${result.ptsAgainst.toFixed(1)} allowed`);
    return result;
  } catch(e) { console.error(`BDL NBA team stats error (${teamName}):`, e.message); return null; }
}

// ── NBA GAME MODEL ────────────────────────────────────────────
// Expected score approach: uses offensive/defensive strength.
// NBA home court advantage is ~3.5 points on average.
// We model expected total points and expected margin, then convert
// to win probability using a normal distribution approximation.
// NBA game-to-game variance (std dev) is ~12 points.
const NBA_HOME_ADVANTAGE  = 3.5;  // points
const NBA_LEAGUE_AVG_PTS  = 113;  // both teams average pts per game
const NBA_SCORE_STD_DEV   = 12.0; // standard deviation of margin

// Normal CDF approximation (Abramowitz & Stegun)
function normalCDF(x) {
  const t = 1 / (1 + 0.2315419 * Math.abs(x));
  const d = 0.3989423 * Math.exp(-x * x / 2);
  const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.7814779 + t * (-1.8212560 + t * 1.3302744))));
  return x >= 0 ? 1 - p : p;
}

// Win probability given expected margin and std dev
function winProbFromMargin(expectedMargin, stdDev = NBA_SCORE_STD_DEV) {
  return normalCDF(expectedMargin / stdDev);
}

async function analyseNBAFixture(event, sport) {
  try {
    const [homeStats, awayStats] = await Promise.all([
      fetchNBATeamStats(event.home_team),
      fetchNBATeamStats(event.away_team),
    ]);
    if (!homeStats || !awayStats) return null;

    // Offensive strength = team pts/gm ÷ league avg
    // Defensive strength = team pts-allowed/gm ÷ league avg
    const homeOff = homeStats.ptsFor     / NBA_LEAGUE_AVG_PTS;
    const homeDef = homeStats.ptsAgainst / NBA_LEAGUE_AVG_PTS;
    const awayOff = awayStats.ptsFor     / NBA_LEAGUE_AVG_PTS;
    const awayDef = awayStats.ptsAgainst / NBA_LEAGUE_AVG_PTS;

    // Expected points scored by each team
    const homeExpected = homeOff * awayDef * NBA_LEAGUE_AVG_PTS + NBA_HOME_ADVANTAGE;
    const awayExpected = awayOff * homeDef * NBA_LEAGUE_AVG_PTS;

    const expectedMargin = homeExpected - awayExpected; // positive = home favoured

    // Win probabilities
    const homeWinP = winProbFromMargin(expectedMargin);
    const awayWinP = 1 - homeWinP;

    // Expected total
    const expectedTotal = homeExpected + awayExpected;

    // Best bookmaker odds (with margin stripping)
    const market = extractMarketData(event);
    if (!market) return null;

    // Over/Under using normal distribution on total
    // P(total > line) requires std dev of total ≈ sqrt(2) * single-team std dev
    const totalStdDev = NBA_SCORE_STD_DEV * Math.sqrt(2);

    const candidates = [];

    // Home win
    if (market.homeOdds >= 1.25 && market.homeOdds <= 5.0 && market.trueHome > 0) {
      const edge = calcEdge(homeWinP, market.trueHome);
      if (edge >= MIN_EDGE_PCT) {
        const injPen = injuryPenalty(event.home_team, event.away_team, 'nba');
        const conf   = Math.min(92, Math.max(75, Math.round(75 + (edge - 5) * 1.5) - injPen));
        if (conf >= MIN_CONFIDENCE) {
          const stake = kellyStake(homeWinP, market.homeOdds);
          if (stake > 0) candidates.push({
            selection: `${event.home_team} Win`, market: 'home', edge,
            modelProb: homeWinP, bookOdds: market.homeOdds, bookmaker: market.homeBook,
            stake, conf,
            notes: `Expected: ${homeExpected.toFixed(1)}-${awayExpected.toFixed(1)} | Model: ${(homeWinP*100).toFixed(1)}% home win | Fair: ${fairOdds(homeWinP)} | Book: ${market.homeOdds} (${market.homeBook}) | Edge: +${edge.toFixed(1)}%${injPen > 0 ? ` | ⚠️ ${injPen} injuries` : ''}`,
          });
        }
      }
    }

    // Away win
    if (market.awayOdds >= 1.25 && market.awayOdds <= 5.0 && market.trueAway > 0) {
      const edge = calcEdge(awayWinP, market.trueAway);
      if (edge >= MIN_EDGE_PCT) {
        const injPen = injuryPenalty(event.home_team, event.away_team, 'nba');
        const conf   = Math.min(92, Math.max(75, Math.round(75 + (edge - 5) * 1.5) - injPen));
        if (conf >= MIN_CONFIDENCE) {
          const stake = kellyStake(awayWinP, market.awayOdds);
          if (stake > 0) candidates.push({
            selection: `${event.away_team} Win`, market: 'away', edge,
            modelProb: awayWinP, bookOdds: market.awayOdds, bookmaker: market.awayBook,
            stake, conf,
            notes: `Expected: ${homeExpected.toFixed(1)}-${awayExpected.toFixed(1)} | Model: ${(awayWinP*100).toFixed(1)}% away win | Fair: ${fairOdds(awayWinP)} | Book: ${market.awayOdds} (${market.awayBook}) | Edge: +${edge.toFixed(1)}%${injPen > 0 ? ` | ⚠️ ${injPen} injuries` : ''}`,
          });
        }
      }
    }

    // Totals — using normal distribution on expected total
    const totalsBooks = event.bookmakers.map(b => b.markets?.find(m => m.key === 'totals')).filter(Boolean);
    if (totalsBooks.length >= 2) {
      const overOdds = [], underOdds = [];
      let line = null;
      for (const b of totalsBooks) {
        for (const o of b.outcomes) {
          if (o.name === 'Over')  { overOdds.push(o.price); line = o.point; }
          if (o.name === 'Under') { underOdds.push(o.price); }
        }
      }
      if (line && overOdds.length && underOdds.length) {
        const overP  = normalCDF((expectedTotal - line) / totalStdDev);
        const underP = 1 - overP;
        const bestOver  = Math.max(...overOdds);
        const bestUnder = Math.max(...underOdds);
        const overIP    = 1 / (overOdds.reduce((a,b)=>a+b,0)/overOdds.length);
        const underIP   = 1 / (underOdds.reduce((a,b)=>a+b,0)/underOdds.length);

        const overEdge  = calcEdge(overP,  overIP);
        const underEdge = calcEdge(underP, underIP);

        if (overEdge >= MIN_EDGE_PCT && bestOver >= 1.50 && bestOver <= 2.30) {
          const conf  = Math.min(90, Math.max(75, Math.round(75 + (overEdge - 5) * 1.2)));
          const stake = kellyStake(overP, bestOver);
          if (stake > 0 && conf >= MIN_CONFIDENCE) candidates.push({
            selection: `Over ${line}`, market: 'over', edge: overEdge,
            modelProb: overP, bookOdds: bestOver,
            bookmaker: event.bookmakers[0]?.title || 'Multiple', stake, conf,
            notes: `Expected total: ${expectedTotal.toFixed(1)} | Line: ${line} | Model: ${(overP*100).toFixed(1)}% over | Edge: +${overEdge.toFixed(1)}%`,
          });
        }
        if (underEdge >= MIN_EDGE_PCT && bestUnder >= 1.50 && bestUnder <= 2.30) {
          const conf  = Math.min(90, Math.max(75, Math.round(75 + (underEdge - 5) * 1.2)));
          const stake = kellyStake(underP, bestUnder);
          if (stake > 0 && conf >= MIN_CONFIDENCE) candidates.push({
            selection: `Under ${line}`, market: 'under', edge: underEdge,
            modelProb: underP, bookOdds: bestUnder,
            bookmaker: event.bookmakers[0]?.title || 'Multiple', stake, conf,
            notes: `Expected total: ${expectedTotal.toFixed(1)} | Line: ${line} | Model: ${(underP*100).toFixed(1)}% under | Edge: +${underEdge.toFixed(1)}%`,
          });
        }
      }
    }

    if (!candidates.length) return null;
    const pick = candidates.reduce((a, b) => a.edge >= b.edge ? a : b);

    return {
      tip_ref:    generateTipRef('Basketball'),
      sport:      'Basketball',
      league:     sport.league,
      home_team:  event.home_team,
      away_team:  event.away_team,
      event_time: event.commence_time,
      selection:  pick.selection,
      market:     pick.market.includes('ver') ? 'totals' : 'h2h',
      odds:       parseFloat(pick.bookOdds.toFixed(2)),
      stake:      pick.stake,
      confidence: pick.conf,
      tier:       'pro',
      status:     'pending',
      bookmaker:  pick.bookmaker,
      notes:      pick.notes,
    };
  } catch(e) {
    console.error(`NBA model error [${event.home_team} vs ${event.away_team}]:`, e.message);
    return null;
  }
}

// ── PERMANENT TEAM ID CACHE ──────────────────────────────────
// Retained for football model (API-Football team IDs).
const permanentTeamIds = {};

// ── NHL TEAM STATS CACHE ──────────────────────────────────────
// Uses the official NHL Stats API (api.nhle.com) — completely free, no auth.
// One call fetches ALL 32 teams at once — extremely efficient.
// Returns goalsPerGame and goalsAgainstPerGame directly.
const nhlTeamCache = {};
let nhlAllTeamsCache = null; // populated once per day
let nhlAllTeamsCacheDate = '';

async function fetchNHLAllTeams() {
  const today = new Date().toISOString().split('T')[0];
  if (nhlAllTeamsCache && nhlAllTeamsCacheDate === today) return nhlAllTeamsCache;

  // Season format for NHL API: YYYYYYYY e.g. 20242025
  const year = seasonFor();
  const seasonId = `${year}${year + 1}`;

  try {
    const res = await fetch(
      `https://api.nhle.com/stats/rest/en/team/summary?cayenneExp=seasonId=${seasonId}%20and%20gameTypeId=2`
    );
    if (!res.ok) return null;
    const data = await res.json();
    nhlAllTeamsCache = data.data || [];
    nhlAllTeamsCacheDate = today;
    console.log(`🏒 NHL team stats loaded: ${nhlAllTeamsCache.length} teams from NHL API`);
    return nhlAllTeamsCache;
  } catch(e) { console.error('NHL API fetch error:', e.message); return null; }
}

async function fetchNHLTeamStats(teamName) {
  const cacheKey = `${teamName}_${seasonFor()}`;
  if (nhlTeamCache[cacheKey]) return nhlTeamCache[cacheKey];

  const allTeams = await fetchNHLAllTeams();
  if (!allTeams) return null;

  // Match team by name — NHL API uses full names like "Edmonton Oilers"
  const team = allTeams.find(t =>
    nameMatch(t.teamFullName, teamName) || nameMatch(t.teamName, teamName)
  );

  if (!team) {
    console.log(`  ⚠️ NHL: no match for "${teamName}" in NHL API`);
    return null;
  }

  if ((team.gamesPlayed || 0) < 5) {
    console.log(`  ⚠️ NHL ${teamName}: only ${team.gamesPlayed} games — skipping`);
    return null;
  }

  const result = {
    teamId:       team.teamId,
    gamesPlayed:  team.gamesPlayed,
    goalsFor:     parseFloat(team.goalsForPerGame || 0),
    goalsAgainst: parseFloat(team.goalsAgainstPerGame || 0),
  };

  nhlTeamCache[cacheKey] = result;
  console.log(`  🏒 NHL ${teamName} [NHL API]: ${result.goalsFor.toFixed(2)} GF/gm, ${result.goalsAgainst.toFixed(2)} GA/gm`);
  return result;
}

// ── NHL GAME MODEL ────────────────────────────────────────────
// Uses Poisson model on goals scored/allowed.
// NHL home advantage ≈ 0.2 extra goals per game.
// Score matrix 0..7 (very rare to score more in NHL).
const NHL_HOME_ADVANTAGE  = 0.20;
const NHL_LEAGUE_AVG_GF   = 3.10; // goals per game per team, recent seasons
const NHL_MATRIX_MAX      = 7;

// ─── NHL GOALIE DATA ──────────────────────────────────────────
// BDL does not reliably expose confirmed NHL starter data.
// Flag is permanently false — all NHL tips run under 82 confidence ceiling.
const HAS_CONFIRMED_NHL_GOALIE_DATA = false;
const NHL_CONFIDENCE_CEILING = 82;

function applyNHLGoalieConfidenceCap(confidence) {
  if (!HAS_CONFIRMED_NHL_GOALIE_DATA) {
    return Math.min(confidence, NHL_CONFIDENCE_CEILING);
  }
  return confidence;
}

async function analyseNHLFixture(event, sport) {
  try {
    const [homeStats, awayStats] = await Promise.all([
      fetchNHLTeamStats(event.home_team),
      fetchNHLTeamStats(event.away_team),
    ]);
    if (!homeStats || !awayStats) return null;

    // Attack/defence strengths relative to league average
    const homeAttack = homeStats.goalsFor     / NHL_LEAGUE_AVG_GF;
    const homeDef    = homeStats.goalsAgainst / NHL_LEAGUE_AVG_GF;
    const awayAttack = awayStats.goalsFor     / NHL_LEAGUE_AVG_GF;
    const awayDef    = awayStats.goalsAgainst / NHL_LEAGUE_AVG_GF;

    // Expected goals (lambda) with home advantage
    let lH = homeAttack * awayDef * NHL_LEAGUE_AVG_GF + NHL_HOME_ADVANTAGE;
    let lA = awayAttack * homeDef * NHL_LEAGUE_AVG_GF;
    lH = Math.max(0.5, Math.min(6.0, lH));
    lA = Math.max(0.5, Math.min(6.0, lA));

    // Score matrix using standard Poisson (no DC correction for hockey)
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
    // Renormalise
    if (total > 0) {
      for (let h = 0; h <= N; h++)
        for (let a = 0; a <= N; a++)
          matrix[h][a] /= total;
    }

    // Outcome probabilities
    let homeWin = 0, awayWin = 0, draw = 0, over55 = 0;
    for (let h = 0; h <= N; h++) {
      for (let a = 0; a <= N; a++) {
        const p = matrix[h][a];
        if (h > a)        homeWin += p;
        else if (h < a)   awayWin += p;
        else              draw    += p;
        if (h + a > 5.5)  over55  += p;
      }
    }

    // In NHL, regulation draws go to OT/shootout — roughly 50/50
    // Adjust for regulation result markets (some books price reg draw separately)
    // For H2H moneyline markets, include OT so: home = homeWin + draw*0.5, away = awayWin + draw*0.5
    const homeWinML = homeWin + draw * 0.5;
    const awayWinML = awayWin + draw * 0.5;

    const market = extractMarketData(event);
    if (!market) return null;

    const candidates = [];
    const injPen = injuryPenalty(event.home_team, event.away_team, 'nhl');

    // Home win (moneyline incl OT)
    if (market.homeOdds >= 1.25 && market.homeOdds <= 4.5 && market.trueHome > 0) {
      const edge = calcEdge(homeWinML, market.trueHome);
      if (edge >= MIN_EDGE_PCT) {
        const conf  = confidenceFromSignals({ edgePct: edge, modelProb: homeWinML, dataQualityTier: 1.0, sport: 'Ice Hockey' });
        const stake = kellyStake(homeWinML, market.homeOdds);
        if (stake > 0 && conf >= MIN_CONFIDENCE) candidates.push({
          selection: `${event.home_team} Win`, market: 'home', edge,
          modelProb: homeWinML, bookOdds: market.homeOdds, bookmaker: market.homeBook,
          stake, conf,
          notes: `xG: ${lH.toFixed(2)} vs ${lA.toFixed(2)} | Model: ${(homeWinML*100).toFixed(1)}% home win (incl OT) | Fair: ${fairOdds(homeWinML)} | Book: ${market.homeOdds} (${market.homeBook}) | Edge: +${edge.toFixed(1)}%${injPen > 0 ? ` | ⚠️ ${injPen} injuries` : ''}`,
        });
      }
    }

    // Away win (moneyline incl OT)
    if (market.awayOdds >= 1.25 && market.awayOdds <= 4.5 && market.trueAway > 0) {
      const edge = calcEdge(awayWinML, market.trueAway);
      if (edge >= MIN_EDGE_PCT) {
        const conf  = confidenceFromSignals({ edgePct: edge, modelProb: awayWinML, dataQualityTier: 1.0, sport: 'Ice Hockey' });
        const stake = kellyStake(awayWinML, market.awayOdds);
        if (stake > 0 && conf >= MIN_CONFIDENCE) candidates.push({
          selection: `${event.away_team} Win`, market: 'away', edge,
          modelProb: awayWinML, bookOdds: market.awayOdds, bookmaker: market.awayBook,
          stake, conf,
          notes: `xG: ${lH.toFixed(2)} vs ${lA.toFixed(2)} | Model: ${(awayWinML*100).toFixed(1)}% away win (incl OT) | Fair: ${fairOdds(awayWinML)} | Book: ${market.awayOdds} (${market.awayBook}) | Edge: +${edge.toFixed(1)}%${injPen > 0 ? ` | ⚠️ ${injPen} injuries` : ''}`,
        });
      }
    }

    // Over 5.5 goals
    if (market.over25Odds >= 1.50 && market.over25Odds <= 2.40) {
      const over55IP = 1 / market.over25Odds;
      const edge = calcEdge(over55, over55IP);
      if (edge >= MIN_EDGE_PCT) {
        const conf  = confidenceFromSignals({ edgePct: edge, modelProb: over55, dataQualityTier: 1.0, sport: 'Ice Hockey' });
        const stake = kellyStake(over55, market.over25Odds);
        if (stake > 0 && conf >= MIN_CONFIDENCE) candidates.push({
          selection: 'Over 5.5', market: 'over55', edge,
          modelProb: over55, bookOdds: market.over25Odds, bookmaker: market.over25Book,
          stake, conf,
          notes: `xG: ${lH.toFixed(2)} vs ${lA.toFixed(2)} | Model: ${(over55*100).toFixed(1)}% over 5.5 | Edge: +${edge.toFixed(1)}%`,
        });
      }
    }

    if (!candidates.length) return null;

    // ── Veto + quality score selection ─────────────────────────
    const vetoed = candidates.filter(c => !vetoCandidate({
      sport:             'Ice Hockey',
      hasCoreData:       !!(homeStats && awayStats),
      hasCompleteMarket: !!market,
      homeWinProb:       homeWinML,
      drawProb:          0,
      awayWinProb:       awayWinML,
      adverseLineMove:   0,
    }));
    const nhlTier = getDataQualityTier({ hasFullStats: !!(homeStats && awayStats), hasFormFallback: false });
    const withTier = vetoed.map(c => ({ ...c, dataQualityTier: nhlTier }));
    const pick = pickBestCandidate(withTier);
    if (!pick) return null;

    return {
      tip_ref:    generateTipRef('Ice Hockey'),
      sport:      'Ice Hockey',
      league:     sport.league,
      home_team:  event.home_team,
      away_team:  event.away_team,
      event_time: event.commence_time,
      selection:  pick.selection,
      market:     pick.market.includes('ver') ? 'totals' : 'h2h',
      odds:       parseFloat(pick.bookOdds.toFixed(2)),
      stake:      pick.stake,
      confidence: pick.conf,
      tier:       'pro',
      status:     'pending',
      bookmaker:  pick.bookmaker,
      notes:      pick.notes,
    };
  } catch(e) {
    console.error(`NHL model error [${event.home_team} vs ${event.away_team}]:`, e.message);
    return null;
  }
}



// ═══════════════════════════════════════════════════════════════
// ODDS FETCHING
// ═══════════════════════════════════════════════════════════════

async function fetchOdds(sportKey) {
  try {
    const url = `${ODDS_BASE}/sports/${sportKey}/odds/?apiKey=${ODDS_API_KEY}&regions=uk&markets=h2h,totals&oddsFormat=decimal&dateFormat=iso`;
    const res = await fetch(url);
    if (!res.ok) { console.log(`No data ${sportKey}: ${res.status}`); return []; }
    return await res.json() || [];
  } catch(e) { console.error(`Fetch error ${sportKey}:`, e.message); return []; }
}

// ═══════════════════════════════════════════════════════════════
// NON-FOOTBALL TIP ANALYSIS (unchanged)
// analyseH2H and analyseTotals are only called for non-football sports.
// Football goes through analyseFootballFixture above.
// ═══════════════════════════════════════════════════════════════

function analyseH2H(event, books, sport) {
  try {
    const oddsMap = {};
    for (const book of books)
      for (const o of book.outcomes) {
        if (!oddsMap[o.name]) oddsMap[o.name] = [];
        oddsMap[o.name].push(o.price);
      }

    let best = null, bestVal = 0;
    for (const [team, odds] of Object.entries(oddsMap)) {
      const maxOdds  = Math.max(...odds);
      const avgOdds  = odds.reduce((a,b) => a+b,0) / odds.length;
      const impliedP = 1 / avgOdds;
      if (impliedP > 0.45 && maxOdds >= 1.35 && maxOdds <= 5.50 && impliedP * 100 > bestVal) {
        bestVal = impliedP * 100;
        const isHome = team === event.home_team;
        const isDraw = team !== event.home_team && team !== event.away_team;
        let conf = Math.min(92, Math.round(50 + impliedP * 50));
        // Non-football only — form cache not available for these sports
        best = { selection: isDraw ? 'Draw' : `${team} Win`, odds: maxOdds, confidence: conf, isHome, bookCount: books.length };
      }
    }

    if (!best || best.confidence < MIN_CONFIDENCE) return null;
    const stake = best.confidence >= 90 ? 3.0 : best.confidence >= 82 ? 2.0 : best.confidence >= 75 ? 1.5 : 1.0;

    return {
      tip_ref: generateTipRef(sport.name), sport: sport.name, league: sport.league,
      home_team: event.home_team, away_team: event.away_team, event_time: event.commence_time,
      selection: best.selection, market: 'h2h', odds: parseFloat(best.odds.toFixed(2)),
      stake, confidence: best.confidence, tier: 'pro', status: 'pending',
      bookmaker: event.bookmakers[0]?.title || 'Multiple',
      notes: `${best.bookCount} bookmakers | Market consensus`,
    };
  } catch(e) { return null; }
}

function analyseTotals(event, books, sport) {
  try {
    const overOdds = [], underOdds = [];
    let point = null;
    for (const book of books)
      for (const o of book.outcomes) {
        if (o.name === 'Over')  { overOdds.push(o.price);  point = o.point; }
        if (o.name === 'Under') { underOdds.push(o.price); }
      }

    if (!overOdds.length || !underOdds.length || !point) return null;
    const avgOver  = overOdds.reduce((a,b)=>a+b,0)  / overOdds.length;
    const avgUnder = underOdds.reduce((a,b)=>a+b,0) / underOdds.length;
    const overP = 1/avgOver, underP = 1/avgUnder;
    if (Math.max(overP, underP) < 0.52) return null;

    const pickOver = overP >= underP;
    const bestOdds = pickOver ? Math.max(...overOdds) : Math.max(...underOdds);
    if (bestOdds < 1.55 || bestOdds > 2.30) return null;

    const conf = Math.min(84, Math.round(48 + Math.max(overP, underP) * 40));
    if (conf < MIN_CONFIDENCE) return null;
    const stake = conf >= 84 ? 2.0 : conf >= 75 ? 1.5 : 1.0;

    return {
      tip_ref: generateTipRef(sport.name), sport: sport.name, league: sport.league,
      home_team: event.home_team, away_team: event.away_team, event_time: event.commence_time,
      selection: `${pickOver ? 'Over' : 'Under'} ${point}`, market: 'totals',
      odds: parseFloat(bestOdds.toFixed(2)), stake, confidence: conf,
      tier: 'pro', status: 'pending', bookmaker: event.bookmakers[0]?.title || 'Multiple',
      notes: `${books.length} bookmakers | Market consensus`,
    };
  } catch(e) { return null; }
}


// ═══════════════════════════════════════════════════════════════
// STRICT RULES ENGINE — v7
// ═══════════════════════════════════════════════════════════════
// Every tip candidate passes through this filter before being saved.
// If any rule fails, the tip is silently dropped — no output, no log.
// Rules are applied in this order:
//   1. Sport allowed
//   2. Market allowed
//   3. Odds range
//   4. Edge threshold (market + sport specific)
//   5. Line movement check
//   6. Grade assignment (A+ or A only — below A = reject)
//   7. Stake assignment from grade
//
// EXACT THRESHOLDS (not to be changed without documented evidence):
//   H2H edge:      ≥ 8%  (Football: ≥ 10% due to secondary status)
//   Overs edge:    ≥ 12% (Football overs: not allowed)
//   Elite H2H:     ≥ 10% (Football: ≥ 12%)
//   Elite Overs:   ≥ 14%
//   Odds min:      1.40
//   Odds core max: 2.20
//   Odds elite max:2.50 (only A+ grade can reach this band)
//   Line move:     reject if odds dropped ≥ 0.10 vs opening price stored in best_odds
// ═══════════════════════════════════════════════════════════════

function applyStrictRules(tip, existingBestOdds = null) {
  // ── Rule 1: Sport must be allowed ──────────────────────────
  const ALLOWED_SPORTS = ['Ice Hockey', 'Basketball', 'Football'];
  if (!ALLOWED_SPORTS.includes(tip.sport)) return null;

  // ── Rule 2: Market must be allowed ─────────────────────────
  // H2H (win) always allowed for approved sports.
  // Totals (overs) allowed for Ice Hockey and Basketball only.
  // Football: H2H only. No totals. No unders.
  const isH2H    = tip.market === 'h2h';
  const isTotals = tip.market === 'totals';
  const isOver   = isTotals && (tip.selection||'').toLowerCase().startsWith('over');
  const isUnder  = isTotals && (tip.selection||'').toLowerCase().startsWith('under');
  const isFootball = tip.sport === 'Football';

  if (!isH2H && !isTotals) return null;           // unknown market
  if (isUnder) return null;                        // Unders: banned
  if (isFootball && isTotals) return null;         // Football overs: banned
  if (isTotals && isFootball) return null;         // belt and braces

  // ── Rule 3: Odds range ─────────────────────────────────────
  const odds = parseFloat(tip.odds || 0);
  if (odds < ODDS_MIN) return null;               // below 1.40: reject
  if (odds > ODDS_ELITE_MAX) return null;         // above 2.50: reject always

  // ── Rule 4: Edge threshold (sport + market specific) ───────
  const edge = parseFloat(tip.confidence || 0) - (1 / odds) * 100;

  // Football is secondary — requires higher edge than primary sports
  const h2hMin    = isFootball ? MIN_EDGE_PCT + FOOTBALL_EDGE_PREMIUM : MIN_EDGE_PCT;
  const eliteH2H  = isFootball ? ELITE_H2H_EDGE + FOOTBALL_EDGE_PREMIUM : ELITE_H2H_EDGE;

  if (isH2H && edge < h2hMin) return null;
  if (isOver && edge < OVERS_MIN_EDGE) return null;

  // ── Rule 5: Line movement check ────────────────────────────
  // If we have a stored best_odds (highest price seen), and current odds
  // have dropped materially below that, the market has moved against us.
  // This is a sharp signal that we are on the wrong side.
  if (existingBestOdds && existingBestOdds > 0) {
    const lineMove = existingBestOdds - odds; // positive = odds shortened (bad)
    if (lineMove >= LINE_MOVE_REJECT) return null; // moved ≥ 0.10 against: reject
  }

  // ── Rule 6: Odds band restriction for elite range ──────────
  // 2.21–2.50 is only accessible to A+ grade selections.
  // Determine grade first, then enforce band restriction.
  let grade;
  if (isH2H) {
    grade = edge >= eliteH2H ? 'A+' : edge >= h2hMin ? 'A' : null;
  } else {
    // Overs
    grade = edge >= ELITE_OVERS_EDGE ? 'A+' : edge >= OVERS_MIN_EDGE ? 'A' : null;
  }

  if (!grade) return null;                        // below A: reject
  if (odds > ODDS_CORE_MAX && grade !== 'A+') return null; // 2.21–2.50: A+ only

  // ── Rule 7: Stake from confidence + grade ─────────────────
  // Confidence gates first — certainty must justify risk.
  // Grade then rewards edge on higher-confidence selections.
  //
  // < 80% confidence:           1u  (publication floor, protect bank)
  // 80–84%, A grade:            1.5u
  // 80–84%, A+:                 2u
  // ≥ 85%, A grade:             1.5u (grade A capped — edge not elite)
  // ≥ 85%, A+:                  2.5u
  //
  // Elite odds band (2.21–2.50) reduces A+ by 0.5u for higher variance.
  const conf = parseFloat(tip.confidence || 0);
  let stake;
  if (conf < 80) {
    stake = 1.0;
  } else if (conf < 85) {
    stake = grade === 'A+' ? 2.0 : 1.5;
  } else {
    stake = grade === 'A+' ? 2.5 : 1.5;
  }
  // Elite odds band variance reduction
  if (odds > ODDS_CORE_MAX && stake > 1.0) stake -= 0.5;

  // ── Output: attach grade and corrected stake ────────────────
  return {
    ...tip,
    stake,
    confidence: tip.confidence, // unchanged — model probability stays
    notes: (tip.notes || '') + ` | Grade: ${grade} | Edge: +${edge.toFixed(1)}%`,
  };
}

// ═══════════════════════════════════════════════════════════════
// TIP GENERATION — ONE tip per game
// [CHANGED] Football goes through analyseFootballFixture.
// All other sports unchanged.
// generateTips is now async because analyseFootballFixture is async.
// ═══════════════════════════════════════════════════════════════

async function generateTips(events, sport) {
  const tips = [];
  for (const event of events) {
    if (!event.bookmakers || event.bookmakers.length < 2) continue;
    const hours = (new Date(event.commence_time) - new Date()) / 3600000;
    if (hours < 0 || hours > 48) continue;

    const candidates = [];

    if (sport.name === 'Football') {
      // ── Football lab: log all markets for research ────────
      // Runs silently alongside main engine, zero extra API calls
      await runFootballLab(supabase, [event], sport, teamStatsCache, seasonFor(sport.leagueId));

      // ── Football: Dixon-Coles Poisson xG model ───────────
      // Falls back to market consensus if model has no data or no edge found.
      // This keeps football tips flowing even when API budget is exhausted
      // or early-season stats are insufficient (< 4 games played).
      const tip = await analyseFootballFixture(event, sport);
      if (tip) {
        candidates.push(tip);
      } else {
        // Fallback: market consensus with margin stripping
        const h2h    = event.bookmakers.map(b => b.markets?.find(m => m.key === 'h2h')).filter(Boolean);
        const totals = event.bookmakers.map(b => b.markets?.find(m => m.key === 'totals')).filter(Boolean);
        if (h2h.length    >= 2) { const t = analyseH2H(event, h2h, sport);     if (t) candidates.push(t); }
        if (totals.length >= 2) { const t = analyseTotals(event, totals, sport); if (t) candidates.push(t); }
      }
    } else if (sport.name === 'Basketball') {
      // ── NBA: pace-adjusted scoring model ─────────────────
      // Falls back to market consensus if stats unavailable
      const tip = await analyseNBAFixture(event, sport);
      if (tip) {
        candidates.push(tip);
      } else {
        const h2h = event.bookmakers.map(b => b.markets?.find(m => m.key === 'h2h')).filter(Boolean);
        if (h2h.length >= 2) { const t = analyseH2H(event, h2h, sport); if (t) candidates.push(t); }
      }
    } else if (sport.name === 'Ice Hockey') {
      // ── NHL: Poisson goals model ──────────────────────────
      // Falls back to market consensus if stats unavailable
      const tip = await analyseNHLFixture(event, sport);
      if (tip) {
        candidates.push(tip);
      } else {
        const h2h = event.bookmakers.map(b => b.markets?.find(m => m.key === 'h2h')).filter(Boolean);
        if (h2h.length >= 2) { const t = analyseH2H(event, h2h, sport); if (t) candidates.push(t); }
      }
    } else {
      // ── NFL / MLB / Other: market consensus ───────────────
      const h2h    = event.bookmakers.map(b => b.markets?.find(m => m.key === 'h2h')).filter(Boolean);
      const totals = event.bookmakers.map(b => b.markets?.find(m => m.key === 'totals')).filter(Boolean);
      if (h2h.length    >= 2) { const t = analyseH2H(event, h2h, sport);     if (t) candidates.push(t); }
      if (totals.length >= 2) { const t = analyseTotals(event, totals, sport); if (t) candidates.push(t); }
    }

    if (!candidates.length) continue;
    // Pick highest-confidence candidate, then apply strict rules filter.
    // If the winner fails any rule, the game produces no tip — silent drop.
    const best = candidates.reduce((a,b) => a.confidence >= b.confidence ? a : b);
    const approved = applyStrictRules(best);
    if (approved) tips.push(approved);
  }
  return tips;
}

// ═══════════════════════════════════════════════════════════════
// SAVE TIPS — one per game, update if improved
// ═══════════════════════════════════════════════════════════════

async function saveTips(tips) {
  if (!tips.length) return;
  let saved = 0, updated = 0, skipped = 0;
  // Hard block — never write Baseball or NFL to database regardless of source
  const BLOCKED_SPORTS = ['Baseball', 'NFL'];
  const filtered = tips.filter(t => !BLOCKED_SPORTS.includes(t.sport));
  if (filtered.length !== tips.length) {
    console.log(`🚫 Blocked ${tips.length - filtered.length} tips from removed sports`);
  }
  tips = filtered;
  if (!tips.length) return;

  for (const tip of tips) {
    try {
      const date = new Date(tip.event_time).toISOString().split('T')[0];
      const { data: existing } = await supabase.from('tips').select('id, tip_ref, confidence, odds, best_odds, bookmaker')
        .eq('home_team', tip.home_team).eq('away_team', tip.away_team)
        .gte('event_time', `${date}T00:00:00Z`).lte('event_time', `${date}T23:59:59Z`)
        .in('status', ['pending', 'won', 'lost', 'void']).maybeSingle();
      if (existing) {
  if (existing.status !== 'pending') { skipped++; continue; } // already settled, skip
        // Always refresh odds and bookmaker — lines move every 15 minutes.
        // Also update confidence, stake and notes if the model reprices.
        // Only skip if literally nothing has changed (within tiny float tolerance).
        const oddsDiff = Math.abs(tip.odds - existing.odds);
        const confDiff = Math.abs(tip.confidence - existing.confidence);
        const bookChanged = tip.bookmaker !== existing.bookmaker;

        // Track the best (highest) odds seen for this tip across all refreshes.
        // Settlement P&L uses best_odds so ROI reflects the most favourable
        // price a subscriber could have taken, not just the final price.
        const currentBest = parseFloat(existing.best_odds || existing.odds || 0);
        const newBest = Math.max(currentBest, tip.odds);
        const bestOddsImproved = newBest > currentBest + 0.001;

        if (oddsDiff > 0.01 || confDiff > 1 || bookChanged || bestOddsImproved) {
          // Use id for update in case tip_ref is NULL on legacy rows
          const updateKey = existing.tip_ref ? { tip_ref: existing.tip_ref } : { id: existing.id };
          const updateField = existing.tip_ref ? 'tip_ref' : 'id';
          const updateVal = existing.tip_ref || existing.id;
          await supabase.from('tips').update({
            odds:       tip.odds,
            best_odds:  newBest,
            bookmaker:  tip.bookmaker,
            confidence: tip.confidence,
            stake:      tip.stake,
            selection:  tip.selection,
            market:     tip.market,
            notes:      tip.notes,
            // Backfill tip_ref if missing
            ...(existing.tip_ref ? {} : { tip_ref: tip.tip_ref }),
          }).eq(updateField, updateVal);
          updated++;
        } else { skipped++; }
        continue;
      }

      // Set best_odds = starting odds on first insert
      const tipWithBest = { ...tip, best_odds: tip.odds };
      const { error } = await supabase.from('tips').insert(tipWithBest);
      if (error) { if (error.code === '23505') skipped++; else console.error('Insert error:', error.message); }
      else saved++;
    } catch(e) { console.error('saveTips error:', e.message); }
  }
  console.log(`✅ Tips: ${saved} new, ${updated} updated, ${skipped} unchanged`);
}

// ═══════════════════════════════════════════════════════════════
// RESULT SETTLER — runs every 60 minutes
// [CHANGED] Uses seasonFor() — no hardcoded season.
// ═══════════════════════════════════════════════════════════════



async function settleResults() {
  // Fetch pending tips AND any settled tips missing from results_history
  const { data: pendingTips } = await supabase.from('tips').select('*').eq('status','pending').lt('event_time', new Date().toISOString());
  
  // Also catch tips that settled in tips table but never made it to results_history
  const { data: missingFromHistory } = await supabase.from('tips')
    .select('*')
    .in('status', ['won', 'lost'])
    .lt('event_time', new Date().toISOString())
    .not('tip_ref', 'in', 
      `(${(await supabase.from('results_history').select('tip_ref')).data?.map(r => `'${r.tip_ref}'`).join(',') || "''"})` 
    );

  const pending = [...(pendingTips || []), ...(missingFromHistory || [])];
  if (!pending?.length) { console.log('🏁 Nothing to settle.'); return; }

  // Backfill any NULL tip_refs before settling
  const nullRefs = pending.filter(t => !t.tip_ref);
  for (const t of nullRefs) {
    const prefixes = { 'Football':'FB','Basketball':'BB','Ice Hockey':'IH' };
    const prefix = prefixes[t.sport] || 'TT';
    const ref = `${prefix}-${Date.now().toString(36).toUpperCase().slice(-4)}${Math.random().toString(36).toUpperCase().slice(-4)}`;
    await supabase.from('tips').update({ tip_ref: ref }).eq('id', t.id);
    t.tip_ref = ref;
  }

  console.log(`🏁 Settling ${pending.length} tips...`);

  // Fetch current running PL once before the loop to avoid race condition
  const { data: lastRow } = await supabase.from('results_history').select('running_pl').order('settled_at', { ascending: false }).limit(1).maybeSingle();
  let currentRunningPL = parseFloat(lastRow?.running_pl || 0);

  const now = Date.now();
  let count = 0;

  for (const tip of pending) {
    try {
      const hoursOld = (now - new Date(tip.event_time).getTime()) / 3600000;
      if (hoursOld > 72) {
        await supabase.from('tips').update({ status: 'void' }).eq('tip_ref', tip.tip_ref);
        console.log(`⚪ VOID: [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team}`);
        continue;
      }

      let homeScore = null, awayScore = null;

      if (tip.sport === 'Football') {
        // Use Odds API scores endpoint for football settlement.
        // Works for all leagues including Champions League with no season restrictions.
        // No API-Football budget consumed — 1 Odds API credit per call, cached per sport.
        const sport = SPORTS.find(s => s.league === tip.league);
        if (!sport) continue;
        try {
          const res = await fetch(`${ODDS_BASE}/sports/${sport.key}/scores/?apiKey=${ODDS_API_KEY}&daysFrom=3`);
          if (!res.ok) { console.log(`⏳ Scores API error ${res.status}: ${tip.home_team} vs ${tip.away_team}`); continue; }
          const scores = await res.json();
          const match = scores.find(s => s.completed && (
            (nameMatch(s.home_team, tip.home_team) && nameMatch(s.away_team, tip.away_team)) ||
            (nameMatch(s.home_team, tip.away_team) && nameMatch(s.away_team, tip.home_team))
          ));
          if (!match?.scores) { console.log(`⏳ No result yet: ${tip.home_team} vs ${tip.away_team}`); continue; }
          const hh = nameMatch(match.home_team, tip.home_team);
          const hs  = parseFloat(match.scores[0]?.score ?? 0);
          const as2 = parseFloat(match.scores[1]?.score ?? 0);
          homeScore = hh ? hs : as2;
          awayScore = hh ? as2 : hs;
        } catch(e) { console.error(`Scores fetch error:`, e.message); continue; }

      } else if (['Basketball','Ice Hockey'].includes(tip.sport)) { // NFL and Baseball removed
        const sport = SPORTS.find(s => s.name === tip.sport && s.league === tip.league);
        if (!sport) continue;
        const res = await fetch(`${ODDS_BASE}/sports/${sport.key}/scores/?apiKey=${ODDS_API_KEY}&daysFrom=3`);
        if (!res.ok) continue;
        const scores = await res.json();
        const match  = scores.find(s => s.completed && (
          (nameMatch(s.home_team, tip.home_team) && nameMatch(s.away_team, tip.away_team)) ||
          (nameMatch(s.home_team, tip.away_team) && nameMatch(s.away_team, tip.home_team))
        ));
        if (!match?.scores) { console.log(`⏳ No scores: ${tip.home_team} vs ${tip.away_team}`); continue; }
        const hh = nameMatch(match.home_team, tip.home_team);
        const hs  = parseFloat(match.scores[0]?.score ?? 0);
        const as2 = parseFloat(match.scores[1]?.score ?? 0);
        homeScore = hh ? hs : as2;
        awayScore = hh ? as2 : hs;
      } else { continue; }

      if (homeScore === null || awayScore === null) continue;

      let won = false;
      const sel = tip.selection.toLowerCase();
      if (sel.includes('win')) {
        const team = tip.selection.replace(/ win$/i,'').trim();
        won = nameMatch(team, tip.home_team) ? homeScore > awayScore : awayScore > homeScore;
      } else if (sel === 'draw') {
        won = homeScore === awayScore;
      } else if (sel.startsWith('over')) {
        won = (homeScore + awayScore) > parseFloat(sel.replace('over ',''));
      } else if (sel.startsWith('under')) {
        won = (homeScore + awayScore) < parseFloat(sel.replace('under ',''));
      } else if (sel.includes('btts')) {
        won = homeScore > 0 && awayScore > 0;
      }

      // Use best_odds for P&L — reflects the highest price seen during the tip's
      // lifetime, which is the most favourable price subscribers could have taken.
      // Falls back to tip.odds if best_odds not yet stored (legacy tips).
      const settlementOdds = parseFloat(tip.best_odds || tip.odds);
      const pl = won
        ? parseFloat(((settlementOdds - 1) * tip.stake).toFixed(2))
        : parseFloat((-tip.stake).toFixed(2));

      const { data: already } = await supabase.from('results_history').select('id').eq('tip_ref', tip.tip_ref).maybeSingle();
      
      // Always update tips status regardless of results_history state
      await supabase.from('tips').update({ status: won ? 'won' : 'lost', profit_loss: pl, result_updated_at: new Date().toISOString() }).eq('tip_ref', tip.tip_ref);

      // If already in results_history, skip insert — but still updated tips above
      if (already) continue;

      // Use in-memory running PL tracker (set before loop) to avoid race condition
      // where multiple tips settling in same cycle all read the same last row
      currentRunningPL = parseFloat((currentRunningPL + pl).toFixed(2));

      await supabase.from('results_history').insert({
        tip_ref:    tip.tip_ref,
        sport:      tip.sport,
        event:      `${tip.home_team} vs ${tip.away_team}`,
        selection:  tip.selection,
        odds:       settlementOdds,   // best odds seen — used for P&L
        stake:      tip.stake,
        tier:       tip.tier || 'pro',
        result:     won ? 'WON' : 'LOST',
        profit_loss: pl,
        running_pl:  currentRunningPL,
        settled_at:  (() => { const d = new Date(); const ukDate = d.toLocaleDateString('en-CA', { timeZone: 'Europe/London' }); return ukDate + 'T12:00:00.000Z'; })(),
        confidence:  tip.confidence || 0,
      });

      console.log(`${won ? '✅ WON' : '❌ LOST'}: [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team} — ${tip.selection} @ ${settlementOdds}${settlementOdds > tip.odds ? ' (best: '+settlementOdds+' vs current: '+tip.odds+')' : ''} (${pl >= 0 ? '+' : ''}${pl}u)`);
      count++;
      await updateStatsCache();

    } catch(e) { console.error(`Settle error [${tip.tip_ref}]:`, e.message); }
  }
  console.log(`🏁 Settled ${count} tips.`);
  await settleFootballLab(supabase, ODDS_BASE, ODDS_API_KEY);

  // ── Settle pending daily accas ──────────────────────────────
  // Won only if ALL active legs won. Lost if ANY leg lost.
  // Void legs are excluded — acca continues on remaining legs.
  // Stays pending if any active leg is still unsettled.
  try {
    const { data: pendingAccas } = await supabase
      .from('daily_accas')
      .select('*')
      .eq('result', 'pending');

    for (const acca of (pendingAccas || [])) {
      const tipRefs = (acca.selections || []).map(s => s.tip_ref).filter(Boolean);
      if (!tipRefs.length) continue;

      const { data: legTips } = await supabase
        .from('tips')
        .select('tip_ref, status, odds, best_odds')
        .in('tip_ref', tipRefs);

      if (!legTips || legTips.length < tipRefs.length) continue; // not all legs found yet

      const allSettled = legTips.every(t => ['won','lost','void'].includes(t.status));
      if (!allSettled) continue; // still waiting on at least one result

      // Exclude void legs — acca continues on remaining active legs
      const activeLegs = legTips.filter(t => t.status !== 'void');

      if (!activeLegs.length) {
        // All legs voided — mark acca void, no P&L
        await supabase.from('daily_accas')
          .update({ result: 'VOID', profit_loss: 0 })
          .eq('id', acca.id);
        console.log(`📋 Acca ${acca.date} VOID (all legs voided)`);
        continue;
      }

      const allWon = activeLegs.every(t => t.status === 'won');
      const result = allWon ? 'WON' : 'LOST';

      let pl;
      if (allWon) {
        // Use best_odds per leg for the combined return
        const combinedBest = activeLegs.reduce((acc, t) => acc * parseFloat(t.best_odds || t.odds), 1);
        pl = parseFloat(((combinedBest - 1) * parseFloat(acca.stake || 1)).toFixed(2));
      } else {
        pl = parseFloat((-parseFloat(acca.stake || 1)).toFixed(2));
      }

      await supabase.from('daily_accas')
        .update({ result, profit_loss: pl })
        .eq('id', acca.id);

      console.log(`📋 Acca ${acca.date} settled: ${result} (${pl >= 0 ? '+' : ''}${pl}u)`);
    }
  } catch(e) {
    console.error('Acca settlement error:', e.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// STATS CACHE
// ═══════════════════════════════════════════════════════════════

async function updateStatsCache() {
  try {
    const { data } = await supabase.from('results_history').select('result, profit_loss, stake');
    if (!data) return;
    const won   = data.filter(r => r.result === 'WON').length;
    const lost  = data.filter(r => r.result === 'LOST').length;
    const total = won + lost;
    const pl    = data.reduce((s,r) => s + parseFloat(r.profit_loss || 0), 0);
    const stk   = data.reduce((s,r) => s + parseFloat(r.stake || 1), 0);
    await supabase.from('stats_cache').update({
      total_tips:   total,
      total_won:    won,
      total_lost:   lost,
      win_rate:     total > 0 ? parseFloat((won/total*100).toFixed(1)) : 0,
      total_pl:     parseFloat(pl.toFixed(2)),
      total_staked: parseFloat(stk.toFixed(2)),
      roi:          stk > 0 ? parseFloat((pl/stk*100).toFixed(1)) : 0,
    }).eq('id', 1);
    console.log(`📈 Stats: ${won}W/${lost}L | ${total > 0 ? (won/total*100).toFixed(1) : 0}% | ${pl >= 0 ? '+' : ''}${pl.toFixed(2)}u`);
  } catch(e) { console.error('Stats cache error:', e.message); }
}

// ═══════════════════════════════════════════════════════════════
// MAIN ENGINE LOOP — every 15 minutes
// [CHANGED] generateTips is now awaited (it is async for football)
// ═══════════════════════════════════════════════════════════════

async function runEngine() {
  console.log(`\n🚀 Engine v7.1 — ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`);
  console.log('═'.repeat(52));
  let all = [];
  for (const sport of SPORTS) {
    console.log(`Fetching ${sport.league}...`);
    const events = await fetchOdds(sport.key);
    if (events.length) {
      const tips = await generateTips(events, sport);
      console.log(`  → ${events.length} events, ${tips.length} tips`);
      all = all.concat(tips);
    }
    await new Promise(r => setTimeout(r, 400));
  }
  console.log(`\n💾 Saving ${all.length} tips...`);
  await saveTips(all);
  console.log('✅ Cycle complete.\n');
}

// ═══════════════════════════════════════════════════════════════
// EMAIL SYSTEM — UNCHANGED
// ═══════════════════════════════════════════════════════════════

const RESEND_API_KEY  = process.env.RESEND_API_KEY || '';
const FROM_EMAIL      = 'info@thetipsteredge.com';
const FROM_NAME       = 'The Tipster';
const SITE_URL        = 'https://www.thetipsteredge.com';

async function sendEmail({ to, subject, html, type = 'daily' }) {
  if (!RESEND_API_KEY) { console.error('RESEND_API_KEY not set'); return null; }
  try {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${RESEND_API_KEY}` },
      body: JSON.stringify({ from: `${FROM_NAME} <${FROM_EMAIL}>`, to: Array.isArray(to) ? to : [to], subject, html })
    });
    const data = await res.json();
    if (!res.ok) { console.error('Resend error:', data); return null; }
    await supabase.from('email_log').insert({ type, subject, recipient: Array.isArray(to) ? to.join(',') : to, status: 'sent', resend_id: data.id || null });
    console.log(`📧 Sent: ${subject}`);
    return data.id;
  } catch(e) { console.error('Email error:', e.message); return null; }
}

function generateUnsubToken(userId) {
  const secret = process.env.UNSUBSCRIBE_SECRET || '';
  return crypto.createHmac('sha256', secret).update(userId).digest('hex');
}
function verifyUnsubToken(token, userId) {
  const expected = generateUnsubToken(userId);
  return crypto.timingSafeEqual(Buffer.from(token, 'hex'), Buffer.from(expected, 'hex'));
}
function unsubLink(userId) { return `${SITE_URL}/unsubscribe?uid=${encodeURIComponent(userId)}&token=${generateUnsubToken(userId)}`; }

function emailBase(content, userId) {
  return `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#07090d;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#07090d;padding:32px 16px;"><tr><td align="center">
<table width="100%" style="max-width:560px;background:#0f141c;border:1px solid #1c2535;border-radius:10px;overflow:hidden;">
<tr><td style="padding:24px 28px 20px;border-bottom:1px solid #1c2535;">
<span style="font-family:Georgia,serif;font-size:20px;font-weight:700;color:#dde6f0;">The <span style="color:#18e07a;">Tipster</span></span>
</td></tr>
<tr><td style="padding:28px;">${content}</td></tr>
<tr><td style="padding:0 28px 24px;"><div style="margin-top:40px;padding-top:24px;border-top:1px solid #1c2535;text-align:center;">
<p style="font-family:monospace;font-size:11px;color:#4a5a70;line-height:1.6;margin:0;">
The Tipster · Tips for informational purposes only · 18+ only · Bet responsibly<br>
<a href="${SITE_URL}" style="color:#4a5a70;">Visit site</a> · <a href="${unsubLink(userId)}" style="color:#4a5a70;">Unsubscribe</a> · <a href="${SITE_URL}/account.html" style="color:#4a5a70;">Manage preferences</a>
</p></div></td></tr>
</table></td></tr></table></body></html>`;
}

function buildWelcomeEmail({ userId, firstName }) {
  const g = firstName || 'there';
  const content = `
<p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#f0b429;margin:0 0 10px;">Pro Member</p>
<h1 style="font-size:22px;font-weight:800;color:#dde6f0;margin:0 0 6px;">Welcome to Pro, ${g}.</h1>
<p style="font-size:13px;color:#4a5a70;line-height:1.7;margin:0 0 24px;">Your account is active. Tomorrow at 07:00 UK you'll receive your first full card.</p>

<table width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:20px;">
<tr><td style="padding:18px 20px;">
<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#f0b429;margin:0 0 12px;">How it works</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0 0 10px;">Every 15 minutes the engine scans odds across leading UK bookmakers. It runs each fixture through a statistical model — Dixon-Coles Poisson for football, pace-adjusted scoring for NBA, Poisson goals model for NHL — and looks for one thing: where the bookmaker has mispriced the probability of an outcome.</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0;">When it finds a genuine edge — where our model probability is meaningfully higher than the bookmaker's implied probability — it generates a tip. No gut feel. No opinions. Pure maths.</p>
</td></tr>
</table>

<table width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:20px;">
<tr><td style="padding:18px 20px;">
<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#18e07a;margin:0 0 12px;">What you receive every morning at 07:00</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0 0 8px;">✦ The full tip card — every selection the engine has approved</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0 0 8px;">✦ Value edge % — how much the model disagrees with the bookmaker</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0 0 8px;">✦ Stake recommendation — sized by the Kelly criterion (A+ or A grade only)</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0;">✦ Confidence score — how certain the model is on each selection</p>
</td></tr>
</table>

<table width="100%" cellpadding="0" cellspacing="0" style="background:#0c0f15;border:1px solid rgba(240,180,41,0.25);border-radius:7px;margin-bottom:20px;">
<tr><td style="padding:18px 20px;">
<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#f0b429;margin:0 0 12px;">The discipline section — please read this</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0 0 10px;">The edge is real but it plays out over hundreds of bets — not dozens. A 61% win rate means roughly 4 in every 10 tips lose. Losing runs of 5, 6, even 7 in a row are normal and expected. That is not the model failing. That is variance.</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0 0 10px;">The three rules that separate profitable bettors from everyone else:</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0 0 6px;"><span style="color:#f0b429;font-weight:700;">1. Follow the stakes.</span> The stake recommendation exists for a reason. Doubling up after a loss or skipping a low-stakes tip destroys the mathematical edge over time.</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0 0 6px;"><span style="color:#f0b429;font-weight:700;">2. Think in weeks, not days.</span> A bad Tuesday means nothing. A bad month is worth reviewing. Judge the service on 30-day and 90-day windows.</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0;"><span style="color:#f0b429;font-weight:700;">3. Never chase.</span> If you've had a losing day, tomorrow's card is the same as any other day. The model doesn't know you lost. Bet accordingly.</p>
</td></tr>
</table>

<div style="text-align:center;margin-bottom:8px;"><a href="${SITE_URL}/#tips" style="display:inline-block;background:#f0b429;color:#07090d;font-size:13px;font-weight:700;padding:13px 32px;border-radius:5px;text-decoration:none;">View Today's Tips</a></div>
<p style="font-size:11px;color:#4a5a70;text-align:center;margin:12px 0 0;">Your first Pro email arrives tomorrow at 07:00 UK.</p>`;
  return emailBase(content, userId);
}

function buildProEmail({ tip, allTips, userId, firstName }) {
  const g = firstName || 'there';
  const edge = (parseFloat(tip.confidence||0) - (1/parseFloat(tip.odds||1))*100).toFixed(1);
  const ec   = parseFloat(edge) >= 0 ? '#18e07a' : '#ff3d5a';
  const es   = parseFloat(edge) >= 0 ? `+${edge}%` : `${edge}%`;
  const extras = allTips.slice(1, 9).map(t => {
    const te = (parseFloat(t.confidence||0) - (1/parseFloat(t.odds||1))*100).toFixed(1);
    const tec = parseFloat(te) >= 0 ? '#18e07a' : '#ff3d5a';
    return `<tr style="border-top:1px solid #1c2535;"><td style="padding:10px 14px;">
<p style="font-size:10px;color:#4a5a70;margin:0 0 2px;font-family:monospace;text-transform:uppercase;">${t.sport} · ${t.league} · [${t.tip_ref}]</p>
<p style="font-size:13px;font-weight:700;color:#dde6f0;margin:0 0 2px;">${t.home_team} vs ${t.away_team}</p>
<p style="font-size:12px;color:#18e07a;margin:0;">${t.selection} <span style="color:#4a5a70;">@</span> <span style="color:#f0b429;font-family:monospace;">${parseFloat(t.odds).toFixed(2)}</span></p>
</td><td style="padding:10px 14px;text-align:right;white-space:nowrap;">
<p style="font-family:monospace;font-size:11px;color:${tec};margin:0;">${parseFloat(te)>=0?'+':''}${te}% edge</p>
<p style="font-family:monospace;font-size:11px;color:#4a5a70;margin:2px 0;">${t.confidence}% conf · ${t.stake}u</p>
</td></tr>`;
  }).join('');
  const content = `
<p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#f0b429;margin:0 0 10px;">Pro Early Access · 07:00</p>
<h1 style="font-size:20px;font-weight:800;color:#dde6f0;margin:0 0 4px;">Morning, ${g}. Here's your full card.</h1>
<p style="font-size:12px;color:#4a5a70;margin:0 0 22px;">${allTips.length} tips ready. Free members get one at 08:30 — you have them all now.</p>
<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#18e07a;margin:0 0 8px;">Best Pick · [${tip.tip_ref}]</p>
<table width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:16px;">
<tr><td style="padding:14px 16px;">
<p style="font-size:11px;color:#4a5a70;margin:0 0 3px;font-family:monospace;text-transform:uppercase;">${tip.sport} · ${tip.league}</p>
<p style="font-size:17px;font-weight:800;color:#dde6f0;margin:0 0 2px;">${tip.home_team} vs ${tip.away_team}</p>
<p style="font-size:14px;color:#18e07a;margin:0;">${tip.selection}</p>
</td><td style="padding:14px 16px;text-align:right;">
<p style="font-family:monospace;font-size:26px;font-weight:700;color:#f0b429;margin:0;line-height:1;">${parseFloat(tip.odds).toFixed(2)}</p>
<p style="font-family:monospace;font-size:10px;color:${ec};margin:4px 0 0;">${es} edge · ${tip.stake}u stake</p>
</td></tr></table>
${extras ? `<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#4a5a70;margin:0 0 8px;">Full Card (${allTips.length} tips)</p>
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0c0f15;border:1px solid #1c2535;border-radius:7px;margin-bottom:20px;">${extras}</table>` : ''}
<div style="text-align:center;"><a href="${SITE_URL}/#tips" style="display:inline-block;background:#f0b429;color:#07090d;font-size:13px;font-weight:700;padding:12px 28px;border-radius:5px;text-decoration:none;">View Full Card on Site</a></div>`;
  return emailBase(content, userId);
}

function buildFreeEmail({ tip, proTipCount, userId, firstName }) {
  const g    = firstName || 'there';
  const edge = (parseFloat(tip.confidence||0) - (1/parseFloat(tip.odds||1))*100).toFixed(1);
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
<p style="font-family:monospace;font-size:26px;font-weight:700;color:#f0b429;margin:0;line-height:1;">${parseFloat(tip.odds).toFixed(2)}</p>
<p style="font-family:monospace;font-size:10px;color:${ec};margin:4px 0 0;">${es} edge</p>
</td></tr></table>
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0c0f15;border:1px solid rgba(240,180,41,0.25);border-radius:7px;margin-bottom:20px;">
<tr><td style="padding:16px 18px;">
<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#f0b429;margin:0 0 8px;">Pro members got this at 07:00</p>
<p style="font-size:13px;color:#dde6f0;margin:0 0 10px;">${proTipCount} more tips were sent to Pro members 90 minutes ago — full card, value edge and stake recommendations included.</p>
<a href="${SITE_URL}/#pricing" style="display:inline-block;background:#f0b429;color:#07090d;font-size:12px;font-weight:700;padding:9px 20px;border-radius:4px;text-decoration:none;">Go Pro — £9.99/mo</a>
</td></tr></table>
<div style="text-align:center;"><a href="${SITE_URL}/#tips" style="display:inline-block;background:#18e07a;color:#07090d;font-size:13px;font-weight:700;padding:12px 28px;border-radius:5px;text-decoration:none;">View Today's Free Tips</a></div>`;
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
<div style="background:#07090d;border:1px solid #1c2535;border-radius:6px;padding:14px 16px;margin-bottom:24px;">
<p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#18e07a;margin:0 0 8px;">Why this acca</p>
<p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0;">${reasoning}</p>
</div>
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
  const s = new Date(); s.setHours(0,0,0,0);
  const e = new Date(); e.setHours(23,59,59,999);
  const { data } = await supabase.from('tips').select('*').eq('status','pending')
    .gte('event_time', s.toISOString()).lte('event_time', e.toISOString())
    .order('confidence', { ascending: false }).limit(limit);
  return data || [];
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
  const s = new Date(); s.setHours(6,0,0,0); const e = new Date(); e.setHours(23,59,59,999);
  const { data: tips } = await supabase.from('tips').select('*').eq('status','pending').eq('sport','Football')
    .gte('event_time', s.toISOString()).lte('event_time', e.toISOString())
    .gte('confidence', 72).order('confidence', { ascending: false }).limit(4);
  if (!tips || tips.length < 3) return null;
  const sels = tips.map(t => ({ match: `${t.home_team} vs ${t.away_team}`, selection: t.selection, odds: t.odds }));
  return { selections: sels, combinedOdds: sels.reduce((a,s) => a * parseFloat(s.odds), 1), reasoning: `${sels.length} high-confidence selections from today's card. All carry 72%+ model confidence. Recommended 0.5u each-way.` };
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
    if (await sendEmail({ to: u.email, subject: `${u.first_name?u.first_name+', ':''}Today's Bet of the Day | ${tip.home_team} vs ${tip.away_team}`, html, type: 'daily' })) sent++;
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
    return { success: !!(await sendEmail({ to, subject: '[TEST] Welcome to The Tipster Pro', html: buildWelcomeEmail({ userId: 'test', firstName: 'Test' }), type: 'test' })) };
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
// ADMIN JOB QUEUE — UNCHANGED
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
// STRIPE — UNCHANGED
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
    if (exp !== wsig || Math.abs(Date.now()/1000 - parseInt(ts)) > 300) return null;
    return JSON.parse(payload);
  } catch(e) { console.error('Webhook verify error:', e.message); return null; }
}

async function handleStripeWebhook(event) {
  console.log('Stripe:', event.type);
  switch(event.type) {
    case 'checkout.session.completed': {
      const s = event.data.object;
      const uid = s.metadata?.user_id;
      if (!uid) break;
      await supabase.from('users').update({ subscription_status:'pro', stripe_customer_id: s.customer, stripe_subscription_id: s.subscription }).eq('id', uid);
      console.log('Upgraded to Pro:', uid);
      const { data: u } = await supabase.from('users').select('email,first_name').eq('id', uid).single();
      if (u) await sendEmail({ to: u.email, subject: `Welcome to The Tipster Pro, ${u.first_name||'there'}`, html: buildWelcomeEmail({ userId: uid, firstName: u.first_name }), type: 'welcome_pro' });
      break;
    }
    case 'customer.subscription.updated': {
      const sub = event.data.object;
      if (sub.status === 'active') await supabase.from('users').update({ subscription_status:'pro' }).eq('stripe_customer_id', sub.customer);
      else if (sub.status === 'past_due') await supabase.from('users').update({ subscription_status:'past_due' }).eq('stripe_customer_id', sub.customer);
      break;
    }
    case 'customer.subscription.deleted':
      await supabase.from('users').update({ subscription_status:'free', stripe_subscription_id: null }).eq('stripe_customer_id', event.data.object.customer);
      console.log('Downgraded:', event.data.object.customer);
      break;
    case 'invoice.payment_failed':
      await supabase.from('users').update({ subscription_status:'past_due' }).eq('stripe_customer_id', event.data.object.customer);
      break;
    case 'invoice.payment_succeeded':
      if (event.data.object.billing_reason === 'subscription_cycle')
        await supabase.from('users').update({ subscription_status:'pro' }).eq('stripe_customer_id', event.data.object.customer);
      break;
  }
}

// ═══════════════════════════════════════════════════════════════
// BEST BET TAGGER
// Runs at 07:00 UK alongside Pro emails.
// Tags the highest confidence pending tip for today as is_best_bet.
// Only one tip per day can be tagged. Never changes after tagging.
// This is the authoritative best bet for streak calculation.
// ═══════════════════════════════════════════════════════════════

async function tagDailyBestBet() {
  try {
    // Tag best bet for TOMORROW ONLY — runs at 06:00 UK.
    // Once tagged, never changes.
    // Only ever one day ahead — no further.
    const now = new Date();
    const ukTomorrow = new Date(now.getTime() + 86400000)
      .toLocaleDateString('en-CA', { timeZone: 'Europe/London' });

    const s = ukTomorrow + 'T00:00:00Z';
    const e = ukTomorrow + 'T23:59:59Z';

    // Skip if already tagged for tomorrow
    const { data: existing } = await supabase
      .from('tips')
      .select('id')
      .eq('is_best_bet', true)
      .gte('event_time', s)
      .lte('event_time', e)
      .maybeSingle();

    if (existing) {
      console.log(`🏆 Best bet already tagged for ${ukTomorrow}.`);
      return;
    }

    // Find highest confidence pending tip for tomorrow
    const { data: tips } = await supabase
      .from('tips')
      .select('id, tip_ref, home_team, away_team, confidence, odds')
      .eq('status', 'pending')
      .gte('event_time', s)
      .lte('event_time', e)
      .order('confidence', { ascending: false })
      .order('odds', { ascending: false })
      .limit(1);

    if (!tips?.length) {
      console.log(`🏆 No pending tips yet for ${ukTomorrow}.`);
      return;
    }

    const best = tips[0];
    await supabase.from('tips').update({ is_best_bet: true }).eq('id', best.id);
    console.log(`🏆 Best bet tagged [${ukTomorrow}]: [${best.tip_ref}] ${best.home_team} vs ${best.away_team} (${best.confidence}% conf)`);

  } catch(e) {
    console.error('tagDailyBestBet error:', e.message);
  }
}

// ═══════════════════════════════════════════════════════════════
// DAILY ACCA GENERATOR — Admin use only
// Auto-runs at 07:00 UK. Also triggered via /admin/generate-acca.
// Selects top tips with confidence ≥ 84%, min 3 legs, max 5.
// Stored in daily_accas for admin tracking and settlement.
// No emails sent — admin page only.
// ═══════════════════════════════════════════════════════════════

async function generateDailyAcca() {
  try {
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'Europe/London' });

    // Don't overwrite if one already exists for today
    const { data: existing } = await supabase
      .from('daily_accas')
      .select('id')
      .eq('date', today)
      .maybeSingle();
    if (existing) {
      console.log('📋 Daily acca: already generated for today.');
      return { skipped: true };
    }

    const s = today + 'T00:00:00Z';
    const e = today + 'T23:59:59Z';

    const { data: tips } = await supabase
      .from('tips')
      .select('*')
      .eq('status', 'pending')
      .gte('event_time', s)
      .lte('event_time', e)
      .gte('confidence', 84)
      .order('confidence', { ascending: false })
      .limit(5);

    if (!tips || tips.length < 3) {
      console.log(`📋 Daily acca: only ${tips?.length || 0} qualifying tips (need ≥3 at 84%+).`);
      return { generated: false, reason: 'insufficient_tips' };
    }

    const legs = tips.slice(0, 5);

    // Determine sport label
    const sportCounts = legs.reduce((acc, t) => { acc[t.sport] = (acc[t.sport] || 0) + 1; return acc; }, {});
    const dominantSport = Object.entries(sportCounts).sort((a, b) => b[1] - a[1])[0][0];
    const sportLabel = Object.keys(sportCounts).length > 1 ? 'Mixed' : dominantSport;

    const combinedOdds = parseFloat(
      legs.reduce((acc, t) => acc * parseFloat(t.odds), 1).toFixed(4)
    );

    const selections = legs.map(t => ({
      match:      `${t.home_team} vs ${t.away_team}`,
      selection:  t.selection,
      odds:       t.odds,
      tip_ref:    t.tip_ref,
      confidence: t.confidence,
    }));

    const { error } = await supabase.from('daily_accas').insert({
      date:          today,
      sport:         sportLabel,
      legs:          legs.length,
      selections,
      combined_odds: combinedOdds,
      stake:         1,
      result:        'pending',
      profit_loss:   null,
    });

    if (error) {
      console.error('Daily acca insert error:', error.message);
      return { generated: false, error: error.message };
    }

    console.log(`📋 Daily acca generated: ${legs.length} legs @ ${combinedOdds} combined odds`);
    return { generated: true, legs: legs.length, combinedOdds };

  } catch(e) {
    console.error('generateDailyAcca error:', e.message);
    return { generated: false, error: e.message };
  }
}

// ═══════════════════════════════════════════════════════════════
// SCHEDULER — UNCHANGED
// ═══════════════════════════════════════════════════════════════

let lastProDate = '', lastFreeDate = '', lastSatDate = '', lastFormDate2 = '';

function startScheduler() {
  setInterval(async () => {
    const uk    = ukTime();
    const h     = uk.getHours();
    const m     = uk.getMinutes();
    const today = uk.toDateString();
    const isSat = uk.getDay() === 6;

    if (h === 6  && m === 0  && lastFormDate2 !== today) { lastFormDate2 = today; await fetchAllFormData(); await tagDailyBestBet(); }
    if (h === 7  && m === 0  && lastProDate   !== today) { lastProDate   = today; await tagDailyBestBet(); await sendProEmails(); await generateDailyAcca(); }
    if (isSat && h === 8 && m === 0 && lastSatDate !== today) { lastSatDate = today; await sendSaturdayEmails(); }
  }, 60 * 1000);

  setInterval(settleResults, 60 * 60 * 1000);

  console.log('⏰ Scheduler active:');
  console.log('   06:00 UK — Form data fetch');
  console.log('   07:00 UK — Pro emails + daily acca');
  console.log('   08:00 UK Sat — Saturday acca (all members)');
  console.log('   Every 60 min — Settler');
  console.log('   Every 15 min — Odds + tips');
}

// ═══════════════════════════════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════════════════════════════

(async () => {
  console.log(`\n🟢 The Tipster Engine v7.1 starting... Season: ${seasonFor()}`);
  await runEngine();
  await settleResults();
  setInterval(runEngine, 15 * 60 * 1000);
  startScheduler();
  setInterval(processAdminJobs, 30 * 1000);
  processAdminJobs();
})();

// ═══════════════════════════════════════════════════════════════
// HTTP SERVER — UNCHANGED
// ═══════════════════════════════════════════════════════════════

const http = require('http');

// ─── RATE LIMITER ─────────────────────────────────────────────
// Simple in-memory rate limiter per IP.
// Limits: 60 requests per minute for general endpoints.
const rateLimitMap = new Map();
const RATE_LIMIT    = 60;
const RATE_WINDOW   = 60 * 1000; // 1 minute

function isRateLimited(ip) {
  const now = Date.now();
  const entry = rateLimitMap.get(ip) || { count: 0, start: now };
  if (now - entry.start > RATE_WINDOW) {
    entry.count = 1; entry.start = now;
  } else {
    entry.count++;
  }
  rateLimitMap.set(ip, entry);
  // Clean up old entries every 1000 requests to prevent memory leak
  if (rateLimitMap.size > 1000) {
    for (const [key, val] of rateLimitMap) {
      if (now - val.start > RATE_WINDOW) rateLimitMap.delete(key);
    }
  }
  return entry.count > RATE_LIMIT;
}
http.createServer(async (req, res) => {
  const url  = new URL(req.url, 'http://localhost');
  const origin = req.headers['origin'] || '';
  const allowedOrigins = ['https://www.thetipsteredge.com', 'https://thetipsteredge.com', 'https://the-tipster.vercel.app'];
  const allowOrigin = allowedOrigins.includes(origin) ? origin : allowedOrigins[0];
  const cors = { 'Access-Control-Allow-Origin': allowOrigin, 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type, Authorization', 'Vary': 'Origin' };

  if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return; }

  const clientIp = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress || '';
  if (isRateLimited(clientIp)) {
    res.writeHead(429, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Too many requests' }));
    return;
  }

  if (url.pathname === '/') {
    res.writeHead(200, { ...cors, 'Content-Type': 'text/plain' });
    res.end(`The Tipster Engine v7.1 | Season: ${seasonFor()}`); return;
  }

  const adminKey = (req.headers['authorization'] || '').replace('Bearer ', '').trim();

  if (url.pathname === '/admin/generate-acca') {
    if (adminKey !== process.env.ADMIN_KEY) { res.writeHead(403); res.end('Forbidden'); return; }
    const result = await generateDailyAcca();
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true, ...result }));
    return;
  }

  if (url.pathname === '/admin/resettle') {
    if (adminKey !== process.env.ADMIN_KEY) { res.writeHead(403); res.end('Forbidden'); return; }
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    // Run async, stream result back
    (async () => {
      try {
        const log = [];
        const push = msg => { log.push(msg); console.log(msg); };

        push('🔁 HISTORICAL RESETTLEMENT STARTING...');

        const { data: tips, error: tipsErr } = await supabase
          .from('tips').select('*').in('status', ['won', 'lost'])
          .order('event_time', { ascending: true });

        if (tipsErr) throw new Error(tipsErr.message);
        push(`📋 Found ${tips.length} settled tips`);

        const scoreCache = {};
        const fetchScores = async (sportKey) => {
          if (scoreCache[sportKey]) return scoreCache[sportKey];
          const r = await fetch(`${ODDS_BASE}/sports/${sportKey}/scores/?apiKey=${ODDS_API_KEY}&daysFrom=30`);
          if (!r.ok) { scoreCache[sportKey] = []; return []; }
          const d = await r.json();
          scoreCache[sportKey] = Array.isArray(d) ? d : [];
          push(`📡 ${scoreCache[sportKey].length} scores for ${sportKey}`);
          return scoreCache[sportKey];
        };

        let corrected = 0, unchanged = 0, noMatch = 0;
        const updates = [];

        for (const tip of tips) {
          const sport = SPORTS.find(s => s.league === tip.league);
          if (!sport) { noMatch++; continue; }

          const scores = await fetchScores(sport.key);
          const match = scores.find(s => s.completed && (
            (nameMatch(s.home_team, tip.home_team) && nameMatch(s.away_team, tip.away_team)) ||
            (nameMatch(s.home_team, tip.away_team) && nameMatch(s.away_team, tip.home_team))
          ));

          if (!match?.scores || match.scores.length < 2) { noMatch++; continue; }

          const hh        = nameMatch(match.home_team, tip.home_team);
          const hs        = parseFloat(match.scores[0]?.score ?? 0);
          const as2       = parseFloat(match.scores[1]?.score ?? 0);
          const homeScore = hh ? hs  : as2;
          const awayScore = hh ? as2 : hs;

          const sel = tip.selection.toLowerCase();
          let won = false;
          if (sel.includes('win')) {
            const team = tip.selection.replace(/ win$/i, '').trim();
            won = nameMatch(team, tip.home_team) ? homeScore > awayScore : awayScore > homeScore;
          } else if (sel === 'draw') {
            won = homeScore === awayScore;
          } else if (sel.startsWith('over')) {
            won = (homeScore + awayScore) > parseFloat(sel.replace('over ', ''));
          } else if (sel.startsWith('under')) {
            won = (homeScore + awayScore) < parseFloat(sel.replace('under ', ''));
          } else if (sel.includes('btts')) {
            won = homeScore > 0 && awayScore > 0;
          }

          const settlementOdds = parseFloat(tip.best_odds || tip.odds);
          const pl = won
            ? parseFloat(((settlementOdds - 1) * tip.stake).toFixed(2))
            : parseFloat((-tip.stake).toFixed(2));
          const newStatus = won ? 'won' : 'lost';

          if (tip.status !== newStatus) {
            push(`✏️  CORRECTION [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team} | ${tip.selection} | ${homeScore}-${awayScore} | ${tip.status.toUpperCase()} → ${newStatus.toUpperCase()} | ${pl >= 0 ? '+' : ''}${pl}u`);
            corrected++;
          } else {
            unchanged++;
          }
          updates.push({ tip, won, pl, newStatus });
        }

        push(`\n📊 ${corrected} corrections, ${unchanged} unchanged, ${noMatch} no score found`);

        // Update tips table
        for (const { tip, pl, newStatus } of updates) {
          await supabase.from('tips').update({ status: newStatus, profit_loss: pl }).eq('tip_ref', tip.tip_ref);
        }

        // Rebuild results_history
        push('🗑️  Clearing results_history...');
        await supabase.from('results_history').delete().neq('id', 0);

        push('📝 Rebuilding results_history...');
        let runningPL = 0;
        const sorted = [...updates].sort((a, b) => new Date(a.tip.event_time) - new Date(b.tip.event_time));

        for (const { tip, won, pl } of sorted) {
          runningPL = parseFloat((runningPL + pl).toFixed(2));
          const settlementOdds = parseFloat(tip.best_odds || tip.odds);
          await supabase.from('results_history').insert({
            tip_ref: tip.tip_ref, sport: tip.sport,
            event: `${tip.home_team} vs ${tip.away_team}`,
            selection: tip.selection, odds: settlementOdds, stake: tip.stake,
            tier: tip.tier || 'pro', result: won ? 'WON' : 'LOST',
            profit_loss: pl, running_pl: runningPL, league: tip.league,
            confidence: tip.confidence,
            settled_at: tip.result_updated_at || tip.event_time,
          });
        }

        push(`✅ DONE — ${sorted.length} tips rebuilt | Final P&L: ${runningPL >= 0 ? '+' : ''}${runningPL}u | Corrections: ${corrected}`);
        res.end(JSON.stringify({ success: true, corrected, unchanged, noMatch, finalPL: runningPL, log }));
      } catch (e) {
        console.error('Resettle error:', e.message);
        res.end(JSON.stringify({ success: false, error: e.message }));
      }
    })();
    return;
  }

  if (url.pathname === '/admin/test-email') {
    if (adminKey !== process.env.ADMIN_KEY) { res.writeHead(403); res.end('Forbidden'); return; }
    const r = await sendTestEmail(url.searchParams.get('to'), url.searchParams.get('type')||'daily');
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify(r)); return;
  }

  if (url.pathname === '/admin/send-daily') {
    if (adminKey !== process.env.ADMIN_KEY) { res.writeHead(403); res.end('Forbidden'); return; }
    sendDailyEmails();
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true })); return;
  }

  if (url.pathname === '/admin/send-saturday') {
    if (adminKey !== process.env.ADMIN_KEY) { res.writeHead(403); res.end('Forbidden'); return; }
    sendSaturdayEmails();
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true })); return;
  }

  if (url.pathname === '/stripe/webhook' && req.method === 'POST') {
    let body = '';
    req.on('data', c => { body += c.toString(); });
    req.on('end', async () => {
      const sig = req.headers['stripe-signature'];
      if (!sig) { res.writeHead(400); res.end('Missing signature'); return; }
      const event = verifyStripeWebhook(body, sig);
      if (!event) { res.writeHead(400); res.end('Invalid signature'); return; }
      res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ received: true }));
      handleStripeWebhook(event).catch(e => console.error('Webhook error:', e.message));
    });
    return;
  }

  if (url.pathname === '/stripe/checkout' && req.method === 'POST') {
    let body = '';
    req.on('data', c => { body += c.toString(); });
    req.on('end', async () => {
      try {
        const { userId, email, plan } = JSON.parse(body);
        if (!userId || !email || !plan) { res.writeHead(400, cors); res.end('Missing params'); return; }
        const session = await createCheckoutSession(userId, email, plan === 'annual' ? STRIPE_PRICE_ANNUAL : STRIPE_PRICE_MONTHLY, plan);
        if (!session) { res.writeHead(500, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Failed' })); return; }
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ url: session.url }));
      } catch(e) { res.writeHead(500, cors); res.end('Server error'); }
    });
    return;
  }

  if (url.pathname === '/stripe/config') {
    res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ publishableKey: STRIPE_PUBLISHABLE_KEY })); return;
  }

  if (url.pathname === '/stripe/portal' && req.method === 'POST') {
    let body = '';
    req.on('data', c => { body += c.toString(); });
    req.on('end', async () => {
      try {
        const { customerId } = JSON.parse(body);
        const session = await stripeRequest('/billing_portal/sessions', 'POST', { customer: customerId, return_url: 'https://thetipsteredge.com/account.html' });
        if (!session) { res.writeHead(500, cors); res.end('Failed'); return; }
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ url: session.url }));
      } catch(e) { res.writeHead(500, cors); res.end('Server error'); }
    });
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
      // Verify user session via Authorization header
      const authHeader = req.headers['authorization'] || '';
      const token = authHeader.replace('Bearer ', '').trim();

      let isPro = false;

      if (token) {
        // Validate the JWT with Supabase and get user
        const { data: { user }, error: authErr } = await supabase.auth.getUser(token);
        if (!authErr && user) {
          const { data: profile } = await supabase
            .from('users')
            .select('subscription_status, stripe_subscription_id')
            .eq('id', user.id)
            .single();
          // Double-check against Stripe to prevent DB manipulation bypass
          if (profile?.stripe_subscription_id) {
            const sub = await stripeRequest(`/subscriptions/${profile.stripe_subscription_id}`);
            isPro = sub && (sub.status === 'active' || sub.status === 'trialing');
          }
        }
      }

      // Fetch tips from DB
      const today = new Date(); today.setHours(0,0,0,0);
      const tom = new Date(today); tom.setDate(tom.getDate() + 3);

      const { data: allTips } = await supabase
        .from('tips')
        .select('*')
        .gte('event_time', today.toISOString())
        .lte('event_time', tom.toISOString())
        .eq('status', 'pending')
        .order('confidence', { ascending: false })
        .limit(50);

      const tips = (allTips || []).map((tip, i) => {
        // Free users get first 3 tips with full data, rest are locked stubs
        const isFree = !isPro;
        const isLocked = isFree && i >= 3;
        if (isLocked) {
          // Return stub — enough to render the card but no actionable data
          return {
            tip_ref:    tip.tip_ref,
            sport:      tip.sport,
            league:     tip.league,
            home_team:  tip.home_team,
            away_team:  tip.away_team,
            event_time: tip.event_time,
            tier:       tip.tier,
            locked:     true,
          };
        }
        return { ...tip, locked: false };
      });

      res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ tips, isPro })); return;
    } catch(e) {
      console.error('Tips endpoint error:', e.message);
      res.writeHead(500, cors); res.end('Server error'); return;
    }
  }

  if (url.pathname === '/verify-pro' && req.method === 'POST') {
    let body = '';
    req.on('data', c => { body += c.toString(); });
    req.on('end', async () => {
      try {
        const { userId } = JSON.parse(body);
        if (!userId) { res.writeHead(400, cors); res.end('Missing userId'); return; }
        const { data: user } = await supabase
          .from('users')
          .select('stripe_customer_id, stripe_subscription_id, subscription_status')
          .eq('id', userId)
          .single();
        if (!user?.stripe_subscription_id) {
          res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ isPro: false })); return;
        }
        const sub = await stripeRequest(`/subscriptions/${user.stripe_subscription_id}`);
        const isPro = sub && (sub.status === 'active' || sub.status === 'trialing');
        // Correct database if out of sync
        if (!isPro && user.subscription_status === 'pro') {
          await supabase.from('users').update({ subscription_status: 'free' }).eq('id', userId);
          console.log(`🔒 Auto-corrected Pro→Free for user ${userId} (Stripe: ${sub?.status})`);
        }
        res.writeHead(200, { ...cors, 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ isPro: !!isPro }));
      } catch(e) {
        res.writeHead(500, cors);
        console.error('verify-pro error:', e.message);
        res.end(JSON.stringify({ error: 'Internal error' }));
      }
    });
    return;
  }

  res.writeHead(404); res.end('Not found');

}).listen(process.env.PORT || 3000, () => {
  console.log(`🟢 HTTP server on port ${process.env.PORT || 3000}`);
});

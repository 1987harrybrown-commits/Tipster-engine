// ============================================================
// THE TIPSTER EDGE — Engine v6
// ============================================================
// Schedule:
//   Odds fetch     → every 15 minutes
//   Results settle → every 60 minutes
//   Form data      → once daily at 06:00 UK
//   Stats cache    → recalculates on each settlement
//   Emails         → Pro 07:00, Free 08:30, Saturday 08:00 UK
//
// Football tips use a Poisson xG model (built inline below).
// NBA + NHL use stats-based scoring models (API-Sports Basketball/Hockey).
// NFL and MLB use market consensus (insufficient free API data).
// Tip IDs: sport-prefixed 10-char alphanumeric ref e.g. FB-A3X9K2
// Min confidence: 75%
// ============================================================

const { createClient } = require('@supabase/supabase-js');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
const crypto = require('crypto');

// ─── CREDENTIALS ─────────────────────────────────────────────
const SUPABASE_URL         = 'https://eyhlzzaaxrwisrtwyoyh.supabase.co';
const SUPABASE_SERVICE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5aGx6emFheHJ3aXNydHd5b3loIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzM3OTI3NywiZXhwIjoyMDg4OTU1Mjc3fQ.9Lry94K4qWWYzh0yd4zcgEaGvb8myeAzxrSHtcBSQus';
const ODDS_API_KEY         = 'cd4587438ed62cce94274935545c86a3';
const ODDS_BASE            = 'https://api.the-odds-api.com/v4';
const API_FOOTBALL_KEY     = 'f53281f35e7871081ebf83478b556b84';
const API_FOOTBALL_BASE    = 'https://v3.football.api-sports.io';
// API-Sports Basketball + Hockey use same key, separate endpoints + separate 100/day budgets
// BallDontLie: free tier for NBA/NHL injuries — sign up at app.balldontlie.io
const BDL_API_KEY          = process.env.BDL_API_KEY || ''; // add to Render env vars

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
  { key: 'americanfootball_nfl',      name: 'NFL',        league: 'NFL' },
  { key: 'baseball_mlb',              name: 'Baseball',   league: 'MLB' },
  { key: 'icehockey_nhl',             name: 'Ice Hockey', league: 'NHL' },
];

const MIN_CONFIDENCE  = 75;
const MIN_EDGE_PCT    = 5;    // minimum model edge % to tip a football market
const MATRIX_MAX_GOALS = 8;  // score matrix built 0..8 each side

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

  const { data: tips } = await supabase
    .from('tips').select('home_team, away_team, league').eq('status', 'pending').eq('sport', 'Football');

  if (!tips?.length) { formFetchedDate = today; return; }

  const pairs = new Set();
  for (const t of tips) {
    const sport = SPORTS.find(s => s.league === t.league);
    if (!sport?.leagueId) continue;
    pairs.add(`${t.home_team}||${sport.leagueId}`);
    pairs.add(`${t.away_team}||${sport.leagueId}`);
  }

  for (const entry of pairs) {
    if (formApiCalls >= MAX_FORM_CALLS) break;
    const [team, lid] = entry.split('||');
    await fetchTeamForm(team, parseInt(lid));
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
let apiBudget = {
  date:  '',
  used:  0,
  limit: 88, // 88 hard ceiling, leaves 12 for settler
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
// Fetches full season home/away goals for/against from API-Football.
// Cached per team+league+season — fetched once then reused all day.
// Guards against < 4 games played (stats unreliable early season).
const teamStatsCache = {};

async function fetchTeamSeasonStats(teamName, leagueId) {
  const season   = seasonFor(leagueId);
  const cacheKey = `stats_${teamName}_${leagueId}_${season}`;
  if (teamStatsCache[cacheKey]) return teamStatsCache[cacheKey];

  // Reuse team ID from formCache if already resolved today
  const formKey = `${teamName}_${leagueId}`;
  let teamId = formCache[formKey]?.teamId || null;

  if (!teamId) {
    if (!budgetCheck(1)) { console.log(`⚠️ Budget: skipping stats for ${teamName}`); return null; }
    try {
      const sr = await fetch(
        `${API_FOOTBALL_BASE}/teams?name=${encodeURIComponent(teamName)}&league=${leagueId}&season=${season}`,
        { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
      );
      if (!sr.ok) return null;
      const sd = await sr.json();
      teamId = sd.response?.[0]?.team?.id || null;
      if (!teamId) return null;
      // Cache the teamId for future reuse
      if (!formCache[formKey]) formCache[formKey] = {};
      formCache[formKey].teamId = teamId;
    } catch(e) { console.error(`Team ID error (${teamName}):`, e.message); return null; }
  }

  if (!budgetCheck(1)) { console.log(`⚠️ Budget: skipping stats for ${teamName}`); return null; }
  try {
    const res = await fetch(
      `${API_FOOTBALL_BASE}/teams/statistics?team=${teamId}&league=${leagueId}&season=${season}`,
      { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
    );
    if (!res.ok) return null;
    const data  = await res.json();
    const stats = data.response;
    if (!stats) return null;

    const fg = stats.goals;
    const fx = stats.fixtures;
    const hg = fx?.played?.home || 0;
    const ag = fx?.played?.away || 0;

    // Require at least 4 home and 4 away games for reliable stats
    if (hg < 4 || ag < 4) {
      console.log(`  ⚠️ ${teamName}: only ${hg}H/${ag}A games — insufficient for model`);
      return null;
    }

    const result = {
      homeScored:   fg?.for?.total?.home    || 0,
      homeConceded: fg?.against?.total?.home || 0,
      awayScored:   fg?.for?.total?.away    || 0,
      awayConceded: fg?.against?.total?.away || 0,
      homeGames:    hg,
      awayGames:    ag,
      teamId,
    };

    teamStatsCache[cacheKey] = result;
    return result;
  } catch(e) { console.error(`Team stats error (${teamName}):`, e.message); return null; }
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
  // Map to site's unit display: 0.5u, 1u, 1.5u, 2u, 2.5u, 3u
  if (sized >= 0.20) return 3.0;
  if (sized >= 0.14) return 2.5;
  if (sized >= 0.10) return 2.0;
  if (sized >= 0.07) return 1.5;
  if (sized >= 0.04) return 1.0;
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
      // Fallback: use last-5 form averages as λ proxies.
      // Less precise but usable. Confidence penalised via hasFullStats=false.
      lambdaHome = (hf.avgGoalsFor + af.avgGoalsAgainst) / 2;
      lambdaAway = (af.avgGoalsFor + hf.avgGoalsAgainst) / 2;
      console.log(`  ⚠️ Form-fallback λ: ${event.home_team} vs ${event.away_team}`);
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

    // Clamp to realistic range
    const lH = Math.max(0.25, Math.min(5.0, lambdaHome));
    const lA = Math.max(0.25, Math.min(5.0, lambdaAway));

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
          const fq   = getFormQuality(hf, af, 'home');
          const conf = confidenceFromEdge(edge, fq, hasFullStats);
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
          const fq   = getFormQuality(hf, af, 'away');
          const conf = confidenceFromEdge(edge, fq, hasFullStats);
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
      if (edge >= MIN_EDGE_PCT + 3) {
        const kelly = kellyStake(draw, market.drawOdds);
        if (kelly > 0) {
          const fq   = getFormQuality(hf, af, 'draw');
          const conf = confidenceFromEdge(edge, fq, hasFullStats);
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
          const fq   = getFormQuality(hf, af, 'over25');
          const conf = confidenceFromEdge(edge, fq, hasFullStats);
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

    // Pick highest-edge candidate — edge is the primary signal
    const pick = candidates.reduce((a, b) => a.edge >= b.edge ? a : b);

    // H2H record string for notes
    const h2hStr = (h2h.total >= 3)
      ? `${h2h.homeWins}W-${h2h.draws}D-${h2h.awayWins}L (last ${h2h.total})`
      : null;

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

// These counters are independent of the football budget counter.
// Each API-Sports API has its own 100/day limit.
let nbaBudget  = { date: '', used: 0, limit: 85 }; // NBA: ~30 teams × 2 calls
let nhlBudget  = { date: '', used: 0, limit: 90 }; // NHL: ~32 teams × 2 calls
let bdlBudget  = { date: '', used: 0, limit: 150 }; // BDL free tier is generous

function checkNBABudget(n = 1) {
  const today = new Date().toISOString().split('T')[0];
  if (nbaBudget.date !== today) { nbaBudget.date = today; nbaBudget.used = 0; }
  if (nbaBudget.used + n > nbaBudget.limit) return false;
  nbaBudget.used += n; return true;
}
function checkNHLBudget(n = 1) {
  const today = new Date().toISOString().split('T')[0];
  if (nhlBudget.date !== today) { nhlBudget.date = today; nhlBudget.used = 0; }
  if (nhlBudget.used + n > nhlBudget.limit) return false;
  nhlBudget.used += n; return true;
}
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
// Keyed by teamName_season.
// Fetches offensive rating, defensive rating, pace from API-NBA.
const nbaTeamCache = {};

async function fetchNBATeamStats(teamName) {
  const season = seasonFor();
  const cacheKey = `${teamName}_${season}`;
  if (nbaTeamCache[cacheKey]) return nbaTeamCache[cacheKey];
  if (!checkNBABudget(2)) { console.log(`⚠️ NBA budget: skipping ${teamName}`); return null; }

  try {
    // Step 1: find team ID — use permanent cache to save API calls
    let teamId = permanentTeamIds[`${teamName}_nba`] || null;
    if (!teamId) {
      const tr = await fetch(
        `${API_BASKETBALL_BASE}/teams?name=${encodeURIComponent(teamName)}&league=12&season=${season}`,
        { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
      );
      if (!tr.ok) return null;
      const td = await tr.json();
      const team = td.response?.[0];
      if (!team?.id) return null;
      teamId = team.id;
      permanentTeamIds[`${teamName}_nba`] = teamId;
    } else {
      checkNBABudget(-1); // refund the budget check — no call needed
    }

    // Step 2: team statistics for current season (league 12 = NBA)
    const sr = await fetch(
      `${API_BASKETBALL_BASE}/teams/statistics?id=${team.id}&season=${season}`,
      { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
    );
    if (!sr.ok) return null;
    const sd = await sr.json();
    const stats = sd.response?.[0];
    if (!stats) return null;

    // API-NBA returns per-game averages directly
    const games = stats.games?.played || 1;
    const result = {
      teamId:       team.id,
      gamesPlayed:  games,
      // Points scored and allowed per game
      ptsFor:       parseFloat(stats.points?.for?.average?.all   || stats.points?.for?.average?.['in'] || 110),
      ptsAgainst:   parseFloat(stats.points?.against?.average?.all || stats.points?.against?.average?.['in'] || 110),
    };

    // Require at least 8 games for reliable stats
    if (result.gamesPlayed < 8) {
      console.log(`  ⚠️ NBA ${teamName}: only ${result.gamesPlayed} games — skipping model`);
      return null;
    }

    nbaTeamCache[cacheKey] = result;
    console.log(`  🏀 NBA ${teamName}: ${result.ptsFor.toFixed(1)} pts/gm, ${result.ptsAgainst.toFixed(1)} allowed`);
    return result;
  } catch(e) { console.error(`NBA team stats error (${teamName}):`, e.message); return null; }
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
// Team ID lookups (name → API id) are cached for the process lifetime.
// This saves 1 API call per team per day — critical for NHL with 32 teams.
// Stats (the second call) still refresh daily via the keyed cache.
const permanentTeamIds = {}; // key: 'teamName_sport' → teamId

// ── NHL TEAM STATS CACHE ──────────────────────────────────────
const nhlTeamCache = {};

async function fetchNHLTeamStats(teamName) {
  const season = seasonFor();
  const cacheKey = `${teamName}_${season}`;
  if (nhlTeamCache[cacheKey]) return nhlTeamCache[cacheKey];
  if (!checkNHLBudget(2)) { console.log(`⚠️ NHL budget: skipping ${teamName}`); return null; }

  try {
    // Find team ID — use permanent cache to save API calls
    let teamId = permanentTeamIds[`${teamName}_nhl`] || null;
    if (!teamId) {
      const tr = await fetch(
        `${API_HOCKEY_BASE}/teams?name=${encodeURIComponent(teamName)}&league=57&season=${season}`,
        { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
      );
      if (!tr.ok) return null;
      const td = await tr.json();
      const team = td.response?.[0];
      if (!team?.id) return null;
      teamId = team.id;
      permanentTeamIds[`${teamName}_nhl`] = teamId;
    } else {
      checkNHLBudget(-1); // refund the budget check — no call needed
    }

    // Team statistics (league 57 = NHL)
    const sr = await fetch(
      `${API_HOCKEY_BASE}/teams/statistics?id=${team.id}&season=${season}`,
      { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
    );
    if (!sr.ok) return null;
    const sd = await sr.json();
    const stats = sd.response?.[0];
    if (!stats) return null;

    const games = stats.games?.played || 1;
    if (games < 8) {
      console.log(`  ⚠️ NHL ${teamName}: only ${games} games`);
      return null;
    }

    const result = {
      teamId:      team.id,
      gamesPlayed: games,
      goalsFor:    parseFloat(stats.goals?.for?.total?.all   || 0) / games,
      goalsAgainst: parseFloat(stats.goals?.against?.total?.all || 0) / games,
    };

    nhlTeamCache[cacheKey] = result;
    console.log(`  🏒 NHL ${teamName}: ${result.goalsFor.toFixed(2)} GF/gm, ${result.goalsAgainst.toFixed(2)} GA/gm`);
    return result;
  } catch(e) { console.error(`NHL team stats error (${teamName}):`, e.message); return null; }
}

// ── NHL GAME MODEL ────────────────────────────────────────────
// Uses Poisson model on goals scored/allowed.
// NHL home advantage ≈ 0.2 extra goals per game.
// Score matrix 0..7 (very rare to score more in NHL).
const NHL_HOME_ADVANTAGE  = 0.20;
const NHL_LEAGUE_AVG_GF   = 3.10; // goals per game per team, recent seasons
const NHL_MATRIX_MAX      = 7;

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
        const conf  = Math.min(92, Math.max(75, Math.round(75 + (edge - 5) * 1.5) - injPen));
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
        const conf  = Math.min(92, Math.max(75, Math.round(75 + (edge - 5) * 1.5) - injPen));
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
        const conf  = Math.min(90, Math.max(75, Math.round(75 + (edge - 5) * 1.2)));
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
    const pick = candidates.reduce((a, b) => a.edge >= b.edge ? a : b);

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
    tips.push(candidates.reduce((a,b) => a.confidence >= b.confidence ? a : b));
  }
  return tips;
}

// ═══════════════════════════════════════════════════════════════
// SAVE TIPS — one per game, update if improved
// ═══════════════════════════════════════════════════════════════

async function saveTips(tips) {
  if (!tips.length) return;
  let saved = 0, updated = 0, skipped = 0;

  for (const tip of tips) {
    try {
      const date = new Date(tip.event_time).toISOString().split('T')[0];
      const { data: existing } = await supabase.from('tips').select('tip_ref, confidence, odds, best_odds, bookmaker')
        .eq('home_team', tip.home_team).eq('away_team', tip.away_team)
        .gte('event_time', `${date}T00:00:00Z`).lte('event_time', `${date}T23:59:59Z`)
        .eq('status', 'pending').maybeSingle();

      if (existing) {
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
          await supabase.from('tips').update({
            odds:       tip.odds,
            best_odds:  newBest,
            bookmaker:  tip.bookmaker,
            confidence: tip.confidence,
            stake:      tip.stake,
            selection:  tip.selection,
            market:     tip.market,
            notes:      tip.notes,
          }).eq('tip_ref', existing.tip_ref);
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
  const { data: pending } = await supabase.from('tips').select('*').eq('status','pending').lt('event_time', new Date().toISOString());
  if (!pending?.length) { console.log('🏁 Nothing to settle.'); return; }
  console.log(`🏁 Settling ${pending.length} tips...`);

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
          const hs = parseFloat(match.scores.find(s => nameMatch(s.name, match.home_team))?.score || 0);
          const as2 = parseFloat(match.scores.find(s => nameMatch(s.name, match.away_team))?.score || 0);
          homeScore = hh ? hs : as2;
          awayScore = hh ? as2 : hs;
        } catch(e) { console.error(`Scores fetch error:`, e.message); continue; }

      } else if (['Basketball','Ice Hockey','NFL','Baseball'].includes(tip.sport)) {
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
        const hs = parseFloat(match.scores.find(s => nameMatch(s.name, match.home_team))?.score || 0);
        const as2 = parseFloat(match.scores.find(s => nameMatch(s.name, match.away_team))?.score || 0);
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
      if (already) {
        await supabase.from('tips').update({ status: won ? 'won' : 'lost' }).eq('tip_ref', tip.tip_ref);
        continue;
      }

      const { data: last } = await supabase.from('results_history').select('running_pl').order('settled_at', { ascending: false }).limit(1).maybeSingle();
      const runningPL = parseFloat(((last?.running_pl || 0) + pl).toFixed(2));

      await supabase.from('tips').update({ status: won ? 'won' : 'lost', profit_loss: pl, result_updated_at: new Date().toISOString() }).eq('tip_ref', tip.tip_ref);

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
        running_pl:  runningPL,
        settled_at:  new Date().toISOString(),
      });

      console.log(`${won ? '✅ WON' : '❌ LOST'}: [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team} — ${tip.selection} @ ${settlementOdds}${settlementOdds > tip.odds ? ' (best: '+settlementOdds+' vs current: '+tip.odds+')' : ''} (${pl >= 0 ? '+' : ''}${pl}u)`);
      count++;
      await updateStatsCache();

    } catch(e) { console.error(`Settle error [${tip.tip_ref}]:`, e.message); }
  }
  console.log(`🏁 Settled ${count} tips.`);
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
  console.log(`\n🚀 Engine v6 — ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`);
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

function unsubLink(userId) { return `${SITE_URL}/unsubscribe?token=${Buffer.from(userId).toString('base64')}`; }

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
      if (u) await sendEmail({ to: u.email, subject: 'Welcome to The Tipster Pro', html: emailBase(`
<p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#f0b429;margin:0 0 10px;">Pro Member</p>
<h1 style="font-size:22px;font-weight:800;color:#dde6f0;margin:0 0 8px;">Welcome to Pro, ${u.first_name||'there'}.</h1>
<p style="font-size:13px;color:#4a5a70;line-height:1.7;margin:0 0 20px;">Your account is now active. Tomorrow at 07:00 you'll receive the full card — every tip, every value edge, every stake recommendation.</p>
<div style="text-align:center;"><a href="https://thetipsteredge.com" style="display:inline-block;background:#f0b429;color:#07090d;font-size:13px;font-weight:700;padding:12px 28px;border-radius:5px;text-decoration:none;">View Today's Tips</a></div>`, uid), type: 'welcome_pro' });
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

    if (h === 6  && m === 0  && lastFormDate2 !== today) { lastFormDate2 = today; await fetchAllFormData(); }
    if (h === 7  && m === 0  && lastProDate   !== today) { lastProDate   = today; await sendProEmails();    }
    if (h === 8  && m === 30 && lastFreeDate  !== today) { lastFreeDate  = today; await sendDailyEmails(); }
    if (isSat && h === 8 && m === 0 && lastSatDate !== today) { lastSatDate = today; await sendSaturdayEmails(); }
  }, 60 * 1000);

  setInterval(settleResults, 60 * 60 * 1000);

  console.log('⏰ Scheduler active:');
  console.log('   06:00 UK — Form data fetch');
  console.log('   07:00 UK — Pro emails');
  console.log('   08:30 UK — Free emails');
  console.log('   Every 60 min — Settler');
  console.log('   Every 15 min — Odds + tips');
}

// ═══════════════════════════════════════════════════════════════
// STARTUP
// ═══════════════════════════════════════════════════════════════

(async () => {
  console.log(`\n🟢 The Tipster Engine v6 starting... Season: ${seasonFor()}`);
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
http.createServer(async (req, res) => {
  const url  = new URL(req.url, 'http://localhost');
  const cors = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' };

  if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return; }

  if (url.pathname === '/') {
    res.writeHead(200, { ...cors, 'Content-Type': 'text/plain' });
    res.end(`The Tipster Engine v6 | Season: ${seasonFor()}`); return;
  }

  const adminKey = url.searchParams.get('key');

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
    if (!token) { res.writeHead(400); res.end('Invalid'); return; }
    try {
      await supabase.from('users').update({ email_opt_in: false }).eq('id', Buffer.from(token, 'base64').toString('utf8'));
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end('<html><body style="font-family:sans-serif;text-align:center;padding:60px;background:#07090d;color:#dde6f0;"><h2>Unsubscribed</h2><p>You have been removed from all emails.</p><a href="https://www.thetipsteredge.com/account.html" style="color:#18e07a;">Manage preferences</a></body></html>');
    } catch(e) { res.writeHead(400); res.end('Invalid token'); }
    return;
  }

  res.writeHead(404); res.end('Not found');

}).listen(process.env.PORT || 3000, () => {
  console.log(`🟢 HTTP server on port ${process.env.PORT || 3000}`);
});

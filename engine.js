// ============================================================
// THE TIPSTER EDGE — Engine v4
// ============================================================
// Schedule:
//   Odds fetch     → every 15 minutes
//   Results settle → every 60 minutes
//   Form data      → once daily at 06:00 UK
//   Stats cache    → recalculates on each settlement
//   Emails         → Pro 07:00, Free 08:30, Saturday 08:00 UK
//
// Football tips use a Poisson xG model (built inline below).
// All other sports use market-consensus logic unchanged.
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
const API_FOOTBALL_KEY     = 'ac0de64e0ea27e1809738361212c003e';
const API_FOOTBALL_BASE    = 'https://v3.football.api-sports.io';

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
// [NEW] FOOTBALL POISSON MODEL
// Inline — no separate module.
//
// Sections:
//   A. League averages (centralised, with dynamic fallback)
//   B. Team season stats cache + fetcher
//   C. Poisson maths (0..MATRIX_MAX_GOALS, renormalised)
//   D. Market extraction from bookmaker odds
//   E. Confidence from edge (explicit bands, no modulo)
//   F. Market-aware form adjustment
//   G. Market-aware notes builder
//   H. Main entry: analyseFootballFixture()
// ═══════════════════════════════════════════════════════════════

// ── A. LEAGUE AVERAGES ────────────────────────────────────────
// These are long-run seasonal averages (2020–2024).
// Used as the denominator in attack/defence strength calculation.
// Centralised here — update once per season if needed.
// Dynamic recalculation from API data is possible but would cost
// ~6 extra API calls per season start; static values are accurate
// enough for the denominator and are reviewed annually.
const LEAGUE_AVERAGES = {
  39:  { homeGoals: 1.53, awayGoals: 1.21 }, // Premier League
  140: { homeGoals: 1.57, awayGoals: 1.14 }, // La Liga
  78:  { homeGoals: 1.68, awayGoals: 1.25 }, // Bundesliga
  135: { homeGoals: 1.49, awayGoals: 1.12 }, // Serie A
  61:  { homeGoals: 1.51, awayGoals: 1.18 }, // Ligue 1
  2:   { homeGoals: 1.64, awayGoals: 1.22 }, // Champions League
};

// Fallback if leagueId not in table — use pan-European average
const LEAGUE_AVG_FALLBACK = { homeGoals: 1.55, awayGoals: 1.18 };

function getLeagueAvg(leagueId) {
  return LEAGUE_AVERAGES[leagueId] || LEAGUE_AVG_FALLBACK;
}

// ── B. TEAM SEASON STATS CACHE ────────────────────────────────
// Keyed by teamId_leagueId_season.
// Populated on first use per engine run (lazy, not pre-fetched).
// Budget: 1 call per team per day (uses team ID from formCache if available).
const teamStatsCache = {};
let teamStatsApiCalls = 0;
const MAX_TEAM_STATS_CALLS = 30; // daily budget for stats calls

// [CHANGED] Uses seasonFor() — no hardcoded season
async function fetchTeamSeasonStats(teamName, leagueId) {
  const season    = seasonFor(leagueId);
  const cacheKey  = `${teamName}_${leagueId}_${season}`;
  if (teamStatsCache[cacheKey]) return teamStatsCache[cacheKey];
  if (teamStatsApiCalls >= MAX_TEAM_STATS_CALLS) return null;

  try {
    // Reuse team ID from formCache if already fetched this session
    const formKey  = `${teamName}_${leagueId}`;
    let teamId     = formCache[formKey]?.teamId || null;

    if (!teamId) {
      teamStatsApiCalls++;
      const sr = await fetch(
        `${API_FOOTBALL_BASE}/teams?name=${encodeURIComponent(teamName)}&league=${leagueId}&season=${season}`,
        { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
      );
      if (!sr.ok) return null;
      const sd = await sr.json();
      teamId = sd.response?.[0]?.team?.id || null;
      if (!teamId) return null;
    }

    teamStatsApiCalls++;
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

    // Guard: if team has played < 3 home or away games, stats are unreliable
    const hg = fx?.played?.home || 0;
    const ag = fx?.played?.away || 0;
    if (hg < 3 || ag < 3) {
      console.log(`  ⚠️ Insufficient games for ${teamName} (H:${hg} A:${ag}) — skipping model`);
      return null;
    }

    const result = {
      homeScored:   fg?.for?.total?.home    || 0,
      homeConceded: fg?.against?.total?.home || 0,
      awayScored:   fg?.for?.total?.away    || 0,
      awayConceded: fg?.against?.total?.away || 0,
      homeGames:    hg,
      awayGames:    ag,
    };

    teamStatsCache[cacheKey] = result;
    return result;
  } catch(e) {
    console.error(`Team stats error (${teamName}):`, e.message);
    return null;
  }
}

// ── C. POISSON MATHS ─────────────────────────────────────────
// P(X=k) = e^-λ * λ^k / k!
function poisson(lambda, k) {
  if (lambda <= 0) return k === 0 ? 1 : 0;
  let logP = -lambda + k * Math.log(lambda);
  for (let i = 1; i <= k; i++) logP -= Math.log(i);
  return Math.exp(logP);
}

// Build score probability matrix P(home=i, away=j) for i,j in 0..MATRIX_MAX_GOALS
// Then renormalise so total probability mass sums to 1.0 within the matrix.
// This corrects for truncation at MATRIX_MAX_GOALS without losing accuracy.
function buildScoreMatrix(lambdaHome, lambdaAway) {
  const N = MATRIX_MAX_GOALS;
  const matrix = [];
  let total = 0;

  for (let h = 0; h <= N; h++) {
    matrix[h] = [];
    for (let a = 0; a <= N; a++) {
      const p = poisson(lambdaHome, h) * poisson(lambdaAway, a);
      matrix[h][a] = p;
      total += p;
    }
  }

  // Renormalise: divide each cell by total mass captured in matrix.
  // For λ ≤ 4, truncating at 8 captures >99.9% of mass; renormalisation
  // corrects the remaining <0.1% so probabilities sum to exactly 1.
  if (total > 0 && total < 1) {
    for (let h = 0; h <= N; h++)
      for (let a = 0; a <= N; a++)
        matrix[h][a] /= total;
  }

  return matrix;
}

// Derive outcome probabilities from renormalised matrix
function calcOutcomes(matrix) {
  const N = MATRIX_MAX_GOALS;
  let homeWin = 0, draw = 0, awayWin = 0, over25 = 0, btts = 0;

  for (let h = 0; h <= N; h++) {
    for (let a = 0; a <= N; a++) {
      const p = matrix[h][a];
      if (h > a)        homeWin += p;
      else if (h === a) draw    += p;
      else              awayWin += p;
      if (h + a > 2.5)  over25  += p;
      if (h > 0 && a > 0) btts  += p;
    }
  }

  return { homeWin, draw, awayWin, over25, btts };
}

// Fair decimal odds from probability (no margin)
function fairOdds(prob) {
  if (prob <= 0.001) return 999.0;
  return parseFloat((1 / prob).toFixed(2));
}

// ── D. MARKET EXTRACTION ──────────────────────────────────────
// Gets best available price for each outcome from event.bookmakers.
// Separate from model — purely what the market offers.
function extractBestOdds(event) {
  const best = {
    home: 0, draw: 0, away: 0, over25: 0,
    homeBook: '', drawBook: '', awayBook: '', over25Book: '',
  };

  for (const book of (event.bookmakers || [])) {
    const h2h = book.markets?.find(m => m.key === 'h2h');
    if (h2h) {
      for (const o of h2h.outcomes) {
        if (nameMatch(o.name, event.home_team) && o.price > best.home) {
          best.home = o.price; best.homeBook = book.title;
        } else if (nameMatch(o.name, event.away_team) && o.price > best.away) {
          best.away = o.price; best.awayBook = book.title;
        } else if (o.name === 'Draw' && o.price > best.draw) {
          best.draw = o.price; best.drawBook = book.title;
        }
      }
    }
    const totals = book.markets?.find(m => m.key === 'totals');
    if (totals) {
      for (const o of totals.outcomes) {
        if (o.name === 'Over' && Math.abs((o.point || 0) - 2.5) < 0.01 && o.price > best.over25) {
          best.over25 = o.price; best.over25Book = book.title;
        }
      }
    }
  }

  return best;
}

// ── E. CONFIDENCE FROM EDGE ───────────────────────────────────
// [CHANGED] Explicit edge bands — no modulo, no circular logic.
// Edge is the primary driver. Form provides a ±5 secondary adjustment.
// Bands:
//   5% to <7%  → base 75  (borderline value — tip if form supports)
//   7% to <10% → base 79  (clear value)
//   10% to <14%→ base 83  (strong value)
//   14% to <18%→ base 87  (high-conviction value)
//   18%+       → base 91  (very high edge — rare, treat with care)
// Within each band, confidence scales linearly to the next band floor.
// Final confidence is clamped 75–95.
function confidenceFromEdge(edgePct, formAdj) {
  let base, bandWidth, bandFloor;

  if (edgePct >= 18) {
    base = 91; bandWidth = 4; bandFloor = 18;
  } else if (edgePct >= 14) {
    base = 87; bandWidth = 4; bandFloor = 14;
  } else if (edgePct >= 10) {
    base = 83; bandWidth = 4; bandFloor = 10;
  } else if (edgePct >= 7) {
    base = 79; bandWidth = 3; bandFloor = 7;
  } else {
    base = 75; bandWidth = 2; bandFloor = 5;
  }

  // Linear interpolation within band
  const progress = Math.min(1, (edgePct - bandFloor) / bandWidth);
  const scaled   = base + Math.round(progress * (bandWidth - 1));

  return Math.min(95, Math.max(75, scaled + formAdj));
}

// ── F. MARKET-AWARE FORM ADJUSTMENT ──────────────────────────
// [CHANGED] Form adjustment only applied where it is meaningful.
// - Home win / Away win: tip-team form vs opposition form
// - Draw: small symmetric form adjustment based on how evenly matched
//   recent form is (high volatility = slightly more draw-probable).
//   Capped at ±2 so it cannot swing the confidence band.
// - Over 2.5: based on combined scoring rate, not team-sided
// - Under 2.5: based on combined defensive rate
// Returns an integer adjustment in range -5 to +5.
function footballFormAdj(baseConf, homeForm, awayForm, { isHome, isDraw, isOver, isUnder }) {
  if (!homeForm && !awayForm) return baseConf;

  let adj = 0;

  if (isDraw) {
    // Draw form adjustment: if both teams are in similar mid-table form
    // (neither dominant nor collapsed), draws are slightly more likely.
    // We don't boost confidence just because a team is in bad form —
    // that's already baked into the model probabilities.
    // Cap: ±2 only.
    if (homeForm && awayForm) {
      const formDiff = Math.abs(homeForm.formScore - awayForm.formScore);
      if (formDiff < 0.15) adj = +1;  // evenly matched — marginal draw boost
      if (formDiff > 0.40) adj = -1;  // clear disparity — draw less likely
    }
    return Math.min(95, Math.max(75, baseConf + adj));
  }

  if (isOver || isUnder) {
    // Totals form adjustment: driven by combined scoring profile
    const hAvg = homeForm?.avgGoalsFor  || 0;
    const aAvg = awayForm?.avgGoalsFor  || 0;
    const hDef = homeForm?.avgGoalsAgainst || 0;
    const aDef = awayForm?.avgGoalsAgainst || 0;
    // Use average goals expected in the game (attack avg + defence conceded avg)
    const expectedGoals = ((hAvg + aDef) + (aAvg + hDef)) / 2;
    if (isOver) {
      if (expectedGoals > 2.8) adj = +3;
      else if (expectedGoals > 2.3) adj = +1;
      else if (expectedGoals < 1.8) adj = -3;
      else if (expectedGoals < 2.1) adj = -1;
    } else { // Under
      if (expectedGoals < 1.8) adj = +3;
      else if (expectedGoals < 2.1) adj = +1;
      else if (expectedGoals > 2.8) adj = -3;
      else if (expectedGoals > 2.3) adj = -1;
    }
    return Math.min(95, Math.max(75, baseConf + adj));
  }

  // Home win or Away win — team-sided form adjustment
  const tipForm = isHome ? homeForm : awayForm;
  const oppForm = isHome ? awayForm : homeForm;

  if (!tipForm) return baseConf;

  // Form score: 0=all losses, 1=all wins. 0.5 = average.
  const formAdj = Math.round((tipForm.formScore - 0.5) * 10); // -5 to +5

  // Goals scored by tip team
  let goalsAdj = 0;
  if (tipForm.avgGoalsFor      > 2.0) goalsAdj += 2;
  else if (tipForm.avgGoalsFor > 1.5) goalsAdj += 1;
  else if (tipForm.avgGoalsFor < 0.8) goalsAdj -= 2;
  else if (tipForm.avgGoalsFor < 1.2) goalsAdj -= 1;

  // Goals conceded by tip team
  if (tipForm.avgGoalsAgainst  < 0.8) goalsAdj += 1;
  else if (tipForm.avgGoalsAgainst > 2.0) goalsAdj -= 1;

  // Opposition form penalty/boost
  let oppAdj = 0;
  if (oppForm) {
    if (oppForm.formScore < 0.25) oppAdj = +1;  // opponent in collapse
    else if (oppForm.formScore > 0.75) oppAdj = -1; // opponent in top form
  }

  adj = Math.max(-5, Math.min(5, formAdj + goalsAdj + oppAdj));
  return Math.min(95, Math.max(75, baseConf + adj));
}

// ── G. MARKET-AWARE NOTES BUILDER ────────────────────────────
// [CHANGED] Notes are now market-specific — "win prob" only for win bets.
// ── G. FOOTBALL NOTES BUILDER ────────────────────────────────
// Generates structured model diagnostics for football tips only.
// Format: xG: H vs A | Fair odds: X.XX | Book: X.XX (Bookie) | Edge: +X.X% | Model: XX.X% <market> probability
// Location: called from analyseFootballFixture() just before the tip object is returned.
// Non-football notes are handled separately in analyseH2H / analyseTotals — unchanged.
// Fallback: if lambdaHome/lambdaAway are undefined (e.g. stats fetch failed), returns generic text safely.
function buildFootballNotes({ market, modelProb, fairPrice, bookOdds, bookmaker, edgePct, lambdaHome, lambdaAway }) {
  // Safety fallback — if model data is missing return generic text
  if (lambdaHome == null || lambdaAway == null || modelProb == null) {
    return `Book: ${bookOdds} (${bookmaker || 'Multiple'}) | Edge: +${(edgePct || 0).toFixed(1)}%`;
  }

  // Market-specific probability suffix — matches required format exactly
  const probSuffix = {
    home:   'home win probability',
    away:   'away win probability',
    draw:   'draw probability',
    over25: 'over 2.5 probability',
  }[market] || 'win probability';

  // Required field order: xG | Fair odds | Book | Edge | Model
  return [
    `xG: ${lambdaHome.toFixed(2)} vs ${lambdaAway.toFixed(2)}`,
    `Fair odds: ${fairPrice}`,
    `Book: ${bookOdds} (${bookmaker})`,
    `Edge: +${edgePct.toFixed(1)}%`,
    `Model: ${(modelProb * 100).toFixed(1)}% ${probSuffix}`,
  ].join(' | ');
}

// Appends form strings to existing notes (used by applyFormToPendingTips)
function appendFormToNotes(hf, af, existingNotes = '') {
  // Don't double-append if already present
  if (existingNotes.includes('Home form:')) return existingNotes;
  const extras = [];
  if (hf) extras.push(`Home form: ${hf.formString}`);
  if (af) extras.push(`Away form: ${af.formString}`);
  return extras.length ? existingNotes + ' | ' + extras.join(' | ') : existingNotes;
}

// ── H. MAIN FOOTBALL FIXTURE ANALYSER ────────────────────────
// Called once per fixture in generateTips() for football only.
// Returns a tip object or null.
// [CHANGED] Dynamic season, 0-8 matrix, edge-based confidence,
// market-aware form adj, market-aware notes.
async function analyseFootballFixture(event, sport) {
  try {
    const leagueId = sport.leagueId;
    if (!leagueId) return null;

    const leagueAvg = getLeagueAvg(leagueId);
    const { homeGoals: lgHome, awayGoals: lgAway } = leagueAvg;

    // Fetch season stats for both teams
    const [homeStats, awayStats] = await Promise.all([
      fetchTeamSeasonStats(event.home_team, leagueId),
      fetchTeamSeasonStats(event.away_team, leagueId),
    ]);

    // Fall back to form cache if season stats unavailable
    // (e.g. early season with < 3 games played)
    const hf = formCache[`${event.home_team}_${leagueId}`] || null;
    const af = formCache[`${event.away_team}_${leagueId}`] || null;

    let lambdaHome, lambdaAway;

    if (homeStats && awayStats) {
      // ── Attack / Defence strengths from season stats ────
      const homeAvgScored   = homeStats.homeScored   / homeStats.homeGames;
      const homeAvgConceded = homeStats.homeConceded / homeStats.homeGames;
      const awayAvgScored   = awayStats.awayScored   / awayStats.awayGames;
      const awayAvgConceded = awayStats.awayConceded / awayStats.awayGames;

      const homeAttack = homeAvgScored   / lgHome;
      const homeDef    = homeAvgConceded / lgAway;
      const awayAttack = awayAvgScored   / lgAway;
      const awayDef    = awayAvgConceded / lgHome;

      lambdaHome = homeAttack * awayDef   * lgHome;
      lambdaAway = awayAttack * homeDef   * lgAway;
    } else if (hf && af) {
      // Fallback: use form data averages as λ proxies
      // Less accurate than season stats but better than refusing to tip
      lambdaHome = (hf.avgGoalsFor  + af.avgGoalsAgainst) / 2;
      lambdaAway = (af.avgGoalsFor  + hf.avgGoalsAgainst) / 2;
      console.log(`  ⚠️ Using form-data fallback λ for ${event.home_team} vs ${event.away_team}`);
    } else {
      // No data — cannot price this fixture
      return null;
    }

    // Clamp λ to realistic range (prevents extreme outlier fixtures)
    const lH = Math.max(0.30, Math.min(4.50, lambdaHome));
    const lA = Math.max(0.30, Math.min(4.50, lambdaAway));

    // Build score matrix (0..8 each side, renormalised)
    const matrix = buildScoreMatrix(lH, lA);
    const { homeWin, draw, awayWin, over25 } = calcOutcomes(matrix);

    // Fair odds
    const fairHome  = fairOdds(homeWin);
    const fairDraw  = fairOdds(draw);
    const fairAway  = fairOdds(awayWin);
    const fair25    = fairOdds(over25);

    // Best bookmaker prices
    const best = extractBestOdds(event);

    // ── Evaluate each market for edge ──────────────────────
    const candidates = [];

    // Home win
    if (best.home >= 1.30 && best.home <= 6.0) {
      const edgePct = (homeWin - 1/best.home) * 100;
      if (edgePct >= MIN_EDGE_PCT) {
        const formAdj = footballFormAdj(0, hf, af, { isHome: true, isDraw: false, isOver: false, isUnder: false });
        const conf    = confidenceFromEdge(edgePct, formAdj);
        if (conf >= MIN_CONFIDENCE) candidates.push({
          market: 'home', edgePct, modelProb: homeWin, fairPrice: fairHome,
          bookOdds: best.home, bookmaker: best.homeBook,
          selection: `${event.home_team} Win`, conf, formAdj,
        });
      }
    }

    // Away win
    if (best.away >= 1.30 && best.away <= 6.0) {
      const edgePct = (awayWin - 1/best.away) * 100;
      if (edgePct >= MIN_EDGE_PCT) {
        const formAdj = footballFormAdj(0, hf, af, { isHome: false, isDraw: false, isOver: false, isUnder: false });
        const conf    = confidenceFromEdge(edgePct, formAdj);
        if (conf >= MIN_CONFIDENCE) candidates.push({
          market: 'away', edgePct, modelProb: awayWin, fairPrice: fairAway,
          bookOdds: best.away, bookmaker: best.awayBook,
          selection: `${event.away_team} Win`, conf, formAdj,
        });
      }
    }

    // Draw — require higher threshold: draws are noisy and margin-sensitive
    if (best.draw >= 2.60 && draw > 0.24) {
      const edgePct = (draw - 1/best.draw) * 100;
      if (edgePct >= MIN_EDGE_PCT + 3) { // +3% extra hurdle for draws
        const formAdj = footballFormAdj(0, hf, af, { isHome: false, isDraw: true, isOver: false, isUnder: false });
        const conf    = confidenceFromEdge(edgePct, formAdj);
        if (conf >= MIN_CONFIDENCE) candidates.push({
          market: 'draw', edgePct, modelProb: draw, fairPrice: fairDraw,
          bookOdds: best.draw, bookmaker: best.drawBook,
          selection: 'Draw', conf, formAdj,
        });
      }
    }

    // Over 2.5
    if (best.over25 >= 1.45 && best.over25 <= 2.40) {
      const edgePct = (over25 - 1/best.over25) * 100;
      if (edgePct >= MIN_EDGE_PCT) {
        const formAdj = footballFormAdj(0, hf, af, { isHome: false, isDraw: false, isOver: true, isUnder: false });
        const conf    = confidenceFromEdge(edgePct, formAdj);
        if (conf >= MIN_CONFIDENCE) candidates.push({
          market: 'over25', edgePct, modelProb: over25, fairPrice: fair25,
          bookOdds: best.over25, bookmaker: best.over25Book,
          selection: 'Over 2.5', conf, formAdj,
        });
      }
    }

    if (!candidates.length) return null;

    // Pick highest-edge candidate (not highest confidence — edge is the signal)
    const pick = candidates.reduce((a, b) => a.edgePct >= b.edgePct ? a : b);

    // Build tip object
    const stake = pick.conf >= 90 ? 3.0 : pick.conf >= 82 ? 2.0 : 1.5;
    // Build structured model notes (football only).
    // Format: xG: H vs A | Fair odds: X.XX | Book: X.XX (Bookie) | Edge: +X.X% | Model: XX.X% <market> probability
    const notes = buildFootballNotes({
      market:     pick.market,
      modelProb:  pick.modelProb,
      fairPrice:  pick.fairPrice,
      bookOdds:   pick.bookOdds,
      bookmaker:  pick.bookmaker,
      edgePct:    pick.edgePct,
      lambdaHome: lH,
      lambdaAway: lA,
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
      stake,
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
      // ── Football: Poisson xG model ────────────────────────
      const tip = await analyseFootballFixture(event, sport);
      if (tip) candidates.push(tip);
    } else {
      // ── Non-football: market consensus (unchanged) ────────
      const h2h    = event.bookmakers.map(b => b.markets?.find(m => m.key === 'h2h')).filter(Boolean);
      const totals = event.bookmakers.map(b => b.markets?.find(m => m.key === 'totals')).filter(Boolean);
      if (h2h.length    >= 2) { const t = analyseH2H(event, h2h, sport);    if (t) candidates.push(t); }
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
      const { data: existing } = await supabase.from('tips').select('tip_ref, confidence, odds')
        .eq('home_team', tip.home_team).eq('away_team', tip.away_team)
        .gte('event_time', `${date}T00:00:00Z`).lte('event_time', `${date}T23:59:59Z`)
        .eq('status', 'pending').maybeSingle();

      if (existing) {
        if (tip.confidence > existing.confidence + 2 || tip.odds > existing.odds + 0.05) {
          await supabase.from('tips').update({
            selection: tip.selection, market: tip.market, odds: tip.odds,
            confidence: tip.confidence, stake: tip.stake, bookmaker: tip.bookmaker, notes: tip.notes,
          }).eq('tip_ref', existing.tip_ref);
          updated++;
        } else { skipped++; }
        continue;
      }

      const { error } = await supabase.from('tips').insert(tip);
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

const fixtureCache = {}, fixtureCacheTime = {};

async function fetchFootballFixtures(leagueId) {
  const now = Date.now();
  if (fixtureCache[leagueId] && now - fixtureCacheTime[leagueId] < 2 * 3600000) return fixtureCache[leagueId];
  try {
    const season = seasonFor(leagueId);
    const from   = new Date(now - 3 * 86400000).toISOString().split('T')[0];
    const to     = new Date().toISOString().split('T')[0];
    const res    = await fetch(
      `${API_FOOTBALL_BASE}/fixtures?league=${leagueId}&season=${season}&from=${from}&to=${to}&status=FT`,
      { headers: { 'x-apisports-key': API_FOOTBALL_KEY } }
    );
    if (!res.ok) return [];
    const data = await res.json();
    if (data.errors && Object.keys(data.errors).length) return [];
    fixtureCache[leagueId] = data.response || [];
    fixtureCacheTime[leagueId] = now;
    return fixtureCache[leagueId];
  } catch(e) { console.error('Fixture fetch error:', e.message); return []; }
}

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
        const sport = SPORTS.find(s => s.league === tip.league);
        if (!sport?.leagueId) continue;
        const fixtures = await fetchFootballFixtures(sport.leagueId);
        const f = fixtures.find(f => {
          const h = f.teams?.home?.name || '', a = f.teams?.away?.name || '';
          return (nameMatch(h, tip.home_team) && nameMatch(a, tip.away_team)) ||
                 (nameMatch(h, tip.away_team) && nameMatch(a, tip.home_team));
        });
        if (!f) { console.log(`⏳ No result: ${tip.home_team} vs ${tip.away_team}`); continue; }
        const hh = nameMatch(f.teams.home.name, tip.home_team);
        homeScore = hh ? f.goals.home : f.goals.away;
        awayScore = hh ? f.goals.away : f.goals.home;

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

      const pl = won ? parseFloat(((tip.odds - 1) * tip.stake).toFixed(2)) : parseFloat((-tip.stake).toFixed(2));

      const { data: already } = await supabase.from('results_history').select('id').eq('tip_ref', tip.tip_ref).maybeSingle();
      if (already) {
        await supabase.from('tips').update({ status: won ? 'won' : 'lost' }).eq('tip_ref', tip.tip_ref);
        continue;
      }

      const { data: last } = await supabase.from('results_history').select('running_pl').order('settled_at', { ascending: false }).limit(1).maybeSingle();
      const runningPL = parseFloat(((last?.running_pl || 0) + pl).toFixed(2));

      await supabase.from('tips').update({ status: won ? 'won' : 'lost', profit_loss: pl, result_updated_at: new Date().toISOString() }).eq('tip_ref', tip.tip_ref);

      await supabase.from('results_history').insert({
        tip_ref: tip.tip_ref, sport: tip.sport,
        event: `${tip.home_team} vs ${tip.away_team}`,
        selection: tip.selection, odds: tip.odds, stake: tip.stake,
        tier: tip.tier || 'pro', result: won ? 'WON' : 'LOST',
        profit_loss: pl, running_pl: runningPL, settled_at: new Date().toISOString(),
      });

      console.log(`${won ? '✅ WON' : '❌ LOST'}: [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team} — ${tip.selection} @ ${tip.odds} (${pl >= 0 ? '+' : ''}${pl}u)`);
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
  console.log(`\n🚀 Engine v4 — ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`);
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
  console.log(`\n🟢 The Tipster Engine v4 starting... Season: ${seasonFor()}`);
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
    res.end(`The Tipster Engine v4 | Season: ${seasonFor()}`); return;
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

// ================================================================
// FOOTBALL LAB v2 — Proper Dixon-Coles model using real team stats
// ================================================================
// Uses teamStatsCache from main engine — zero extra API calls.
// Only logs fixtures where both teams have season stats.
// Minimum edge filters applied to keep data meaningful.
// ================================================================

// ── LEAGUE AVERAGES (mirrors engine.js) ─────────────────────
const LAB_LEAGUE_AVERAGES = {
  39:  { homeGoals: 1.53, awayGoals: 1.21 }, // Premier League
  140: { homeGoals: 1.57, awayGoals: 1.14 }, // La Liga
  78:  { homeGoals: 1.68, awayGoals: 1.25 }, // Bundesliga
  135: { homeGoals: 1.49, awayGoals: 1.12 }, // Serie A
  61:  { homeGoals: 1.51, awayGoals: 1.18 }, // Ligue 1
  2:   { homeGoals: 1.64, awayGoals: 1.22 }, // Champions League
};
const LAB_AVG_FALLBACK = { homeGoals: 1.55, awayGoals: 1.18 };

// Minimum edge to log — below this = no meaningful value
const MIN_H2H_EDGE   = 3;  // %
const MIN_TOTAL_EDGE = 5;  // %

// ── HELPERS ──────────────────────────────────────────────────

function labPoisson(lambda, k) {
  if (lambda <= 0) return k === 0 ? 1 : 0;
  let logP = -lambda + k * Math.log(lambda);
  for (let i = 1; i <= k; i++) logP -= Math.log(i);
  return Math.exp(logP);
}

function labBuildMatrix(lH, lA) {
  const N = 8, RHO = 0.10;
  const matrix = [];
  let total = 0;
  for (let h = 0; h <= N; h++) {
    matrix[h] = [];
    for (let a = 0; a <= N; a++) {
      const raw = labPoisson(lH, h) * labPoisson(lA, a);
      let tau = 1;
      if      (h === 0 && a === 0) tau = 1 - lH * lA * RHO;
      else if (h === 1 && a === 0) tau = 1 + lA * RHO;
      else if (h === 0 && a === 1) tau = 1 + lH * RHO;
      else if (h === 1 && a === 1) tau = 1 - RHO;
      matrix[h][a] = Math.max(0, raw * tau);
      total += matrix[h][a];
    }
  }
  if (total > 0)
    for (let h = 0; h <= N; h++)
      for (let a = 0; a <= N; a++)
        matrix[h][a] /= total;
  return matrix;
}

function labCalcOutcomes(matrix) {
  const N = 8;
  let homeWin = 0, draw = 0, awayWin = 0;
  let over25 = 0, over35 = 0, under25 = 0, under35 = 0;
  for (let h = 0; h <= N; h++) {
    for (let a = 0; a <= N; a++) {
      const p = matrix[h][a];
      if (h > a)        homeWin += p;
      else if (h === a) draw    += p;
      else              awayWin += p;
      if (h + a > 2.5)  over25  += p;
      if (h + a > 3.5)  over35  += p;
      if (h + a < 2.5)  under25 += p;
      if (h + a < 3.5)  under35 += p;
    }
  }
  return { homeWin, draw, awayWin, over25, over35, under25, under35 };
}

function stripMargin(homeOdds, drawOdds, awayOdds) {
  const total = (1 / homeOdds) + (1 / drawOdds) + (1 / awayOdds);
  return {
    trueHome: (1 / homeOdds) / total,
    trueDraw: (1 / drawOdds) / total,
    trueAway: (1 / awayOdds) / total,
  };
}

function nameMatch(a, b) {
  if (!a || !b) return false;
  const clean = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  const ac = clean(a), bc = clean(b);
  return ac === bc || ac.includes(bc) || bc.includes(ac);
}

// ── LAMBDA CALCULATION ────────────────────────────────────────
// Uses real team season stats from the shared cache.
// Returns null if either team has insufficient data.
function calcLambdas(homeTeam, awayTeam, leagueId, season, teamStatsCache) {
  const leagueAvg = LAB_LEAGUE_AVERAGES[leagueId] || LAB_AVG_FALLBACK;
  const { homeGoals: lgHome, awayGoals: lgAway } = leagueAvg;

  const homeKey = `stats_${homeTeam}_${leagueId}_${season}`;
  const awayKey = `stats_${awayTeam}_${leagueId}_${season}`;

  const homeStats = teamStatsCache[homeKey];
  const awayStats = teamStatsCache[awayKey];

  // Require real stats for both teams — no flat fallback
  if (!homeStats || !awayStats) return null;
  if (homeStats.homeGames < 4 || awayStats.awayGames < 4) return null;

  // Attack/defence strengths (same formula as main engine)
  const homeAvgScoredH = homeStats.homeScored   / homeStats.homeGames;
  const homeAvgConcH   = homeStats.homeConceded  / homeStats.homeGames;
  const awayAvgScoredA = awayStats.awayScored    / awayStats.awayGames;
  const awayAvgConcA   = awayStats.awayConceded  / awayStats.awayGames;

  const homeAttack = homeAvgScoredH / lgHome;
  const homeDef    = homeAvgConcH   / lgAway;
  const awayAttack = awayAvgScoredA / lgAway;
  const awayDef    = awayAvgConcA   / lgHome;

  const lH = Math.max(0.25, Math.min(5.0, homeAttack * awayDef * lgHome));
  const lA = Math.max(0.25, Math.min(5.0, awayAttack * homeDef * lgAway));

  return { lH, lA };
}

// ── MAIN LAB FUNCTION ─────────────────────────────────────────

async function runFootballLab(supabase, events, sport, teamStatsCache, season) {
  if (!events?.length || sport.name !== 'Football') return;
  if (!sport.leagueId) return;

  let inserted = 0, skipped = 0, noStats = 0;

  for (const event of events) {
    try {
      const hours = (new Date(event.commence_time) - new Date()) / 3600000;
      if (hours < 0 || hours > 72) continue;
      if (!event.bookmakers?.length) continue;

      const fixtureId = event.id;

      // Skip if already logged
      const { data: existing } = await supabase
        .from('football_lab')
        .select('id')
        .eq('fixture_id', fixtureId)
        .maybeSingle();
      if (existing) { skipped++; continue; }

      // ── Calculate λ using real team stats ────────────────
      const lambdas = calcLambdas(
        event.home_team,
        event.away_team,
        sport.leagueId,
        season,
        teamStatsCache
      );

      if (!lambdas) { noStats++; continue; } // skip — no real data

      const { lH, lA } = lambdas;
      const matrix = labBuildMatrix(lH, lA);
      const probs  = labCalcOutcomes(matrix);

      // ── Extract best odds ────────────────────────────────
      let bestHome = 0, bestDraw = 0, bestAway = 0;
      let homeBook = '', drawBook = '', awayBook = '';
      let bestOver25 = 0, bestUnder25 = 0;
      let bestOver35 = 0, bestUnder35 = 0;

      for (const book of event.bookmakers) {
        const h2h = book.markets?.find(m => m.key === 'h2h');
        if (h2h) {
          for (const o of h2h.outcomes) {
            if (nameMatch(o.name, event.home_team) && o.price > bestHome) {
              bestHome = o.price; homeBook = book.title;
            } else if (nameMatch(o.name, event.away_team) && o.price > bestAway) {
              bestAway = o.price; awayBook = book.title;
            } else if (o.name === 'Draw' && o.price > bestDraw) {
              bestDraw = o.price; drawBook = book.title;
            }
          }
        }
        const totals = book.markets?.find(m => m.key === 'totals');
        if (totals) {
          for (const o of totals.outcomes) {
            const pt = o.point || 0;
            if (o.name === 'Over'  && Math.abs(pt - 2.5) < 0.01 && o.price > bestOver25)  { bestOver25  = o.price; }
            if (o.name === 'Under' && Math.abs(pt - 2.5) < 0.01 && o.price > bestUnder25) { bestUnder25 = o.price; }
            if (o.name === 'Over'  && Math.abs(pt - 3.5) < 0.01 && o.price > bestOver35)  { bestOver35  = o.price; }
            if (o.name === 'Under' && Math.abs(pt - 3.5) < 0.01 && o.price > bestUnder35) { bestUnder35 = o.price; }
          }
        }
      }

      if (!bestHome || !bestDraw || !bestAway) continue;

      const trueOdds = stripMargin(bestHome, bestDraw, bestAway);
      const date = new Date(event.commence_time).toISOString().split('T')[0];

      const base = {
        fixture_id:  fixtureId,
        date,
        league:      sport.league,
        home_team:   event.home_team,
        away_team:   event.away_team,
        event_time:  event.commence_time,
        stake:       1,
        result:      'pending',
        profit_loss: null,
        settled_at:  null,
      };

      const modelNotes = `xG: ${lH.toFixed(2)} vs ${lA.toFixed(2)}`;
      const rows = [];

      // ── H2H markets (min 3% edge) ────────────────────────
      const homeEdge = parseFloat(((probs.homeWin - trueOdds.trueHome) * 100).toFixed(2));
      const drawEdge = parseFloat(((probs.draw    - trueOdds.trueDraw) * 100).toFixed(2));
      const awayEdge = parseFloat(((probs.awayWin - trueOdds.trueAway) * 100).toFixed(2));

      if (Math.abs(homeEdge) >= MIN_H2H_EDGE) rows.push({
        ...base, market: 'h2h_home', selection: `${event.home_team} Win`,
        odds: parseFloat(bestHome.toFixed(2)), model_prob: parseFloat(probs.homeWin.toFixed(4)),
        edge_pct: homeEdge, notes: `${modelNotes} | Book: ${homeBook}`,
      });

      if (Math.abs(drawEdge) >= MIN_H2H_EDGE) rows.push({
        ...base, market: 'h2h_draw', selection: 'Draw',
        odds: parseFloat(bestDraw.toFixed(2)), model_prob: parseFloat(probs.draw.toFixed(4)),
        edge_pct: drawEdge, notes: `${modelNotes} | Book: ${drawBook}`,
      });

      if (Math.abs(awayEdge) >= MIN_H2H_EDGE) rows.push({
        ...base, market: 'h2h_away', selection: `${event.away_team} Win`,
        odds: parseFloat(bestAway.toFixed(2)), model_prob: parseFloat(probs.awayWin.toFixed(4)),
        edge_pct: awayEdge, notes: `${modelNotes} | Book: ${awayBook}`,
      });

      // ── Over/Under markets (min 5% edge) ─────────────────
      if (bestOver25 && bestUnder25) {
        const o25Edge = parseFloat(((probs.over25  - 1/bestOver25)  * 100).toFixed(2));
        const u25Edge = parseFloat(((probs.under25 - 1/bestUnder25) * 100).toFixed(2));
        if (Math.abs(o25Edge) >= MIN_TOTAL_EDGE) rows.push({
          ...base, market: 'over25', selection: 'Over 2.5',
          odds: parseFloat(bestOver25.toFixed(2)), model_prob: parseFloat(probs.over25.toFixed(4)),
          edge_pct: o25Edge, notes: modelNotes,
        });
        if (Math.abs(u25Edge) >= MIN_TOTAL_EDGE) rows.push({
          ...base, market: 'under25', selection: 'Under 2.5',
          odds: parseFloat(bestUnder25.toFixed(2)), model_prob: parseFloat(probs.under25.toFixed(4)),
          edge_pct: u25Edge, notes: modelNotes,
        });
      }

      if (bestOver35 && bestUnder35) {
        const o35Edge = parseFloat(((probs.over35  - 1/bestOver35)  * 100).toFixed(2));
        const u35Edge = parseFloat(((probs.under35 - 1/bestUnder35) * 100).toFixed(2));
        if (Math.abs(o35Edge) >= MIN_TOTAL_EDGE) rows.push({
          ...base, market: 'over35', selection: 'Over 3.5',
          odds: parseFloat(bestOver35.toFixed(2)), model_prob: parseFloat(probs.over35.toFixed(4)),
          edge_pct: o35Edge, notes: modelNotes,
        });
        if (Math.abs(u35Edge) >= MIN_TOTAL_EDGE) rows.push({
          ...base, market: 'under35', selection: 'Under 3.5',
          odds: parseFloat(bestUnder35.toFixed(2)), model_prob: parseFloat(probs.under35.toFixed(4)),
          edge_pct: u35Edge, notes: modelNotes,
        });
      }

      if (!rows.length) { skipped++; continue; }

      const { error } = await supabase.from('football_lab').insert(rows);
      if (error) {
        console.error(`⚽ Lab insert error [${event.home_team} vs ${event.away_team}]:`, error.message);
      } else {
        inserted++;
      }

    } catch(e) {
      console.error('⚽ Lab error:', e.message);
    }
  }

  if (inserted > 0 || noStats > 0) {
    console.log(`⚽ Lab [${sport.league}]: ${inserted} logged, ${noStats} skipped (no stats), ${skipped} already exist`);
  }
}

// ── SETTLEMENT ────────────────────────────────────────────────

async function settleFootballLab(supabase, ODDS_BASE, ODDS_API_KEY) {
  try {
    const { data: pending } = await supabase
      .from('football_lab')
      .select('*')
      .eq('result', 'pending')
      .lt('event_time', new Date().toISOString());

    if (!pending?.length) return;

    const byLeague = {};
    for (const row of pending) {
      if (!byLeague[row.league]) byLeague[row.league] = [];
      byLeague[row.league].push(row);
    }

    const leagueKeyMap = {
      'Premier League':   'soccer_epl',
      'La Liga':          'soccer_spain_la_liga',
      'Bundesliga':       'soccer_germany_bundesliga',
      'Serie A':          'soccer_italy_serie_a',
      'Ligue 1':          'soccer_france_ligue_one',
      'Champions League': 'soccer_uefa_champs_league',
    };

    const scoreCache = {};
    let settled = 0;

    for (const [league, rows] of Object.entries(byLeague)) {
      const sportKey = leagueKeyMap[league];
      if (!sportKey) continue;

      if (!scoreCache[sportKey]) {
        try {
          const res = await fetch(`${ODDS_BASE}/sports/${sportKey}/scores/?apiKey=${ODDS_API_KEY}&daysFrom=3`);
          if (!res.ok) continue;
          scoreCache[sportKey] = await res.json();
        } catch(e) { continue; }
      }

      const scores = scoreCache[sportKey];
      const byFixture = {};
      for (const row of rows) {
        if (!byFixture[row.fixture_id]) byFixture[row.fixture_id] = [];
        byFixture[row.fixture_id].push(row);
      }

      for (const [fixtureId, fixtureRows] of Object.entries(byFixture)) {
        const first = fixtureRows[0];
        const match = scores.find(s => s.completed && (
          (nameMatch(s.home_team, first.home_team) && nameMatch(s.away_team, first.away_team)) ||
          (nameMatch(s.home_team, first.away_team) && nameMatch(s.away_team, first.home_team))
        ));
        if (!match?.scores) continue;

        const hh = nameMatch(match.home_team, first.home_team);
        const hs  = parseFloat(match.scores.find(s => nameMatch(s.name, match.home_team))?.score || 0);
        const as2 = parseFloat(match.scores.find(s => nameMatch(s.name, match.away_team))?.score || 0);
        const homeScore = hh ? hs : as2;
        const awayScore = hh ? as2 : hs;
        const total = homeScore + awayScore;
        const settledAt = new Date().toLocaleDateString('en-CA', { timeZone: 'Europe/London' }) + 'T12:00:00.000Z';

        for (const row of fixtureRows) {
          let won = false;
          switch(row.market) {
            case 'h2h_home':  won = homeScore > awayScore; break;
            case 'h2h_draw':  won = homeScore === awayScore; break;
            case 'h2h_away':  won = awayScore > homeScore; break;
            case 'over25':    won = total > 2.5; break;
            case 'under25':   won = total < 2.5; break;
            case 'over35':    won = total > 3.5; break;
            case 'under35':   won = total < 3.5; break;
            default: continue;
          }
          const pl = won
            ? parseFloat(((row.odds - 1) * row.stake).toFixed(2))
            : parseFloat((-row.stake).toFixed(2));

          await supabase.from('football_lab').update({
            result: won ? 'WON' : 'LOST', profit_loss: pl, settled_at: settledAt,
          }).eq('id', row.id);
          settled++;
        }
      }
    }

    if (settled > 0) console.log(`⚽ Football lab: settled ${settled} rows`);
  } catch(e) {
    console.error('⚽ Lab settlement error:', e.message);
  }
}

module.exports = { runFootballLab, settleFootballLab };

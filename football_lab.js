// ================================================================
// FOOTBALL LAB — Experimental market tracker
// ================================================================
// Tracks H2H and Over/Under markets across all football fixtures.
// Completely independent from main tips engine.
// Writes only to football_lab table in Supabase.
// Zero additional API calls — reuses events already fetched.
// ================================================================

// ── LEAGUE AVERAGES FALLBACK ─────────────────────────────────
// Used when no team stats available (free plan limitation).
// Simple but sufficient for market pattern discovery.
const LAB_LEAGUE_AVG = { homeGoals: 1.55, awayGoals: 1.18 };

// ── HELPERS ──────────────────────────────────────────────────

function labPoisson(lambda, k) {
  if (lambda <= 0) return k === 0 ? 1 : 0;
  let logP = -lambda + k * Math.log(lambda);
  for (let i = 1; i <= k; i++) logP -= Math.log(i);
  return Math.exp(logP);
}

function labBuildMatrix(lH, lA) {
  const N = 8;
  const RHO = 0.10;
  const matrix = [];
  let total = 0;
  for (let h = 0; h <= N; h++) {
    matrix[h] = [];
    for (let a = 0; a <= N; a++) {
      const raw = labPoisson(lH, h) * labPoisson(lA, a);
      let tau = 1;
      if (h === 0 && a === 0) tau = 1 - lH * lA * RHO;
      else if (h === 1 && a === 0) tau = 1 + lA * RHO;
      else if (h === 0 && a === 1) tau = 1 + lH * RHO;
      else if (h === 1 && a === 1) tau = 1 - RHO;
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

// ── MAIN LAB FUNCTION ─────────────────────────────────────────
// Called from generateTips() with already-fetched events.
// Zero extra API calls.

async function runFootballLab(supabase, events, sport) {
  if (!events?.length) return;

  // Only process football events
  if (sport.name !== 'Football') return;

  let inserted = 0, skipped = 0;

  for (const event of events) {
    try {
      const hours = (new Date(event.commence_time) - new Date()) / 3600000;
      if (hours < 0 || hours > 72) continue; // only upcoming within 72hrs
      if (!event.bookmakers?.length) continue;

      const fixtureId = event.id;
      const date = new Date(event.commence_time).toISOString().split('T')[0];

      // Check if already logged for this fixture
      const { data: existing } = await supabase
        .from('football_lab')
        .select('id')
        .eq('fixture_id', fixtureId)
        .maybeSingle();

      if (existing) { skipped++; continue; }

      // ── Extract H2H odds ─────────────────────────────────
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
            if (o.name === 'Over'  && Math.abs(pt - 2.5) < 0.01 && o.price > bestOver25)  bestOver25  = o.price;
            if (o.name === 'Under' && Math.abs(pt - 2.5) < 0.01 && o.price > bestUnder25) bestUnder25 = o.price;
            if (o.name === 'Over'  && Math.abs(pt - 3.5) < 0.01 && o.price > bestOver35)  bestOver35  = o.price;
            if (o.name === 'Under' && Math.abs(pt - 3.5) < 0.01 && o.price > bestUnder35) bestUnder35 = o.price;
          }
        }
      }

      if (!bestHome || !bestDraw || !bestAway) continue; // need full 1X2

      // ── Model probabilities (league average λ) ───────────
      const lH = LAB_LEAGUE_AVG.homeGoals;
      const lA = LAB_LEAGUE_AVG.awayGoals;
      const matrix  = labBuildMatrix(lH, lA);
      const probs   = labCalcOutcomes(matrix);
      const trueOdds = stripMargin(bestHome, bestDraw, bestAway);

      // ── Build rows ───────────────────────────────────────
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

      const rows = [
        // H2H markets
        {
          ...base,
          market:     'h2h_home',
          selection:  `${event.home_team} Win`,
          odds:       parseFloat(bestHome.toFixed(2)),
          model_prob: parseFloat(probs.homeWin.toFixed(4)),
          edge_pct:   parseFloat(((probs.homeWin - trueOdds.trueHome) * 100).toFixed(2)),
          notes:      `Book: ${homeBook} | λ: ${lH.toFixed(2)} vs ${lA.toFixed(2)}`,
        },
        {
          ...base,
          market:     'h2h_draw',
          selection:  'Draw',
          odds:       parseFloat(bestDraw.toFixed(2)),
          model_prob: parseFloat(probs.draw.toFixed(4)),
          edge_pct:   parseFloat(((probs.draw - trueOdds.trueDraw) * 100).toFixed(2)),
          notes:      `Book: ${drawBook} | λ: ${lH.toFixed(2)} vs ${lA.toFixed(2)}`,
        },
        {
          ...base,
          market:     'h2h_away',
          selection:  `${event.away_team} Win`,
          odds:       parseFloat(bestAway.toFixed(2)),
          model_prob: parseFloat(probs.awayWin.toFixed(4)),
          edge_pct:   parseFloat(((probs.awayWin - trueOdds.trueAway) * 100).toFixed(2)),
          notes:      `Book: ${awayBook} | λ: ${lH.toFixed(2)} vs ${lA.toFixed(2)}`,
        },
      ];

      // Over/Under 2.5
      if (bestOver25 && bestUnder25) {
        const over25IP  = 1 / bestOver25;
        const under25IP = 1 / bestUnder25;
        rows.push(
          { ...base, market: 'over25',  selection: 'Over 2.5',  odds: parseFloat(bestOver25.toFixed(2)),  model_prob: parseFloat(probs.over25.toFixed(4)),  edge_pct: parseFloat(((probs.over25  - over25IP)  * 100).toFixed(2)), notes: `λ: ${lH.toFixed(2)} vs ${lA.toFixed(2)}` },
          { ...base, market: 'under25', selection: 'Under 2.5', odds: parseFloat(bestUnder25.toFixed(2)), model_prob: parseFloat(probs.under25.toFixed(4)), edge_pct: parseFloat(((probs.under25 - under25IP) * 100).toFixed(2)), notes: `λ: ${lH.toFixed(2)} vs ${lA.toFixed(2)}` }
        );
      }

      // Over/Under 3.5
      if (bestOver35 && bestUnder35) {
        const over35IP  = 1 / bestOver35;
        const under35IP = 1 / bestUnder35;
        rows.push(
          { ...base, market: 'over35',  selection: 'Over 3.5',  odds: parseFloat(bestOver35.toFixed(2)),  model_prob: parseFloat(probs.over35.toFixed(4)),  edge_pct: parseFloat(((probs.over35  - over35IP)  * 100).toFixed(2)), notes: `λ: ${lH.toFixed(2)} vs ${lA.toFixed(2)}` },
          { ...base, market: 'under35', selection: 'Under 3.5', odds: parseFloat(bestUnder35.toFixed(2)), model_prob: parseFloat(probs.under35.toFixed(4)), edge_pct: parseFloat(((probs.under35 - under35IP) * 100).toFixed(2)), notes: `λ: ${lH.toFixed(2)} vs ${lA.toFixed(2)}` }
        );
      }

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

  if (inserted > 0) {
    console.log(`⚽ Football lab: ${inserted} fixtures logged, ${skipped} skipped (${sport.league})`);
  }
}

// ── SETTLEMENT ────────────────────────────────────────────────
// Called from settleResults(). Uses Odds API scores endpoint.
// Settles all pending football_lab rows once fixture is complete.

async function settleFootballLab(supabase, ODDS_BASE, ODDS_API_KEY) {
  try {
    const { data: pending } = await supabase
      .from('football_lab')
      .select('*')
      .eq('result', 'pending')
      .lt('event_time', new Date().toISOString());

    if (!pending?.length) return;

    // Group by league to minimise API calls
    const byLeague = {};
    for (const row of pending) {
      if (!byLeague[row.league]) byLeague[row.league] = [];
      byLeague[row.league].push(row);
    }

    // Map league name to sport key
    const leagueKeyMap = {
      'Premier League':   'soccer_epl',
      'La Liga':          'soccer_spain_la_liga',
      'Bundesliga':       'soccer_germany_bundesliga',
      'Serie A':          'soccer_italy_serie_a',
      'Ligue 1':          'soccer_france_ligue_one',
      'Champions League': 'soccer_uefa_champs_league',
    };

    const scoreCache = {}; // cache per sport key to avoid duplicate calls

    let settled = 0;

    for (const [league, rows] of Object.entries(byLeague)) {
      const sportKey = leagueKeyMap[league];
      if (!sportKey) continue;

      // Fetch scores (cached per sport key)
      if (!scoreCache[sportKey]) {
        try {
          const res = await fetch(`${ODDS_BASE}/sports/${sportKey}/scores/?apiKey=${ODDS_API_KEY}&daysFrom=3`);
          if (!res.ok) continue;
          scoreCache[sportKey] = await res.json();
        } catch(e) { continue; }
      }

      const scores = scoreCache[sportKey];

      // Group rows by fixture
      const byFixture = {};
      for (const row of rows) {
        if (!byFixture[row.fixture_id]) byFixture[row.fixture_id] = [];
        byFixture[row.fixture_id].push(row);
      }

      for (const [fixtureId, fixtureRows] of Object.entries(byFixture)) {
        const first = fixtureRows[0];

        // Find matching completed score
        const match = scores.find(s => s.completed && (
          (nameMatch(s.home_team, first.home_team) && nameMatch(s.away_team, first.away_team)) ||
          (nameMatch(s.home_team, first.away_team) && nameMatch(s.away_team, first.home_team))
        ));

        if (!match?.scores) continue;

        const hh = nameMatch(match.home_team, first.home_team);
        const hs = parseFloat(match.scores.find(s => nameMatch(s.name, match.home_team))?.score || 0);
        const as2 = parseFloat(match.scores.find(s => nameMatch(s.name, match.away_team))?.score || 0);
        const homeScore = hh ? hs : as2;
        const awayScore = hh ? as2 : hs;
        const total = homeScore + awayScore;
        const settledAt = new Date().toLocaleDateString('en-CA', { timeZone: 'Europe/London' }) + 'T12:00:00.000Z';

        // Settle each market row
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
            result:      won ? 'WON' : 'LOST',
            profit_loss: pl,
            settled_at:  settledAt,
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

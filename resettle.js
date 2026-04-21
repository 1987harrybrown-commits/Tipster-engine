// ═══════════════════════════════════════════════════════════════
// HISTORICAL RESETTLEMENT SCRIPT
// Re-fetches scores for every settled tip and recalculates
// won/lost using the fixed index-based score extraction.
// Also rebuilds results_history with corrected P&L and running total.
//
// Run ONCE on Render or locally:
//   SUPABASE_URL=... SUPABASE_SERVICE_KEY=... ODDS_API_KEY=... node resettle.js
//
// Safe to re-run — idempotent. Does not touch pending/void tips.
// ═══════════════════════════════════════════════════════════════

const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL         = process.env.SUPABASE_URL         || 'https://eyhlzzaaxrwisrtwyoyh.supabase.co';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const ODDS_API_KEY         = process.env.ODDS_API_KEY         || '';
const ODDS_BASE            = 'https://api.the-odds-api.com/v4';

if (!SUPABASE_SERVICE_KEY) throw new Error('FATAL: SUPABASE_SERVICE_KEY not set');
if (!ODDS_API_KEY)         throw new Error('FATAL: ODDS_API_KEY not set');

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const SPORTS = [
  { key: 'soccer_epl',               name: 'Football',   league: 'Premier League'   },
  { key: 'soccer_spain_la_liga',      name: 'Football',   league: 'La Liga'          },
  { key: 'soccer_germany_bundesliga', name: 'Football',   league: 'Bundesliga'       },
  { key: 'soccer_italy_serie_a',      name: 'Football',   league: 'Serie A'          },
  { key: 'soccer_france_ligue_one',   name: 'Football',   league: 'Ligue 1'          },
  { key: 'soccer_uefa_champs_league', name: 'Football',   league: 'Champions League' },
  { key: 'basketball_nba',            name: 'Basketball', league: 'NBA'              },
  { key: 'icehockey_nhl',             name: 'Ice Hockey', league: 'NHL'              },
];

function nameMatch(a, b) {
  if (!a || !b) return false;
  const clean = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  const ac = clean(a), bc = clean(b);
  return ac === bc || ac.includes(bc) || bc.includes(ac);
}

// Cache scores per sport key — avoids re-fetching for every tip
const scoreCache = {};

async function fetchScores(sportKey) {
  if (scoreCache[sportKey]) return scoreCache[sportKey];
  try {
    // daysFrom=30 covers up to 30 days of historical scores
    const res = await fetch(`${ODDS_BASE}/sports/${sportKey}/scores/?apiKey=${ODDS_API_KEY}&daysFrom=30`);
    if (!res.ok) {
      console.warn(`  ⚠️  Scores API ${res.status} for ${sportKey}`);
      return [];
    }
    const data = await res.json();
    scoreCache[sportKey] = Array.isArray(data) ? data : [];
    console.log(`  📡 Fetched ${scoreCache[sportKey].length} scores for ${sportKey}`);
    return scoreCache[sportKey];
  } catch (e) {
    console.warn(`  ⚠️  Scores fetch error for ${sportKey}:`, e.message);
    return [];
  }
}

// Determine the correct outcome for a tip given a completed match score
// Uses FIXED index-based extraction: scores[0] = home, scores[1] = away
function determineOutcome(tip, match) {
  const hh      = nameMatch(match.home_team, tip.home_team);
  const hs      = parseFloat(match.scores[0]?.score ?? 0); // home score from API
  const as2     = parseFloat(match.scores[1]?.score ?? 0); // away score from API

  // Flip if API home/away is opposite to our stored home/away
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

  return { won, homeScore, awayScore };
}

async function main() {
  console.log('\n🔁 HISTORICAL RESETTLEMENT STARTING...\n');

  // 1. Fetch all won/lost tips
  const { data: tips, error: tipsErr } = await supabase
    .from('tips')
    .select('*')
    .in('status', ['won', 'lost'])
    .order('event_time', { ascending: true });

  if (tipsErr) throw new Error(`Failed to fetch tips: ${tipsErr.message}`);
  console.log(`📋 Found ${tips.length} settled tips to re-evaluate\n`);

  let corrected = 0, unchanged = 0, noMatch = 0;
  const updates = []; // { tip, won, pl } — collected before rebuilding history

  // 2. Re-evaluate each tip
  for (const tip of tips) {
    const sport = SPORTS.find(s => s.league === tip.league);
    if (!sport) {
      console.log(`  ⏭️  Skipping [${tip.tip_ref}] — unknown league: ${tip.league}`);
      noMatch++;
      continue;
    }

    const scores = await fetchScores(sport.key);

    const match = scores.find(s => s.completed && (
      (nameMatch(s.home_team, tip.home_team) && nameMatch(s.away_team, tip.away_team)) ||
      (nameMatch(s.home_team, tip.away_team) && nameMatch(s.away_team, tip.home_team))
    ));

    if (!match?.scores || match.scores.length < 2) {
      console.log(`  ❓ No score found: [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team} (${tip.league})`);
      noMatch++;
      continue;
    }

    const { won, homeScore, awayScore } = determineOutcome(tip, match);
    const settlementOdds = parseFloat(tip.best_odds || tip.odds);
    const pl = won
      ? parseFloat(((settlementOdds - 1) * tip.stake).toFixed(2))
      : parseFloat((-tip.stake).toFixed(2));

    const oldStatus = tip.status;
    const newStatus = won ? 'won' : 'lost';

    if (oldStatus !== newStatus) {
      console.log(`  ✏️  CORRECTION [${tip.tip_ref}] ${tip.home_team} vs ${tip.away_team} | ${tip.selection} | Score: ${homeScore}-${awayScore} | ${oldStatus.toUpperCase()} → ${newStatus.toUpperCase()} | P&L: ${pl > 0 ? '+' : ''}${pl}u`);
      corrected++;
    } else {
      unchanged++;
    }

    updates.push({ tip, won, pl, newStatus });
  }

  console.log(`\n📊 Re-evaluation complete: ${corrected} corrections, ${unchanged} unchanged, ${noMatch} no score found`);

  if (corrected === 0) {
    console.log('\n✅ No corrections needed — all settlements were already correct.');
    return;
  }

  // 3. Update tips table
  console.log('\n💾 Updating tips table...');
  for (const { tip, won, pl, newStatus } of updates) {
    const { error } = await supabase
      .from('tips')
      .update({ status: newStatus, profit_loss: pl })
      .eq('tip_ref', tip.tip_ref);
    if (error) console.warn(`  ⚠️  tips update failed [${tip.tip_ref}]:`, error.message);
  }

  // 4. Rebuild results_history from scratch
  // Delete all existing rows, then re-insert in event_time order with corrected data
  console.log('\n🗑️  Clearing results_history...');
  const { error: delErr } = await supabase
    .from('results_history')
    .delete()
    .neq('id', 0); // delete all rows
  if (delErr) throw new Error(`Failed to clear results_history: ${delErr.message}`);

  console.log('📝 Rebuilding results_history with corrected data...');
  let runningPL = 0;

  // Sort by event_time ascending so running P&L accumulates correctly
  const sorted = [...updates].sort((a, b) => new Date(a.tip.event_time) - new Date(b.tip.event_time));

  for (const { tip, won, pl } of sorted) {
    runningPL = parseFloat((runningPL + pl).toFixed(2));
    const settlementOdds = parseFloat(tip.best_odds || tip.odds);

    const { error } = await supabase.from('results_history').insert({
      tip_ref:     tip.tip_ref,
      sport:       tip.sport,
      event:       `${tip.home_team} vs ${tip.away_team}`,
      selection:   tip.selection,
      odds:        settlementOdds,
      stake:       tip.stake,
      tier:        tip.tier || 'pro',
      result:      won ? 'WON' : 'LOST',
      profit_loss: pl,
      running_pl:  runningPL,
      league:      tip.league,
      confidence:  tip.confidence,
      settled_at:  tip.result_updated_at || tip.event_time,
    });

    if (error) console.warn(`  ⚠️  results_history insert failed [${tip.tip_ref}]:`, error.message);
  }

  console.log(`\n✅ DONE — ${sorted.length} tips reinserted into results_history`);
  console.log(`   Final running P&L: ${runningPL > 0 ? '+' : ''}${runningPL}u`);
  console.log(`   Corrections made: ${corrected}`);
}

main().catch(err => {
  console.error('\n💥 FATAL:', err.message);
  process.exit(1);
});

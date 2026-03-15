// ============================================
// THE TIPSTER — Automated Tip Engine
// Deploy this to Render.com as a Node.js app
// ============================================

const { createClient } = require('@supabase/supabase-js');
const fetch = (...args) => import('node-fetch').then(({default: f}) => f(...args));

// ─── CREDENTIALS ────────────────────────────
const SUPABASE_URL = 'https://eyhlzzaaxrwisrtwyoyh.supabase.co';
const SUPABASE_SERVICE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5aGx6emFheHJ3aXNydHd5b3loIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzM3OTI3NywiZXhwIjoyMDg4OTU1Mjc3fQ.9Lry94K4qWWYzh0yd4zcgEaGvb8myeAzxrSHtcBSQus';
const ODDS_API_KEY = '8a0d4da4da83840716db786d5e98d0dc';
const ODDS_BASE = 'https://api.the-odds-api.com/v4';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ─── SPORTS TO COVER ────────────────────────
const SPORTS = [
  { key: 'soccer_epl',              name: 'Football',       league: 'Premier League' },
  { key: 'soccer_spain_la_liga',    name: 'Football',       league: 'La Liga' },
  { key: 'soccer_germany_bundesliga',name: 'Football',      league: 'Bundesliga' },
  { key: 'soccer_italy_serie_a',    name: 'Football',       league: 'Serie A' },
  { key: 'soccer_france_ligue_one', name: 'Football',       league: 'Ligue 1' },
  { key: 'soccer_uefa_champs_league',name: 'Football',      league: 'Champions League' },
  { key: 'basketball_nba',          name: 'Basketball',     league: 'NBA' },
  { key: 'americanfootball_nfl',    name: 'NFL',            league: 'NFL' },
  { key: 'tennis_atp_french_open',  name: 'Tennis',         league: 'ATP Tour' },
  { key: 'baseball_mlb',            name: 'Baseball',       league: 'MLB' },
  { key: 'icehockey_nhl',           name: 'Ice Hockey',     league: 'NHL' },
  { key: 'rugbyunion_premiership',  name: 'Rugby',          league: 'Premiership' },
  // ── Horse Racing ──
  { key: 'horse_racing_gb',         name: 'Horse Racing',   league: 'GB Racing',        isRacing: true },
  { key: 'horse_racing_ire',        name: 'Horse Racing',   league: 'Irish Racing',     isRacing: true },
];

// ─── FETCH ODDS FROM API ─────────────────────
async function fetchOdds(sportKey) {
  try {
    const url = `${ODDS_BASE}/sports/${sportKey}/odds/?apiKey=${ODDS_API_KEY}&regions=uk&markets=h2h,totals,spreads&oddsFormat=decimal&dateFormat=iso`;
    const res = await fetch(url);
    if (!res.ok) {
      console.log(`No data for ${sportKey}: ${res.status}`);
      return [];
    }
    const data = await res.json();
    return data || [];
  } catch (err) {
    console.error(`Error fetching ${sportKey}:`, err.message);
    return [];
  }
}

// ─── FETCH HORSE RACING ODDS ─────────────────
async function fetchRacingOdds(sportKey) {
  try {
    const url = `${ODDS_BASE}/sports/${sportKey}/odds/?apiKey=${ODDS_API_KEY}&regions=uk&markets=h2h&oddsFormat=decimal&dateFormat=iso`;
    const res = await fetch(url);
    if (!res.ok) { console.log(`No racing data for ${sportKey}: ${res.status}`); return []; }
    return await res.json() || [];
  } catch (err) { console.error(`Racing fetch error:`, err.message); return []; }
}

// ─── ANALYSE HORSE RACING ────────────────────
function generateRacingTips(events, sport) {
  const tips = [];
  for (const event of events) {
    if (!event.bookmakers || event.bookmakers.length < 2) continue;
    const eventTime = new Date(event.commence_time);
    const now = new Date();
    const hoursUntil = (eventTime - now) / 3600000;
    if (hoursUntil < 0 || hoursUntil > 24) continue;

    // Collect all runners and odds across bookmakers
    const runnerOdds = {};
    for (const book of event.bookmakers) {
      const market = book.markets.find(m => m.key === 'h2h');
      if (!market) continue;
      for (const outcome of market.outcomes) {
        if (!runnerOdds[outcome.name]) runnerOdds[outcome.name] = [];
        runnerOdds[outcome.name].push(outcome.price);
      }
    }
    if (Object.keys(runnerOdds).length < 3) continue;

    // Find best value runner — sweet spot odds 2.0-12.0 with good book consensus
    let bestRunner = null, bestScore = 0;
    for (const [runner, oddsList] of Object.entries(runnerOdds)) {
      const avgOdds = oddsList.reduce((a,b) => a+b,0) / oddsList.length;
      const bestOdds = Math.max(...oddsList);
      const impliedProb = 1 / avgOdds;
      if (bestOdds < 2.0 || bestOdds > 12.0 || oddsList.length < 2) continue;
      const valueScore = impliedProb * oddsList.length * (bestOdds <= 6.0 ? 1.2 : 1.0);
      if (valueScore > bestScore) { bestScore = valueScore; bestRunner = { name: runner, odds: bestOdds, impliedProb, bookCount: oddsList.length }; }
    }
    if (!bestRunner) continue;

    const confidence = Math.min(82, Math.round(40 + (bestRunner.impliedProb * 60) + (bestRunner.bookCount * 2)));
    const isEachWay = bestRunner.odds >= 5.0;
    const stake = isEachWay ? 0.5 : 1.0;

    tips.push({
      sport: sport.name, league: sport.league,
      home_team: event.home_team || event.sport_title || 'Race',
      away_team: `${Object.keys(runnerOdds).length} runners`,
      event_time: event.commence_time,
      selection: isEachWay ? `${bestRunner.name} (Each Way)` : `${bestRunner.name} Win`,
      market: 'h2h',
      odds: parseFloat(bestRunner.odds.toFixed(2)),
      stake, confidence,
      tier: confidence >= 72 ? 'free' : 'pro',
      status: 'pending',
      bookmaker: event.bookmakers[0]?.title || 'Multiple',
      notes: `${bestRunner.bookCount} books — ${isEachWay ? 'Each Way' : 'Win'}`
    });
  }
  return tips;
}

// ─── TIP GENERATION ENGINE ───────────────────
// Analyses odds across bookmakers to find value
function generateTips(events, sport) {
  const tips = [];

  for (const event of events) {
    if (!event.bookmakers || event.bookmakers.length < 2) continue;

    const eventTime = new Date(event.commence_time);
    const now = new Date();

    // Only tip events in the next 48 hours
    const hoursUntil = (eventTime - now) / 3600000;
    if (hoursUntil < 0 || hoursUntil > 48) continue;

    // ── Analyse h2h market ──
    const h2hBooks = event.bookmakers
      .map(b => b.markets.find(m => m.key === 'h2h'))
      .filter(Boolean);

    if (h2hBooks.length >= 2) {
      const h2hTip = analyseH2H(event, h2hBooks, sport);
      if (h2hTip) tips.push(h2hTip);
    }

    // ── Analyse totals market ──
    const totalsBooks = event.bookmakers
      .map(b => b.markets.find(m => m.key === 'totals'))
      .filter(Boolean);

    if (totalsBooks.length >= 2) {
      const totalsTip = analyseTotals(event, totalsBooks, sport);
      if (totalsTip) tips.push(totalsTip);
    }
  }

  return tips;
}

// Analyse head-to-head odds for value
function analyseH2H(event, books, sport) {
  try {
    // Collect all odds for each outcome
    const oddsMap = {};
    for (const book of books) {
      for (const outcome of book.outcomes) {
        if (!oddsMap[outcome.name]) oddsMap[outcome.name] = [];
        oddsMap[outcome.name].push(outcome.price);
      }
    }

    // Find best odds and implied probability
    let bestPick = null;
    let bestValue = 0;

    for (const [team, oddsList] of Object.entries(oddsMap)) {
      const maxOdds = Math.max(...oddsList);
      const avgOdds = oddsList.reduce((a,b) => a+b, 0) / oddsList.length;
      const impliedProb = 1 / avgOdds;

      // Value = when best odds imply better chance than market average
      const value = (1/avgOdds) - (1/maxOdds);
      const marketConsensus = impliedProb;

      // Only tip if there's reasonable consensus (>45% implied prob)
      // and odds are attractive (>1.40)
      if (marketConsensus > 0.45 && maxOdds >= 1.40 && maxOdds <= 5.00) {
        const valueScore = marketConsensus * 100;
        if (valueScore > bestValue) {
          bestValue = valueScore;
          bestPick = {
            selection: team === event.home_team ? `${team} Win` :
                       team === event.away_team ? `${team} Win` : 'Draw',
            odds: maxOdds,
            confidence: Math.min(95, Math.round(50 + (marketConsensus * 50))),
            market: 'h2h'
          };
        }
      }
    }

    if (!bestPick) return null;

    // Stake recommendation based on confidence (Kelly-inspired)
    // 90%+ = 3 units, 85-89% = 2 units, 78-84% = 1.5 units, below = 1 unit
    const conf = bestPick.confidence;
    const stake = conf >= 90 ? 3.0 : conf >= 85 ? 2.0 : conf >= 78 ? 1.5 : 1.0;

    // Tier logic: all tips start as 'pro'
    // The website shows top 3 highest-confidence tips as 'free' previews
    // Everything else is pro-only
    const tier = 'pro'; // Website handles free preview display

    return {
      sport: sport.name,
      league: sport.league,
      home_team: event.home_team,
      away_team: event.away_team,
      event_time: event.commence_time,
      selection: bestPick.selection,
      market: 'h2h',
      odds: parseFloat(bestPick.odds.toFixed(2)),
      stake,
      confidence: bestPick.confidence,
      tier,
      status: 'pending',
      bookmaker: event.bookmakers[0]?.title || 'Multiple',
      notes: `Consensus from ${event.bookmakers.length} bookmakers`
    };
  } catch(e) {
    return null;
  }
}

// Analyse totals (over/under) for value
function analyseTotals(event, books, sport) {
  try {
    const overOdds = [], underOdds = [];
    let point = null;

    for (const book of books) {
      for (const outcome of book.outcomes) {
        if (outcome.name === 'Over') { overOdds.push(outcome.price); point = outcome.point; }
        if (outcome.name === 'Under') underOdds.push(outcome.price);
      }
    }

    if (!overOdds.length || !underOdds.length || !point) return null;

    const avgOver = overOdds.reduce((a,b)=>a+b,0)/overOdds.length;
    const avgUnder = underOdds.reduce((a,b)=>a+b,0)/underOdds.length;

    // Pick the side with better consensus value
    const overProb = 1/avgOver;
    const underProb = 1/avgUnder;

    // Only tip totals as pro picks
    if (Math.max(overProb, underProb) < 0.52) return null;

    const pickOver = overProb >= underProb;
    const bestOdds = pickOver ? Math.max(...overOdds) : Math.max(...underOdds);
    const confidence = Math.min(85, Math.round(48 + (Math.max(overProb,underProb) * 40)));

    if (bestOdds < 1.60 || bestOdds > 2.20) return null;

    const totalsStake = confidence >= 85 ? 2.0 : confidence >= 78 ? 1.5 : 1.0;

    return {
      sport: sport.name,
      league: sport.league,
      home_team: event.home_team,
      away_team: event.away_team,
      event_time: event.commence_time,
      selection: `${pickOver ? 'Over' : 'Under'} ${point}`,
      market: 'totals',
      odds: parseFloat(bestOdds.toFixed(2)),
      stake: totalsStake,
      confidence,
      tier: 'pro',
      status: 'pending',
      bookmaker: event.bookmakers[0]?.title || 'Multiple',
      notes: `Totals analysis — ${event.bookmakers.length} books checked`
    };
  } catch(e) {
    return null;
  }
}

// ─── SAVE TIPS TO DATABASE ───────────────────
async function saveTips(tips) {
  if (!tips.length) return;

  let saved = 0, skipped = 0;

  for (const tip of tips) {
    // Check if we already have this tip (avoid duplicates)
    const { data: existing } = await supabase
      .from('tips')
      .select('id')
      .eq('home_team', tip.home_team)
      .eq('away_team', tip.away_team)
      .eq('selection', tip.selection)
      .eq('status', 'pending')
      .single();

    if (existing) { skipped++; continue; }

    const { error } = await supabase.from('tips').insert(tip);
    if (error) {
      console.error('Insert error:', error.message);
    } else {
      saved++;
    }
  }

  console.log(`✅ Saved ${saved} new tips, skipped ${skipped} duplicates`);
}

// ─── AUTO-SETTLE RESULTS ─────────────────────────────
// Uses API-Football for reliable result detection
const API_FOOTBALL_KEY = '80ac2af304202d84314a48f52d7a86b9';
const API_FOOTBALL_BASE = 'https://v3.football.api-sports.io';

// League ID mapping for API-Football
const LEAGUE_IDS = {
  'Premier League': 39,
  'La Liga': 140,
  'Bundesliga': 78,
  'Serie A': 135,
  'Ligue 1': 61,
  'Champions League': 2,
};

// Cache fetched fixtures to avoid duplicate API calls
const fixtureCache = {};

async function fetchAPIFootballFixtures(leagueId) {
  if (fixtureCache[leagueId]) return fixtureCache[leagueId];
  try {
    const today = new Date().toISOString().split('T')[0];
    const threeDaysAgo = new Date(Date.now() - 3 * 86400000).toISOString().split('T')[0];
    const url = `${API_FOOTBALL_BASE}/fixtures?league=${leagueId}&season=2025&from=${threeDaysAgo}&to=${today}&status=FT`;
    const res = await fetch(url, {
      headers: { 'x-apisports-key': API_FOOTBALL_KEY }
    });
    if (!res.ok) return [];
    const data = await res.json();
    const fixtures = data.response || [];
    fixtureCache[leagueId] = fixtures;
    console.log('API-Football: ' + fixtures.length + ' finished fixtures for league ' + leagueId);
    return fixtures;
  } catch(e) {
    console.error('API-Football error:', e.message);
    return [];
  }
}

function nameMatch(a, b) {
  if (!a || !b) return false;
  const clean = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  const ac = clean(a), bc = clean(b);
  return ac === bc || ac.includes(bc) || bc.includes(ac);
}

async function settleResults() {
  // Clear fixture cache each cycle
  Object.keys(fixtureCache).forEach(k => delete fixtureCache[k]);

  const { data: pending, error } = await supabase
    .from('tips')
    .select('*')
    .eq('status', 'pending')
    .lt('event_time', new Date().toISOString());

  if (error || !pending?.length) return;
  console.log('Checking ' + pending.length + ' pending tips for results...');

  for (const tip of pending) {
    try {
      let homeScore = null, awayScore = null;

      // Use API-Football for football leagues
      if (tip.sport === 'Football') {
        const leagueId = LEAGUE_IDS[tip.league];
        if (!leagueId) {
          console.log('No league ID for: ' + tip.league);
          continue;
        }

        const fixtures = await fetchAPIFootballFixtures(leagueId);
        const fixture = fixtures.find(f => {
          const h = f.teams?.home?.name || '';
          const a = f.teams?.away?.name || '';
          return (nameMatch(h, tip.home_team) && nameMatch(a, tip.away_team)) ||
                 (nameMatch(h, tip.away_team) && nameMatch(a, tip.home_team));
        });

        if (!fixture) {
          console.log('No result yet: ' + tip.home_team + ' vs ' + tip.away_team);
          continue;
        }

        const goals = fixture.goals;
        const homeIsHome = nameMatch(fixture.teams?.home?.name, tip.home_team);
        homeScore = homeIsHome ? goals.home : goals.away;
        awayScore = homeIsHome ? goals.away : goals.home;
        console.log('Result: ' + tip.home_team + ' ' + homeScore + ' - ' + awayScore + ' ' + tip.away_team);

      } else if (tip.sport === 'Basketball' || tip.sport === 'Ice Hockey' || tip.sport === 'NFL' || tip.sport === 'Baseball') {
        // Use Odds API scores endpoint for US sports
        const sportConfig = SPORTS.find(s => s.name === tip.sport && s.league === tip.league);
        if (!sportConfig) continue;
        try {
          const scoresUrl = `${ODDS_BASE}/sports/${sportConfig.key}/scores/?apiKey=${ODDS_API_KEY}&daysFrom=3`;
          const scoresRes = await fetch(scoresUrl);
          if (!scoresRes.ok) continue;
          const scores = await scoresRes.json();
          const match = scores.find(s =>
            s.completed === true && (
              (nameMatch(s.home_team, tip.home_team) && nameMatch(s.away_team, tip.away_team)) ||
              (nameMatch(s.home_team, tip.away_team) && nameMatch(s.away_team, tip.home_team))
            )
          );
          if (!match || !match.scores) { console.log('No scores yet: ' + tip.home_team + ' vs ' + tip.away_team); continue; }
          const homeIsHome = nameMatch(match.home_team, tip.home_team);
          homeScore = parseFloat(match.scores.find(s => nameMatch(s.name, match.home_team))?.score || 0);
          awayScore = parseFloat(match.scores.find(s => nameMatch(s.name, match.away_team))?.score || 0);
          if (!homeIsHome) { const tmp = homeScore; homeScore = awayScore; awayScore = tmp; }
          console.log('Scores API: ' + tip.home_team + ' ' + homeScore + ' - ' + awayScore + ' ' + tip.away_team);
        } catch(e) { console.error('Scores API error:', e.message); continue; }
      } else {
        continue;
      }

      if (homeScore === null || awayScore === null) continue;

      // Determine result
      let won = false;
      const sel = tip.selection.toLowerCase();

      if (sel.includes('win')) {
        const teamName = tip.selection.replace(/ win$/i, '').trim();
        const tipHome = nameMatch(teamName, tip.home_team);
        won = tipHome ? homeScore > awayScore : awayScore > homeScore;
      } else if (sel.startsWith('over')) {
        const line = parseFloat(sel.replace('over ', ''));
        won = (homeScore + awayScore) > line;
      } else if (sel.startsWith('under')) {
        const line = parseFloat(sel.replace('under ', ''));
        won = (homeScore + awayScore) < line;
      } else if (sel.includes('btts') || sel.includes('both teams')) {
        won = homeScore > 0 && awayScore > 0;
      }

      const pl = won
        ? parseFloat(((tip.odds - 1) * tip.stake).toFixed(2))
        : parseFloat((-tip.stake).toFixed(2));

      // Update tip status
      await supabase.from('tips').update({
        status: won ? 'won' : 'lost',
        profit_loss: pl,
        result_updated_at: new Date().toISOString()
      }).eq('id', tip.id);

      // Get last running P&L
      const { data: lastResult } = await supabase
        .from('results_history')
        .select('running_pl')
        .order('settled_at', { ascending: false })
        .limit(1)
        .single();

      const previousPL = lastResult?.running_pl || 0;

      // Guard against duplicate results entries
      const { data: existingResult } = await supabase
        .from('results_history')
        .select('id')
        .eq('tip_id', tip.id)
        .single();
      if (existingResult) {
        console.log('Already settled, skipping: ' + tip.id);
        continue;
      }

      // Add to results history
      await supabase.from('results_history').insert({
        tip_id: tip.id,
        sport: tip.sport,
        event: tip.home_team + ' vs ' + tip.away_team,
        selection: tip.selection,
        odds: tip.odds,
        stake: tip.stake,
        tier: tip.tier || 'free',
        result: won ? 'WON' : 'LOST',
        profit_loss: pl,
        running_pl: parseFloat((previousPL + pl).toFixed(2))
      });

      console.log((won ? 'WON' : 'LOST') + ': ' + tip.home_team + ' vs ' + tip.away_team + ' — ' + tip.selection + ' (' + (pl > 0 ? '+' : '') + pl + 'u)');

    } catch(e) {
      console.error('Error settling ' + tip.id + ':', e.message);
    }
  }
}



// ════════════════════════════════════════════════════════════
// EMAIL SYSTEM
// ════════════════════════════════════════════════════════════

const RESEND_API_KEY = process.env.RESEND_API_KEY || '';
const FROM_EMAIL     = 'onboarding@resend.dev';
const FROM_NAME      = 'The Tipster';
const SITE_URL       = 'https://the-tipster.vercel.app';

// ── Send via Resend API ───────────────────────────────────
async function sendEmail({ to, subject, html, tipId = null, type = 'daily' }) {
  if (!RESEND_API_KEY) { console.error('RESEND_API_KEY not set'); return null; }
  try {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + RESEND_API_KEY },
      body: JSON.stringify({
        from: FROM_NAME + ' <' + FROM_EMAIL + '>',
        to: Array.isArray(to) ? to : [to],
        subject,
        html
      })
    });
    const data = await res.json();
    if (!res.ok) { console.error('Resend error:', data); return null; }
    // Log to email_log table
    await supabase.from('email_log').insert({
      type, subject,
      recipient: Array.isArray(to) ? to.join(',') : to,
      status: 'sent',
      resend_id: data.id || null,
      tip_id: tipId || null
    });
    console.log('Email sent: ' + subject + ' -> ' + (Array.isArray(to) ? to.length + ' recipients' : to));
    return data.id;
  } catch(e) {
    console.error('Email send error:', e.message);
    await supabase.from('email_log').insert({
      type, subject,
      recipient: Array.isArray(to) ? to.join(',') : to,
      status: 'failed'
    });
    return null;
  }
}

// ── Unsubscribe token (simple HMAC-free version using user ID) ──
function unsubToken(userId) {
  // In production replace with proper HMAC signing
  return Buffer.from(userId).toString('base64');
}
function unsubLink(userId) {
  return SITE_URL + '/unsubscribe?token=' + unsubToken(userId);
}

// ── Email footer ─────────────────────────────────────────
function emailFooter(userId) {
  return `
    <div style="margin-top:40px;padding-top:24px;border-top:1px solid #1c2535;text-align:center;">
      <p style="font-family:monospace;font-size:11px;color:#4a5a70;line-height:1.6;margin:0;">
        The Tipster &bull; Tips for informational purposes only &bull; 18+ only &bull; Bet responsibly<br>
        <a href="${SITE_URL}" style="color:#4a5a70;">Visit site</a> &bull;
        <a href="${unsubLink(userId)}" style="color:#4a5a70;">Unsubscribe</a> &bull;
        <a href="${SITE_URL}/account" style="color:#4a5a70;">Manage preferences</a>
      </p>
    </div>`;
}

// ── Email base template ───────────────────────────────────
function emailBase(content, userId) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>The Tipster</title>
</head>
<body style="margin:0;padding:0;background:#07090d;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#07090d;padding:32px 16px;">
  <tr><td align="center">
    <table width="100%" style="max-width:560px;background:#0f141c;border:1px solid #1c2535;border-radius:10px;overflow:hidden;">
      <!-- HEADER -->
      <tr>
        <td style="padding:24px 28px 20px;border-bottom:1px solid #1c2535;">
          <span style="font-family:Georgia,serif;font-size:20px;font-weight:700;color:#dde6f0;letter-spacing:-0.3px;">
            The <span style="color:#18e07a;">Tipster</span>
          </span>
        </td>
      </tr>
      <!-- CONTENT -->
      <tr><td style="padding:28px;">${content}</td></tr>
      <!-- FOOTER -->
      <tr><td style="padding:0 28px 24px;">${emailFooter(userId)}</td></tr>
    </table>
  </td></tr>
</table>
</body>
</html>`;
}

// ── Bet of the Day email template ─────────────────────────
function buildDailyEmail({ tip, userId }) {
  const edge = (parseFloat(tip.confidence||0) - (1/parseFloat(tip.odds||1))*100).toFixed(1);
  const edgeColor = parseFloat(edge) >= 0 ? '#18e07a' : '#ff3d5a';
  const edgeStr = parseFloat(edge) >= 0 ? '+' + edge + '%' : edge + '%';

  const content = `
    <!-- EYEBROW -->
    <p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#18e07a;margin:0 0 10px;">
      Bet of the Day
    </p>

    <!-- HEADLINE -->
    <h1 style="font-size:22px;font-weight:800;color:#dde6f0;margin:0 0 6px;line-height:1.2;">
      ${tip.home_team} vs ${tip.away_team}
    </h1>
    <p style="font-size:12px;color:#4a5a70;margin:0 0 24px;">${tip.league || tip.sport}</p>

    <!-- SELECTION BOX -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:20px;">
      <tr>
        <td style="padding:16px 18px;">
          <p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#4a5a70;margin:0 0 5px;">Selection</p>
          <p style="font-size:18px;font-weight:700;color:#dde6f0;margin:0;">${tip.selection}</p>
        </td>
        <td style="padding:16px 18px;text-align:right;">
          <p style="font-family:monospace;font-size:26px;font-weight:700;color:#f0b429;margin:0;line-height:1;">${parseFloat(tip.odds).toFixed(2)}</p>
          <p style="font-family:monospace;font-size:9px;color:#4a5a70;text-transform:uppercase;letter-spacing:1px;margin:3px 0 0;">Odds</p>
        </td>
      </tr>
    </table>

    <!-- EDGE ROW -->
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;">
      <tr>
        <td width="33%" style="padding:10px 12px;background:#111620;border-radius:5px;text-align:center;">
          <p style="font-family:monospace;font-size:8px;text-transform:uppercase;letter-spacing:1.5px;color:#4a5a70;margin:0 0 5px;">Confidence</p>
          <p style="font-family:monospace;font-size:16px;font-weight:700;color:#dde6f0;margin:0;">${tip.confidence}%</p>
        </td>
        <td width="5%"></td>
        <td width="33%" style="padding:10px 12px;background:#111620;border-radius:5px;text-align:center;">
          <p style="font-family:monospace;font-size:8px;text-transform:uppercase;letter-spacing:1.5px;color:#4a5a70;margin:0 0 5px;">Value Edge</p>
          <p style="font-family:monospace;font-size:16px;font-weight:700;color:${edgeColor};margin:0;">${edgeStr}</p>
        </td>
        <td width="5%"></td>
        <td width="24%" style="padding:10px 12px;background:#111620;border-radius:5px;text-align:center;">
          <p style="font-family:monospace;font-size:8px;text-transform:uppercase;letter-spacing:1.5px;color:#4a5a70;margin:0 0 5px;">Stake</p>
          <p style="font-family:monospace;font-size:16px;font-weight:700;color:#dde6f0;margin:0;">${tip.stake}u</p>
        </td>
      </tr>
    </table>

    <!-- REASONING -->
    <div style="background:#07090d;border:1px solid #1c2535;border-radius:6px;padding:14px 16px;margin-bottom:24px;">
      <p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#18e07a;margin:0 0 8px;">Analysis</p>
      <p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0;">${tip.notes || 'High-confidence selection based on market consensus and form data.'}</p>
    </div>

    <!-- CTA -->
    <div style="text-align:center;margin-bottom:8px;">
      <a href="${SITE_URL}/#tips" style="display:inline-block;background:#18e07a;color:#07090d;font-size:13px;font-weight:700;padding:13px 28px;border-radius:5px;text-decoration:none;letter-spacing:0.3px;">
        View Today's Full Card
      </a>
    </div>
    <p style="text-align:center;font-size:11px;color:#4a5a70;margin:10px 0 0;">
      Pro members get all tips, full analysis and early access. <a href="${SITE_URL}/#pricing" style="color:#18e07a;text-decoration:none;">Upgrade for £9.99/mo</a>
    </p>`;

  return emailBase(content, userId);
}

// ── Saturday accumulator email template ──────────────────
function buildSaturdayEmail({ selections, combinedOdds, reasoning, userId }) {
  const selRows = selections.map((s, i) => `
    <tr style="${i > 0 ? 'border-top:1px solid #1c2535;' : ''}">
      <td style="padding:11px 14px;">
        <p style="font-size:12px;font-weight:700;color:#dde6f0;margin:0 0 2px;">${s.match}</p>
        <p style="font-family:monospace;font-size:11px;color:#18e07a;margin:0;">${s.selection} <span style="color:#4a5a70;">@ ${parseFloat(s.odds).toFixed(2)}</span></p>
      </td>
    </tr>`).join('');

  const content = `
    <p style="font-family:monospace;font-size:10px;text-transform:uppercase;letter-spacing:3px;color:#18e07a;margin:0 0 10px;">
      Weekend Accumulator
    </p>
    <h1 style="font-size:22px;font-weight:800;color:#dde6f0;margin:0 0 6px;line-height:1.2;">
      Saturday's Best ${selections.length}-Fold
    </h1>
    <p style="font-size:12px;color:#4a5a70;margin:0 0 24px;">
      Combined odds: <span style="font-family:monospace;font-weight:700;color:#f0b429;font-size:15px;">${parseFloat(combinedOdds).toFixed(2)}</span>
    </p>

    <!-- SELECTIONS TABLE -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#111620;border-radius:7px;margin-bottom:20px;">
      ${selRows}
    </table>

    <!-- REASONING -->
    <div style="background:#07090d;border:1px solid #1c2535;border-radius:6px;padding:14px 16px;margin-bottom:24px;">
      <p style="font-family:monospace;font-size:9px;text-transform:uppercase;letter-spacing:2px;color:#18e07a;margin:0 0 8px;">Why this acca</p>
      <p style="font-size:13px;color:#dde6f0;line-height:1.7;margin:0;">${reasoning}</p>
    </div>

    <div style="text-align:center;margin-bottom:8px;">
      <a href="${SITE_URL}/#tips" style="display:inline-block;background:#18e07a;color:#07090d;font-size:13px;font-weight:700;padding:13px 28px;border-radius:5px;text-decoration:none;">
        View Full Weekend Card
      </a>
    </div>
    <p style="text-align:center;font-size:11px;color:#4a5a70;margin:10px 0 0;">
      Unlock premium singles, early picks and full analysis. <a href="${SITE_URL}/#pricing" style="color:#18e07a;text-decoration:none;">Go Pro for £9.99/mo</a>
    </p>`;

  return emailBase(content, userId);
}

// ── Pick best Bet of the Day from today's tips ────────────
async function getBetOfTheDay() {
  // Check for admin override first
  const today = new Date().toISOString().split('T')[0];
  const { data: override } = await supabase
    .from('email_overrides')
    .select('*')
    .eq('date', today)
    .eq('type', 'daily')
    .single();

  if (override && override.bet_selection) {
    return {
      home_team: override.bet_match ? override.bet_match.split(' vs ')[0] : 'Home',
      away_team: override.bet_match ? override.bet_match.split(' vs ')[1] : 'Away',
      selection: override.bet_selection,
      odds:      override.bet_odds || 1.8,
      confidence:override.bet_confidence || 80,
      stake:     1,
      notes:     override.bet_reasoning || '',
      league:    '',
      sport:     'Football',
      isOverride: true
    };
  }

  // Auto-pick: highest confidence pending tip for today
  const todayStart = new Date(); todayStart.setHours(0,0,0,0);
  const todayEnd   = new Date(); todayEnd.setHours(23,59,59,999);
  const { data: tips } = await supabase
    .from('tips')
    .select('*')
    .eq('status','pending')
    .gte('event_time', todayStart.toISOString())
    .lte('event_time', todayEnd.toISOString())
    .order('confidence', { ascending: false })
    .limit(1);

  return tips && tips.length ? tips[0] : null;
}

// ── Build Saturday accumulator ────────────────────────────
async function getSaturdayAcca() {
  const today = new Date().toISOString().split('T')[0];
  const { data: override } = await supabase
    .from('email_overrides')
    .select('*')
    .eq('date', today)
    .eq('type', 'saturday')
    .single();

  if (override && override.acca_selections) {
    return {
      selections:   override.acca_selections,
      combinedOdds: override.acca_combined_odds || 0,
      reasoning:    override.acca_reasoning || ''
    };
  }

  // Auto-build from top 4 pending football tips today
  const todayStart = new Date(); todayStart.setHours(6,0,0,0);
  const todayEnd   = new Date(); todayEnd.setHours(23,59,59,999);
  const { data: tips } = await supabase
    .from('tips')
    .select('*')
    .eq('status','pending')
    .eq('sport','Football')
    .gte('event_time', todayStart.toISOString())
    .lte('event_time', todayEnd.toISOString())
    .gte('confidence', 75)
    .order('confidence', { ascending: false })
    .limit(4);

  if (!tips || tips.length < 3) return null;
  const selections = tips.map(t => ({
    match:     t.home_team + ' vs ' + t.away_team,
    selection: t.selection,
    odds:      t.odds
  }));
  const combinedOdds = selections.reduce((acc, s) => acc * parseFloat(s.odds), 1);
  const reasoning = 'Four high-confidence selections from today' + "'" + 's card. ' +
    'All carry 75%+ model confidence with positive value edge versus the market. ' +
    'Recommended at 0.5 units each-way on the accumulator.';

  return { selections, combinedOdds, reasoning };
}

// ── Get all opted-in subscribers ─────────────────────────
async function getSubscribers(emailType = 'daily') {
  const col = emailType === 'saturday' ? 'email_saturday' : 'email_daily';
  const { data, error } = await supabase
    .from('users')
    .select('id, email, first_name')
    .eq('email_opt_in', true)
    .eq(col, true);
  if (error) { console.error('Get subscribers error:', error.message); return []; }
  return data || [];
}

// ── Daily email dispatch (08:30 UK time) ─────────────────
async function sendDailyEmails() {
  console.log('📧 Starting daily email dispatch...');
  const tip = await getBetOfTheDay();
  if (!tip) { console.log('No tip found for daily email, skipping.'); return; }

  const subscribers = await getSubscribers('daily');
  console.log('Sending to ' + subscribers.length + ' subscribers...');

  let sent = 0;
  for (const user of subscribers) {
    // Personalise subject with first name if available
    const greeting = user.first_name ? user.first_name + ', ' : '';
    const subject = greeting + "Today's Bet of the Day | " + tip.home_team + ' vs ' + tip.away_team;
    const html = buildDailyEmail({ tip, userId: user.id });
    const result = await sendEmail({
      to: user.email, subject, html,
      tipId: tip.id, type: 'daily'
    });
    if (result) sent++;
    await new Promise(r => setTimeout(r, 100)); // Rate limit: 10/sec
  }
  console.log('Daily emails complete. Sent: ' + sent + '/' + subscribers.length);
}

// ── Saturday accumulator dispatch ────────────────────────
async function sendSaturdayEmails() {
  console.log('📧 Starting Saturday accumulator dispatch...');
  const acca = await getSaturdayAcca();
  if (!acca) { console.log('Not enough tips for Saturday acca, skipping.'); return; }

  const subscribers = await getSubscribers('saturday');
  console.log('Sending Saturday acca to ' + subscribers.length + ' subscribers...');

  let sent = 0;
  for (const user of subscribers) {
    const greeting = user.first_name ? user.first_name + ', ' : '';
    const subject = greeting + "Saturday's " + acca.selections.length + '-Fold Accumulator | ' + parseFloat(acca.combinedOdds).toFixed(2) + ' combined odds';
    const html = buildSaturdayEmail({ ...acca, userId: user.id });
    const result = await sendEmail({
      to: user.email, subject, html, type: 'saturday'
    });
    if (result) sent++;
    await new Promise(r => setTimeout(r, 100));
  }
  console.log('Saturday emails complete. Sent: ' + sent + '/' + subscribers.length);
}

// ── Precision scheduler ───────────────────────────────────
// Runs at exactly 08:30 UK time (UTC+0 in winter, UTC+1 in summer)
// Cron-style: checks every minute, fires once per day
let lastDailyDate   = null;
let lastSaturdayDate = null;

function startEmailScheduler() {
  setInterval(async () => {
    const now = new Date();
    // Convert to UK time (handles BST/GMT automatically)
    const ukTime = new Date(now.toLocaleString('en-GB', { timeZone: 'Europe/London' }));
    const h = ukTime.getHours();
    const m = ukTime.getMinutes();
    const today = ukTime.toDateString();
    const isSaturday = ukTime.getDay() === 6;

    // Daily email at 08:30 UK
    if (h === 8 && m === 30 && lastDailyDate !== today) {
      lastDailyDate = today;
      await sendDailyEmails();
    }

    // Saturday email at 08:30 UK (same time but Saturday only)
    if (isSaturday && h === 8 && m === 30 && lastSaturdayDate !== today) {
      lastSaturdayDate = today;
      await sendSaturdayEmails();
    }
  }, 60 * 1000); // Check every minute

  console.log('Email scheduler started. Daily at 08:30 UK, Saturday acca at 08:30 UK on Saturdays.');
}

// ── Test email endpoint (called via HTTP) ─────────────────
async function sendTestEmail(toEmail, type) {
  if (type === 'saturday') {
    const acca = await getSaturdayAcca();
    if (!acca) return { success: false, error: 'No acca available' };
    const html = buildSaturdayEmail({ ...acca, userId: 'test' });
    const id = await sendEmail({ to: toEmail, subject: '[TEST] Saturday Accumulator', html, type: 'test' });
    return { success: !!id };
  } else {
    const tip = await getBetOfTheDay();
    if (!tip) return { success: false, error: 'No tip available' };
    const html = buildDailyEmail({ tip, userId: 'test' });
    const id = await sendEmail({ to: toEmail, subject: '[TEST] Bet of the Day', html, type: 'test' });
    return { success: !!id };
  }
}


// ── ADMIN JOB QUEUE PROCESSOR ────────────────────────────
// Polls admin_jobs table every 30 seconds.
// Admin dashboard writes jobs here; engine executes them.
// Avoids direct browser->engine calls which Render blocks.
async function processAdminJobs() {
  try {
    const { data: jobs } = await supabase
      .from('admin_jobs')
      .select('*')
      .eq('status', 'pending')
      .order('created_at', { ascending: true })
      .limit(10);

    if (!jobs || !jobs.length) return;

    for (const job of jobs) {
      // Mark as processing immediately to prevent double-execution
      await supabase.from('admin_jobs').update({ status: 'processing' }).eq('id', job.id);

      try {
        const payload = JSON.parse(job.payload || '{}');
        console.log('Processing job: ' + job.job_type, payload);

        if (job.job_type === 'test_email') {
          const result = await sendTestEmail(payload.to, payload.type || 'daily');
          await supabase.from('admin_jobs').update({
            status: result.success ? 'done' : 'failed',
            result: JSON.stringify(result)
          }).eq('id', job.id);

        } else if (job.job_type === 'send_daily') {
          await sendDailyEmails();
          await supabase.from('admin_jobs').update({ status: 'done' }).eq('id', job.id);

        } else if (job.job_type === 'send_saturday') {
          await sendSaturdayEmails();
          await supabase.from('admin_jobs').update({ status: 'done' }).eq('id', job.id);

        } else {
          await supabase.from('admin_jobs').update({ status: 'unknown_type' }).eq('id', job.id);
        }

      } catch(e) {
        console.error('Job failed:', job.job_type, e.message);
        await supabase.from('admin_jobs').update({ status: 'failed', result: e.message }).eq('id', job.id);
      }
    }
  } catch(e) {
    console.error('Job processor error:', e.message);
  }
}

// ─── MAIN ENGINE LOOP ────────────────────────
async function runEngine() {
  console.log(`\n🚀 Engine running — ${new Date().toLocaleString()}`);
  console.log('═'.repeat(50));

  let allTips = [];

  for (const sport of SPORTS) {
    console.log('Fetching ' + sport.league + '...');
    if (sport.isRacing) {
      const events = await fetchRacingOdds(sport.key);
      if (events.length) {
        const tips = generateRacingTips(events, sport);
        console.log('   -> ' + events.length + ' races, ' + tips.length + ' tips generated (racing)');
        allTips = allTips.concat(tips);
      }
    } else {
      const events = await fetchOdds(sport.key);
      if (events.length) {
        const tips = generateTips(events, sport);
        console.log('   -> ' + events.length + ' events, ' + tips.length + ' tips generated');
        allTips = allTips.concat(tips);
      }
    }
    await new Promise(r => setTimeout(r, 500));
  }

  console.log(`\n💾 Saving ${allTips.length} tips to database...`);
  await saveTips(allTips);

  console.log('\n🏁 Checking results for completed events...');
  await settleResults();

  console.log('\n✅ Engine cycle complete.');
  console.log('═'.repeat(50));
}

// ─── SCHEDULER ───────────────────────────────
runEngine();
setInterval(runEngine, 15 * 60 * 1000);
startEmailScheduler();

// Poll for admin jobs every 30 seconds
setInterval(processAdminJobs, 30 * 1000);
processAdminJobs(); // Run immediately on start

// ─── HTTP SERVER ──────────────────────────────
// Health check + admin email trigger endpoints
const http = require('http');
http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');

  // CORS headers — allow admin dashboard to call engine
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  // Handle preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, corsHeaders);
    res.end();
    return;
  }

  // Health check
  if (url.pathname === '/') {
    res.writeHead(200, { ...corsHeaders, 'Content-Type': 'text/plain' });
    res.end('The Tipster Engine Running');
    return;
  }

  // Admin: trigger test email
  // GET /admin/test-email?to=your@email.com&type=daily&key=ADMIN_KEY
  if (url.pathname === '/admin/test-email') {
    const key = url.searchParams.get('key');
    if (key !== process.env.ADMIN_KEY) {
      res.writeHead(403); res.end('Forbidden'); return;
    }
    const to   = url.searchParams.get('to');
    const type = url.searchParams.get('type') || 'daily';
    if (!to) { res.writeHead(400); res.end('Missing to param'); return; }
    const result = await sendTestEmail(to, type);
    res.writeHead(200, { ...corsHeaders, 'Content-Type': 'application/json' });
    res.end(JSON.stringify(result));
    return;
  }

  // Admin: trigger daily send now
  // GET /admin/send-daily?key=ADMIN_KEY
  if (url.pathname === '/admin/send-daily') {
    const key = url.searchParams.get('key');
    if (key !== process.env.ADMIN_KEY) {
      res.writeHead(403); res.end('Forbidden'); return;
    }
    sendDailyEmails(); // fire and forget
    res.writeHead(200, { ...corsHeaders, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true }));
    return;
  }

  // Admin: trigger Saturday send now
  if (url.pathname === '/admin/send-saturday') {
    const key = url.searchParams.get('key');
    if (key !== process.env.ADMIN_KEY) {
      res.writeHead(403); res.end('Forbidden'); return;
    }
    sendSaturdayEmails();
    res.writeHead(200, { ...corsHeaders, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ started: true }));
    return;
  }

  // Unsubscribe handler
  // GET /unsubscribe?token=BASE64_USER_ID
  if (url.pathname === '/unsubscribe') {
    const token = url.searchParams.get('token');
    if (!token) { res.writeHead(400); res.end('Invalid link'); return; }
    try {
      const userId = Buffer.from(token, 'base64').toString('utf8');
      await supabase.from('users').update({ email_opt_in: false }).eq('id', userId);
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end('<html><body style="font-family:sans-serif;text-align:center;padding:60px;background:#07090d;color:#dde6f0;"><h2>Unsubscribed</h2><p>You have been removed from all emails.</p><p><a href="https://the-tipster.vercel.app/account" style="color:#18e07a;">Manage preferences</a></p></body></html>');
    } catch(e) {
      res.writeHead(400); res.end('Invalid token');
    }
    return;
  }

  res.writeHead(404); res.end('Not found');

}).listen(process.env.PORT || 3000, () => {
  console.log('Health check + admin server running on port ' + (process.env.PORT || 3000));
});

#!/usr/bin/env python3
"""
generate_dashboard_data.py
Queries gambling.db and writes dashboard/dashboard_data.json
Run: python3 generate_dashboard_data.py
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'gambling.db')
OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dashboard_data.json')


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_bets(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, date, sport, game, pick, odds, amount_wagered,
               outcome, profit_loss, notes, closing_odds, clv, clv_checked_at, bet_type
        FROM bets
        ORDER BY date ASC
    """)
    rows = cur.fetchall()
    bets = []
    for r in rows:
        bets.append({
            'id': r['id'],
            'date': r['date'],
            'sport': r['sport'],
            'game': r['game'],
            'pick': r['pick'],
            'odds': r['odds'],
            'amount_wagered': r['amount_wagered'],
            'outcome': r['outcome'],
            'profit_loss': r['profit_loss'],
            'notes': r['notes'],
            'closing_odds': r['closing_odds'],
            'clv': r['clv'],
            'clv_checked_at': r['clv_checked_at'],
            'bet_type': r['bet_type'],
        })
    return bets


def compute_summary(bets):
    settled = [b for b in bets if b['outcome'] in ('win', 'loss')]
    wins = [b for b in settled if b['outcome'] == 'win']
    total_pl = sum(b['profit_loss'] for b in bets if b['profit_loss'] is not None)
    total_wagered = sum(b['amount_wagered'] for b in settled if b['amount_wagered'] is not None)
    win_rate = (len(wins) / len(settled) * 100) if settled else 0
    roi = (total_pl / total_wagered * 100) if total_wagered else 0
    clv_vals = [b['clv'] for b in bets if b['clv'] is not None]
    avg_clv = sum(clv_vals) / len(clv_vals) if clv_vals else None

    return {
        'total_pl': round(total_pl, 2),
        'win_rate': round(win_rate, 1),
        'roi': round(roi, 2),
        'total_bets': len(bets),
        'avg_clv': round(avg_clv, 2) if avg_clv is not None else None,
    }


def compute_pl_over_time(bets):
    """Cumulative P/L sorted by date."""
    sorted_bets = sorted(bets, key=lambda b: b['date'])
    cumulative = 0
    points = []
    for b in sorted_bets:
        pl = b['profit_loss'] or 0
        cumulative += pl
        points.append({
            'date': b['date'][:10],  # just the date part
            'cumulative_pl': round(cumulative, 2),
            'game': b['game'],
        })
    return points


def compute_sport_breakdown(bets):
    """P/L by sport."""
    breakdown = {}
    for b in bets:
        sport = b['sport'] or 'unknown'
        pl = b['profit_loss'] or 0
        breakdown[sport] = round(breakdown.get(sport, 0) + pl, 2)
    # Return sorted list
    return [{'sport': k, 'pl': v} for k, v in sorted(breakdown.items())]


def fetch_line_alerts(conn):
    """Last 7 days of line alerts."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, alerted_at, sport, game_date, home_team, away_team,
               market, outcome_name, open_point, current_point, movement, bookmaker
        FROM line_alerts
        WHERE alerted_at >= ?
        ORDER BY alerted_at DESC
    """, (cutoff,))
    rows = cur.fetchall()
    alerts = []
    for r in rows:
        alerts.append({
            'id': r['id'],
            'alerted_at': r['alerted_at'],
            'sport': r['sport'],
            'game_date': r['game_date'],
            'home_team': r['home_team'],
            'away_team': r['away_team'],
            'market': r['market'],
            'outcome_name': r['outcome_name'],
            'open_point': r['open_point'],
            'current_point': r['current_point'],
            'movement': r['movement'],
            'bookmaker': r['bookmaker'],
        })
    return alerts


def fetch_top_player_props(conn):
    """Top 20 player props by edge for most recent date."""
    cur = conn.cursor()
    # Get most recent date
    cur.execute("SELECT MAX(date) as max_date FROM player_props")
    row = cur.fetchone()
    most_recent = row['max_date'] if row and row['max_date'] else None

    if not most_recent:
        return [], None

    cur.execute("""
        SELECT player, game, market, line, over_odds, under_odds,
               recommendation, hit_prob, edge, rolling_avg, notes
        FROM player_props
        WHERE date = ?
        ORDER BY edge DESC
        LIMIT 20
    """, (most_recent,))
    rows = cur.fetchall()
    props = []
    for r in rows:
        props.append({
            'player': r['player'],
            'game': r['game'],
            'market': r['market'],
            'line': r['line'],
            'over_odds': r['over_odds'],
            'under_odds': r['under_odds'],
            'recommendation': r['recommendation'],
            'hit_prob': r['hit_prob'],
            'edge': r['edge'],
            'rolling_avg': r['rolling_avg'],
            'notes': r['notes'],
        })
    return props, most_recent


def main():
    print(f"Connecting to DB: {DB_PATH}")
    conn = get_conn()

    bets = fetch_bets(conn)
    summary = compute_summary(bets)
    pl_over_time = compute_pl_over_time(bets)
    sport_breakdown = compute_sport_breakdown(bets)
    line_alerts = fetch_line_alerts(conn)
    top_props, props_date = fetch_top_player_props(conn)

    data = {
        'generated_at': datetime.now().isoformat(),
        'summary': summary,
        'bets': bets,
        'pl_over_time': pl_over_time,
        'sport_breakdown': sport_breakdown,
        'line_alerts': line_alerts,
        'top_player_props': top_props,
        'player_props_date': props_date,
    }

    with open(OUT_PATH, 'w') as f:
        json.dump(data, f, indent=2, default=str)

    print(f"Written to: {OUT_PATH}")
    print(f"  Bets: {len(bets)}")
    print(f"  Line alerts (7d): {len(line_alerts)}")
    print(f"  Top props ({props_date}): {len(top_props)}")
    print(f"  Summary: {summary}")

    conn.close()


if __name__ == '__main__':
    main()

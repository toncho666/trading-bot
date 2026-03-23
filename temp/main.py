#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import matplotlib
matplotlib.use('Agg')                         # без GUI‑бекенда
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import base64, io, os, locale, logging
from datetime import datetime, date
from pathlib import Path
from flask import Flask, render_template, request
from sqlalchemy import create_engine, text

# -------------------   CONFIG   -------------------
DB_URL = "postgresql+psycopg2://usen:pass@host:port/db"
ENGINE = create_engine(DB_URL)
app = Flask(__name__, template_folder='templates')

# --------- locale / filter ----------
try:
    locale.setlocale(locale.LC_ALL, "ru_RU.UTF-8")
except locale.Error:
    locale.setlocale(locale.LC_ALL, "")

@app.template_filter('intcomma')
def intcomma(value):
    try:
        return locale.format_string("%d", int(value), grouping=True)
    except (ValueError, TypeError):
        return value

# --------- helpers ----------
def img_to_base64(p: Path) -> str:
    return base64.b64encode(p.read_bytes()).decode() if p.is_file() else ""

def fig_to_base64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=120)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()

# --------- DB ----------
def max_load(tbl):
    with ENGINE.connect() as conn:
        return conn.execute(text(f"SELECT MAX(load_dttm) AS m FROM test.{tbl}")).scalar()

def load_profiles():
    dt = max_load("ig_profile")
    return pd.read_sql(text("SELECT * FROM test.ig_profile WHERE load_dttm = :dt"),
                       ENGINE, params={"dt": dt})

def load_posts():
    dt = max_load("ig_post")
    return pd.read_sql(text("SELECT * FROM test.ig_post WHERE load_dttm = :dt"),
                       ENGINE, params={"dt": dt})

def load_history():
    sql = text("""
        SELECT
            p.load_dttm::date AS dt,
            p.owner_username,
            p.likes_cnt,
            p.comments_cnt,
            p.video_play_cnt,
            p.video_view_cnt,
            pr.followers_cnt
        FROM test.ig_post p
        JOIN test.ig_profile pr ON p.owner_username = pr.username
    """)
    return pd.read_sql(sql, ENGINE)

# --------- KPI & charts ----------
def calc_kpi(profiles, posts):
    profiles = profiles.sort_values('load_dttm')
    last = profiles.iloc[-1]
    prev = profiles.iloc[-2] if len(profiles) >= 2 else last
    followers_change = (last['followers_cnt'] - prev['followers_cnt']) / prev['followers_cnt'] * 100
    total_likes = posts['likes_cnt'].sum()
    merged = posts.merge(
        profiles[['username', 'followers_cnt']].rename(columns={'username': 'owner_username'}),
        on='owner_username', how='left')
    merged['eng'] = (merged['likes_cnt'] + merged['comments_cnt']) / merged['followers_cnt']
    avg_engagement = merged['eng'].mean() * 100
    video = posts[posts['post_type'] == 'Video']
    video_completion = (video['video_view_cnt'].sum() / video['video_play_cnt'].sum() * 100
                        if not video.empty else 0)
    return {
        "followers_change": followers_change,
        "total_likes": total_likes,
        "avg_engagement": avg_engagement,
        "video_completion": video_completion,
    }

def plot_followers(df):
    agg = df.groupby('dt')['followers_cnt'].mean().reset_index()
    fig, ax = plt.subplots(figsize=(7, 2.2))
    sns.lineplot(data=agg, x='dt', y='followers_cnt', marker='o', ax=ax, color='#0D3B66')
    ax.set_title('Средний рост подписчиков', fontsize=11, weight='semibold')
    ax.set_xlabel(''); ax.set_ylabel('')
    plt.tight_layout()
    return fig_to_base64(fig)

def plot_engagement(df):
    df['eng'] = (df['likes_cnt'] + df['comments_cnt']) / df['followers_cnt']
    agg = df.groupby('dt')['eng'].mean().reset_index()
    fig, ax = plt.subplots(figsize=(7, 2.2))
    sns.barplot(data=agg, x='dt', y='eng', color='#0066CC', ax=ax, legend=False)
    ax.set_title('Средний Engagement', fontsize=11, weight='semibold')
    ax.set_xlabel(''); ax.set_ylabel('')
    plt.tight_layout()
    return fig_to_base64(fig)

def plot_video(df):
    v = df[df['video_play_cnt'] > 0].copy()
    if v.empty:
        fig, ax = plt.subplots(figsize=(7, 2.2))
        ax.text(0.5, 0.5, 'Видео‑данные отсутствуют', ha='center', va='center', color='#555')
        plt.axis('off')
        return fig_to_base64(fig)
    v['completion'] = v['video_view_cnt'] / v['video_play_cnt']
    agg = v.groupby('dt')['completion'].mean().reset_index()
    fig, ax = plt.subplots(figsize=(7, 2.2))
    ax.fill_between(agg['dt'], agg['completion'], color='#007AFF', alpha=0.15)
    ax.plot(agg['dt'], agg['completion'], color='#007AFF')
    ax.set_title('Video Completion Rate', fontsize=11, weight='semibold')
    ax.set_xlabel(''); ax.set_ylabel('')
    plt.tight_layout()
    return fig_to_base64(fig)

def top_posts(posts, profiles, n=10):
    merged = posts.merge(
        profiles[['username', 'followers_cnt']].rename(columns={'username': 'owner_username'}),
        on='owner_username', how='left')
    merged['engagement_rate'] = (merged['likes_cnt'] + merged['comments_cnt']) / merged['followers_cnt'] * 100
    top = merged.nlargest(n, 'engagement_rate')
    top['preview'] = ''
    return top.to_dict(orient='records')

def top_profiles(profiles, n=5):
    df = profiles.copy()
    df['avg_engagement'] = 0.0
    top = df.nlargest(n, 'followers_cnt')
    top['avatar'] = ''
    return top.to_dict(orient='records')

# -------------------  Маршрут -------------------


@app.route("/ping")
def ping():
    return "pong"


@app.route("/")
def index():
    try:
        profiles = load_profiles()
        posts    = load_posts()
        history  = load_history()

        kpi = calc_kpi(profiles, posts)

        ctx = {
            "logo_path": img_to_base64(Path('assets') / 'logo.png'),   # ← может быть пустой строкой
            "report_date": date.today().strftime("%d %B %Y"),
            "generation_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "latest_load": max_load('ig_post').strftime("%Y-%m-%d %H:%M"),
            "kpi": kpi,
            "chart_followers": plot_followers(history),
            "chart_engagement": plot_engagement(history),
            "chart_video": plot_video(history),
            "top_posts": top_posts(posts, profiles),
            "top_profiles": top_profiles(profiles),
        }
        return render_template('report.html', **ctx)

    except Exception as exc:
        # Полный traceback в консоль и в браузер (для быстрой отладки)
        import traceback, sys
        traceback.print_exc()
        return f"<pre>{traceback.format_exc()}</pre>", 500
    
    
import logging
logging.basicConfig(level=logging.DEBUG)          # вывод всех сообщений в консоль
app.logger.setLevel(logging.DEBUG)

@app.before_request
def log_request():
    app.logger.debug(f"🔔 Incoming {request.method} {request.path}")

# -------------------  Запуск -------------------
if __name__ == "__main__":
    # Отключаем перезапуск‑watchdog, чтобы процесс не терялся в фоне
    app.run(host="127.0.0.1", port=5050, debug=False, use_reloader=False, threaded=True)

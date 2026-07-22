import os
import time
import json
import requests
import psycopg2
import psycopg2.extras
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor

DATABASE_URL = os.environ.get("DATABASE_URL")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10000))
DB_BATCH_SIZE = 50  # Flush DB writes in batches of 50 games
MAX_WORKERS = 5

# Create a global session to reuse connections (MASSIVE speedup for high-latency networking)
http_session = requests.Session()
adapter = HTTPAdapter(pool_connections=25, pool_maxsize=25)
http_session.mount('https://', adapter)

if not DATABASE_URL:
    print("Error: Missing DATABASE_URL environment variable.")
    exit(1)

# --- SAFETY HELPER FUNCTIONS ---

def safe_int(val, default=0):
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        val = val.strip()
        if val.isdigit() or (val.startswith('-') and val[1:].isdigit()):
            return int(val)
    return default

def safe_str(val, default=None):
    if val is None:
        return default
    if isinstance(val, str):
        val = val.strip()
        return val if val else default
    return str(val)

def safe_date(val):
    return safe_str(val)

# --- INDIVIDUAL API FETCHERS (LOCKED TO USD, ENGLISH, & 429 PROOF) ---

def fetch_store_data(app_id):
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&cc=us&l=english"
    for attempt in range(3):
        try:
            res = http_session.get(url, timeout=7)
            if res.status_code == 429:
                print(f"⚠️ [RATE LIMIT] Store API paused. Sleeping 210s...(3.5mins)")
                time.sleep(210)
                continue
            data = res.json().get(str(app_id), {})
            return data.get("data") if data.get("success") else None
        except Exception as e:
            print(f"⚠️ [NETWORK] Store API error on {app_id}: {type(e).__name__}")
            time.sleep(2)
    return None

def fetch_player_count(app_id):
    url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}"
    for attempt in range(3):
        try:
            res = http_session.get(url, timeout=7)
            if res.status_code == 429:
                print(f"⚠️ [RATE LIMIT] Store API paused. Sleeping 210s...(3.5mins)")
                time.sleep(210)
                continue
            return res.json().get("response", {}).get("player_count")
        except Exception as e:
            print(f"⚠️ [NETWORK] Player API error on {app_id}: {type(e).__name__}")
            time.sleep(2)
    return None

def fetch_steamspy_data(app_id):
    url = f"https://steamspy.com/api.php?request=appdetails&appid={app_id}"
    for attempt in range(3):
        try:
            res = http_session.get(url, timeout=7)
            if res.status_code == 429:
                print(f"⚠️ [RATE LIMIT] Store API paused. Sleeping 210s...(3.5mins)")
                time.sleep(210)
                continue
            return res.json()
        except Exception as e:
            print(f"⚠️ [NETWORK] SteamSpy API error on {app_id}: {type(e).__name__}")
            time.sleep(2)
    return None

def fetch_reviews(app_id):
    url = f"https://store.steampowered.com/appreviews/{app_id}?json=1&language=all&l=english"
    for attempt in range(3):
        try:
            res = http_session.get(url, timeout=7)
            if res.status_code == 429:
                print(f"⚠️ [RATE LIMIT] Store API paused. Sleeping 210s...(3.5mins)")
                time.sleep(210)
                continue
            return res.json().get("query_summary")
        except Exception as e:
            print(f"⚠️ [NETWORK] Review API error on {app_id}: {type(e).__name__}")
            time.sleep(2)
    return None

# --- PARALLEL WORKER ---

def enrich_game(app_id):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_store = executor.submit(fetch_store_data, app_id)
        future_players = executor.submit(fetch_player_count, app_id)
        future_spy = executor.submit(fetch_steamspy_data, app_id)
        future_reviews = executor.submit(fetch_reviews, app_id)

        store_data = future_store.result()
        if not store_data:
            return None

        return {
            "app_id": app_id,
            "store": store_data,
            "players": future_players.result(),
            "spy": future_spy.result() or {},
            "reviews": future_reviews.result() or {}
        }

# --- BULK DATABASE SAVER ---

def save_game_metrics_batch(conn, cursor, payloads, skipped_app_ids):
    if skipped_app_ids:
        cursor.execute(
            "UPDATE games SET metrics_updated_at = NOW() WHERE app_id = ANY(%s);",
            (list(skipped_app_ids),)
        )

    if not payloads:
        conn.commit()
        return

    games_update_data = []
    company_roles_all = []
    tags_all = []
    metrics_data = []
    app_ids_in_batch = []

    for payload in payloads:
        app_id = payload["app_id"]
        app_ids_in_batch.append(app_id)
        
        store = payload["store"]
        spy = payload["spy"]
        reviews = payload["reviews"]
        
        release_data = store.get("release_date") or {}
        price_overview = store.get("price_overview", {})
        metacritic = store.get("metacritic", {})
        platforms = store.get("platforms", {})
        
        game_type = safe_str(store.get("type"), "game")
        is_free = bool(store.get("is_free", False))
        required_age = safe_int(store.get("required_age"), 0)
        short_desc = safe_str(store.get("short_description"))
        release_date_str = safe_date(release_data.get("date"))
        base_price = safe_int(price_overview.get("initial"), 0)
        controller_support = safe_str(store.get("controller_support"), "none")
        metacritic_score = safe_int(metacritic.get("score"), 0)
        
        languages = store.get("supported_languages")
        languages_json_str = json.dumps(languages) if languages else None
        
        mac_support = bool(platforms.get("mac", False))
        linux_support = bool(platforms.get("linux", False))

        games_update_data.append((
            game_type, is_free, required_age, short_desc, release_date_str,
            base_price, controller_support, metacritic_score, languages_json_str,
            mac_support, linux_support, app_id
        ))

        developers = store.get("developers", [])
        publishers = store.get("publishers", [])
        if isinstance(developers, list):
            for dev in developers:
                if dev and isinstance(dev, str):
                    company_roles_all.append((app_id, dev.strip(), "developer"))
        if isinstance(publishers, list):
            for pub in publishers:
                if pub and isinstance(pub, str):
                    company_roles_all.append((app_id, pub.strip(), "publisher"))

        spy_tags = spy.get("tags", {})
        if isinstance(spy_tags, dict) and spy_tags:
            clean_spy_tags = {k.strip(): v for k, v in spy_tags.items() if k and isinstance(k, str)}
            for t_name, votes in clean_spy_tags.items():
                tags_all.append((app_id, t_name, safe_int(votes, 0)))

        live_players = safe_int(payload["players"], 0)
        owners_raw = spy.get("owners", "0 .. 0")
        owners_parts = owners_raw.replace(",", "").split("..") if isinstance(owners_raw, str) else ["0", "0"]
            
        min_owners = safe_int(owners_parts[0] if len(owners_parts) > 0 else 0, 0)
        max_owners = safe_int(owners_parts[1] if len(owners_parts) > 1 else 0, 0)

        current_price = safe_int(price_overview.get("final"), base_price)
        discount_pct = safe_int(price_overview.get("discount_percent"), 0)
        pos_reviews = safe_int(reviews.get("total_positive"), 0)
        neg_reviews = safe_int(reviews.get("total_negative"), 0)
        review_desc = safe_str(reviews.get("review_score_desc"))
        avg_playtime = safe_int(spy.get("average_2weeks"), 0)
        med_playtime = safe_int(spy.get("median_2weeks"), 0)
        ccu = safe_int(spy.get("ccu"), 0)

        metrics_data.append((
            app_id, current_price, discount_pct, live_players,
            pos_reviews, neg_reviews, review_desc, min_owners,
            max_owners, avg_playtime, med_playtime, ccu
        ))

    if games_update_data:
        update_games_sql = """
            UPDATE games AS g SET
                type = v.type,
                is_free = v.is_free::boolean,
                required_age = v.required_age::int,
                short_description = v.short_description,
                release_date = v.release_date,
                base_price = v.base_price::int,
                controller_support = v.controller_support,
                metacritic_score = v.metacritic_score::int,
                supported_languages = v.supported_languages::jsonb,
                mac_support = v.mac_support::boolean,
                linux_support = v.linux_support::boolean,
                updated_at = NOW(),
                metrics_updated_at = NOW()
            FROM (VALUES %s) AS v(
                type, is_free, required_age, short_description, release_date,
                base_price, controller_support, metacritic_score, supported_languages,
                mac_support, linux_support, app_id
            )
            WHERE g.app_id = v.app_id::int;
        """
        psycopg2.extras.execute_values(cursor, update_games_sql, games_update_data)

    if company_roles_all:
        unique_comp_names = list(set([c[1] for c in company_roles_all]))
        psycopg2.extras.execute_values(
            cursor,
            "INSERT INTO companies (name) VALUES %s ON CONFLICT (name) DO NOTHING;",
            [(c,) for c in unique_comp_names]
        )
        cursor.execute("SELECT company_id, name FROM companies WHERE name = ANY(%s);", (unique_comp_names,))
        comp_map = {row[1]: row[0] for row in cursor.fetchall()}

        cursor.execute("DELETE FROM game_companies WHERE app_id = ANY(%s);", (app_ids_in_batch,))
        game_comp_rows = [(app_id, comp_map[c_name], role) for app_id, c_name, role in company_roles_all if c_name in comp_map]
        if game_comp_rows:
            psycopg2.extras.execute_values(
                cursor,
                "INSERT INTO game_companies (app_id, company_id, role_type) VALUES %s ON CONFLICT DO NOTHING;",
                game_comp_rows
            )

    if tags_all:
        unique_tag_names = list(set([t[1] for t in tags_all]))
        psycopg2.extras.execute_values(
            cursor,
            "INSERT INTO tags (name) VALUES %s ON CONFLICT (name) DO NOTHING;",
            [(t,) for t in unique_tag_names]
        )
        cursor.execute("SELECT tag_id, name FROM tags WHERE name = ANY(%s);", (unique_tag_names,))
        tag_map = {row[1]: row[0] for row in cursor.fetchall()}

        cursor.execute("DELETE FROM game_tags WHERE app_id = ANY(%s);", (app_ids_in_batch,))
        game_tag_rows = [(app_id, tag_map[t_name], votes) for app_id, t_name, votes in tags_all if t_name in tag_map]
        if game_tag_rows:
            psycopg2.extras.execute_values(
                cursor,
                "INSERT INTO game_tags (app_id, tag_id, votes) VALUES %s ON CONFLICT DO NOTHING;",
                game_tag_rows
            )

    if metrics_data:
        insert_metrics_sql = """
            INSERT INTO weekly_metrics (
                app_id, recorded_at, current_price, discount_percent, 
                concurrent_players, total_positive_reviews, total_negative_reviews, 
                review_score_desc, steam_followers, estimated_owners_min, 
                estimated_owners_max, average_playtime_2weeks, median_playtime_2weeks, top_seller_rank
            ) VALUES %s
            ON CONFLICT (app_id, recorded_at) DO UPDATE SET 
                current_price = EXCLUDED.current_price,
                discount_percent = EXCLUDED.discount_percent,
                concurrent_players = EXCLUDED.concurrent_players,
                total_positive_reviews = EXCLUDED.total_positive_reviews,
                total_negative_reviews = EXCLUDED.total_negative_reviews,
                review_score_desc = EXCLUDED.review_score_desc,
                estimated_owners_min = EXCLUDED.estimated_owners_min,
                estimated_owners_max = EXCLUDED.estimated_owners_max,
                average_playtime_2weeks = EXCLUDED.average_playtime_2weeks,
                median_playtime_2weeks = EXCLUDED.median_playtime_2weeks,
                top_seller_rank = EXCLUDED.top_seller_rank;
        """
        metrics_template = "(%s, DATE_TRUNC('day', NOW()), %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s, %s, %s)"
        psycopg2.extras.execute_values(cursor, insert_metrics_sql, metrics_data, template=metrics_template)

    conn.commit()

# --- MAIN EXECUTION ---

def run_enrichment_pipeline():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print(f"[INFO] Fetching batch of {BATCH_SIZE} un-enriched games from database queue...")
    
    cursor.execute("SELECT app_id FROM games ORDER BY metrics_updated_at ASC NULLS FIRST LIMIT %s;", (BATCH_SIZE,))
    app_ids = [row[0] for row in cursor.fetchall()]

    print(f"[INFO] Processing {len(app_ids)} games with parallel API fetching & DB batching...")

    successful_count = 0
    payload_buffer = []
    skipped_buffer = []

    for idx, app_id in enumerate(app_ids, start=1):
        start_time = time.time()
        
        payload = enrich_game(app_id)
        
        if payload:
            payload_buffer.append(payload)
            successful_count += 1
            status = "ENRICHED"
        else:
            skipped_buffer.append(app_id)
            status = "SKIPPED/NO_DATA"

        elapsed = time.time() - start_time
        print(f"[{idx}/{len(app_ids)}] App ID {app_id} -> {status} ({elapsed:.2f}s)")

        if len(payload_buffer) + len(skipped_buffer) >= DB_BATCH_SIZE:
            print(f"[INFO] Flushing batch of {DB_BATCH_SIZE} records to database...")
            save_game_metrics_batch(conn, cursor, payload_buffer, skipped_buffer)
            payload_buffer.clear()
            skipped_buffer.clear()

        time.sleep(0.2)

    if payload_buffer or skipped_buffer:
        print(f"[INFO] Flushing final remaining records to database...")
        save_game_metrics_batch(conn, cursor, payload_buffer, skipped_buffer)

    cursor.close()
    conn.close()
    print(f"[SUCCESS] Pipeline complete! Successfully enriched {successful_count} games.")

if __name__ == "__main__":
    run_enrichment_pipeline()
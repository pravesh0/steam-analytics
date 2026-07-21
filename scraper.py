import os
import time
import requests
import psycopg2
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor, as_completed

DATABASE_URL = os.environ.get("DATABASE_URL")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 4000))
MAX_WORKERS = 5

if not DATABASE_URL:
    print("Error: Missing DATABASE_URL environment variable.")
    exit(1)

# --- SAFETY HELPER FUNCTIONS ---

def safe_int(val, default=0):
    """Safely converts messy API values into clean integers."""
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
    """Safely sanitizes text fields, turning empty strings or blanks into None."""
    if val is None:
        return default
    if isinstance(val, str):
        val = val.strip()
        return val if val else default
    return str(val)

def safe_date(val):
    """Sanitizes Steam release dates, rejecting non-date phrases like 'Coming soon'."""
    val_str = safe_str(val)
    if not val_str:
        return None
    
    # Catch text phrases that break PostgreSQL DATE parsing
    invalid_phrases = ["coming soon", "tba", "soon", "tbd", "to be announced"]
    if any(phrase in val_str.lower() for phrase in invalid_phrases):
        return None
        
    return val_str

# --- INDIVIDUAL API FETCHERS ---

def fetch_store_data(app_id):
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&l=english"
    try:
        res = requests.get(url, timeout=7)
        data = res.json().get(str(app_id), {})
        return data.get("data") if data.get("success") else None
    except Exception:
        return None

def fetch_player_count(app_id):
    url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}"
    try:
        res = requests.get(url, timeout=7)
        return res.json().get("response", {}).get("player_count")
    except Exception:
        return None

def fetch_steamspy_data(app_id):
    url = f"https://steamspy.com/api.php?request=appdetails&appid={app_id}"
    try:
        res = requests.get(url, timeout=7)
        return res.json()
    except Exception:
        return None

def fetch_reviews(app_id):
    url = f"https://store.steampowered.com/appreviews/{app_id}?json=1&language=all"
    try:
        res = requests.get(url, timeout=7)
        return res.json().get("query_summary")
    except Exception:
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

# --- DATABASE SAVER ---

def save_game_metrics(conn, cursor, payload):
    app_id = payload["app_id"]
    store = payload["store"]
    spy = payload["spy"]
    reviews = payload["reviews"]
    
    release_data = store.get("release_date") or {}
    price_overview = store.get("price_overview", {})
    metacritic = store.get("metacritic", {})
    platforms = store.get("platforms", {})
    
    # --- BULLETPROOF FIELD SANITIZATION ---
    game_type = safe_str(store.get("type"), "game")
    is_free = bool(store.get("is_free", False))
    required_age = safe_int(store.get("required_age"), 0)
    short_desc = safe_str(store.get("short_description"))
    release_date_str = safe_date(release_data.get("date"))  # Safely checks for 'Coming soon'
    base_price = safe_int(price_overview.get("initial"), 0)
    controller_support = safe_str(store.get("controller_support"), "none")
    metacritic_score = safe_int(metacritic.get("score"), 0)
    
    languages = store.get("supported_languages")
    languages_json = psycopg2.extras.Json(languages) if languages else None
    
    mac_support = bool(platforms.get("mac", False))
    linux_support = bool(platforms.get("linux", False))
    # -------------------------------------

    update_game_sql = """
        UPDATE games SET
            type = %s,
            is_free = %s,
            required_age = %s,
            short_description = %s,
            release_date = %s,
            base_price = %s,
            controller_support = %s,
            metacritic_score = %s,
            supported_languages = %s,
            mac_support = %s,
            linux_support = %s,
            updated_at = NOW(),
            metrics_updated_at = NOW()
        WHERE app_id = %s;
    """

    cursor.execute(update_game_sql, (
        game_type, is_free, required_age, short_desc, release_date_str,
        base_price, controller_support, metacritic_score, languages_json,
        mac_support, linux_support, app_id
    ))

    # --- STORAGE STRATEGY ---
    is_coming_soon = release_data.get("coming_soon", False)
    live_players = safe_int(payload["players"], 0)
    peak_players = safe_int(spy.get("ccu"), 0)

    is_dead = (not is_coming_soon) and (live_players == 0 and peak_players == 0)

    if not is_dead:
        # NOTICE: DATE_TRUNC('day', NOW()) prevents database bloat by grouping updates by calendar day
        insert_metrics_sql = """
            INSERT INTO weekly_metrics (
                app_id, recorded_at, current_price, discount_percent, 
                concurrent_players, total_positive_reviews, total_negative_reviews, 
                review_score_desc, steam_followers, estimated_owners_min, 
                estimated_owners_max, average_playtime_2weeks, median_playtime_2weeks, top_seller_rank
            ) VALUES (%s, DATE_TRUNC('day', NOW()), %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s, %s, %s)
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
        
        owners_raw = spy.get("owners", "0 .. 0")
        if isinstance(owners_raw, str):
            owners_parts = owners_raw.replace(",", "").split("..")
        else:
            owners_parts = ["0", "0"]
            
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

        cursor.execute(insert_metrics_sql, (
            app_id, current_price, discount_pct, live_players,
            pos_reviews, neg_reviews, review_desc, min_owners,
            max_owners, avg_playtime, med_playtime, ccu
        ))

    cursor.execute("UPDATE games SET metrics_updated_at = NOW() WHERE app_id = %s;", (app_id,))
    conn.commit()

# --- MAIN EXECUTION ---

def run_enrichment_pipeline():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print(f"[INFO] Fetching batch of {BATCH_SIZE} un-enriched games from database queue...")
    
    cursor.execute("SELECT app_id FROM games ORDER BY metrics_updated_at ASC NULLS FIRST LIMIT %s;", (BATCH_SIZE,))
    app_ids = [row[0] for row in cursor.fetchall()]

    print(f"[INFO] Processing {len(app_ids)} games with parallel API fetching...")

    successful_count = 0
    for idx, app_id in enumerate(app_ids, start=1):
        start_time = time.time()
        
        payload = enrich_game(app_id)
        
        if payload:
            save_game_metrics(conn, cursor, payload)
            successful_count += 1
            status = "ENRICHED"
        else:
            cursor.execute("UPDATE games SET metrics_updated_at = NOW() WHERE app_id = %s;", (app_id,))
            conn.commit()
            status = "SKIPPED/NO_DATA"

        elapsed = time.time() - start_time
        print(f"[{idx}/{len(app_ids)}] App ID {app_id} -> {status} ({elapsed:.2f}s)")
        
        time.sleep(1.2)

    cursor.close()
    conn.close()
    print(f"[SUCCESS] Pipeline complete! Successfully enriched {successful_count} games.")

if __name__ == "__main__":
    run_enrichment_pipeline()
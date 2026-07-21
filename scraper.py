import os
import time
import requests
import psycopg2
import psycopg2.extras
from concurrent.futures import ThreadPoolExecutor, as_completed

DATABASE_URL = os.environ.get("DATABASE_URL")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 3000))  # Process 1,500 games per run
MAX_WORKERS = 5  # Parallel threads for fetching endpoints simultaneously

if not DATABASE_URL:
    print("Error: Missing DATABASE_URL environment variable.")
    exit(1)

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

def fetch_followers(app_id):
    url = f"https://steamcommunity.com/games/{app_id}/memberslistxml/?xml=1"
    try:
        res = requests.get(url, timeout=7)
        if "<groupDetails>" in res.text:
            start = res.text.find("<memberCount>") + 13
            end = res.text.find("</memberCount>")
            return int(res.text[start:end])
    except Exception:
        pass
    return None

# --- PARALLEL WORKER ---

def enrich_game(app_id):
    """Fires all 5 API calls in parallel for a single game."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_store = executor.submit(fetch_store_data, app_id)
        future_players = executor.submit(fetch_player_count, app_id)
        future_spy = executor.submit(fetch_steamspy_data, app_id)
        future_reviews = executor.submit(fetch_reviews, app_id)
        future_followers = executor.submit(fetch_followers, app_id)

        store_data = future_store.result()
        # If store data fails or app isn't a game, skip heavy metric writing
        if not store_data:
            return None

        return {
            "app_id": app_id,
            "store": store_data,
            "players": future_players.result(),
            "spy": future_spy.result() or {},
            "reviews": future_reviews.result() or {},
            "followers": future_followers.result()
        }

# --- DATABASE QUEUE & SAVER ---

def get_queue_batch(cursor, limit):
    """Pulls the highest priority games needing enrichment."""
    query = """
        SELECT app_id 
        FROM games 
        ORDER BY metrics_updated_at ASC NULLS FIRST 
        LIMIT %s;
    """
    cursor.execute(query, (limit,))
    return [row[0] for rowquery in cursor.fetchall() for row in rowquery] # flattened list

def save_game_metrics(conn, cursor, payload):
    """Persists metrics and updates the queue timestamp."""
    app_id = payload["app_id"]
    store = payload["store"]
    spy = payload["spy"]
    reviews = payload["reviews"]
    
    # 1. Update Games Table Metadata
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
    
    price_overview = store.get("price_overview", {})
    release_date_str = store.get("release_date", {}).get("date")
    
    # Basic date cleanup
    release_date = None
    if release_date_str:
        try:
            # Add simple parsing if needed or let PG handle standard string format
            release_date = release_date_str
        except Exception:
            pass

    cursor.execute(update_game_sql, (
        store.get("type"),
        store.get("is_free", False),
        store.get("required_age", 0),
        store.get("short_description"),
        release_date,
        price_overview.get("initial", 0),
        store.get("controller_support", "none"),
        store.get("metacritic", {}).get("score", 0),
        psycopg2.extras.Json(store.get("supported_languages")),
        store.get("platforms", {}).get("mac", False),
        store.get("platforms", {}).get("linux", False),
        app_id
    ))

    # 2. Insert Time-Series Record into weekly_metrics
    insert_metrics_sql = """
        INSERT INTO weekly_metrics (
            app_id, recorded_at, current_price, discount_percent, 
            concurrent_players, total_positive_reviews, total_negative_reviews, 
            review_score_desc, steam_followers, estimated_owners_min, 
            estimated_owners_max, average_playtime_2weeks, median_playtime_2weeks, top_seller_rank
        ) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (app_id, recorded_at) DO UPDATE SET current_price = EXCLUDED.current_price;
    """
    
    owners_raw = spy.get("owners", "0 .. 0").replace(",", "").split("..")
    min_owners = int(owners_raw[0].strip()) if len(owners_raw) > 0 and owners_raw[0].strip().isdigit() else 0
    max_owners = int(owners_raw[1].strip()) if len(owners_raw) > 1 and owners_raw[1].strip().isdigit() else 0

    cursor.execute(insert_metrics_sql, (
        app_id,
        price_overview.get("final", 0),
        price_overview.get("discount_percent", 0),
        payload["players"],
        reviews.get("total_positive", 0),
        reviews.get("total_negative", 0),
        reviews.get("review_score_desc"),
        payload["followers"],
        min_owners,
        max_owners,
        spy.get("average_2weeks", 0),
        spy.get("median_2weeks", 0),
        spy.get("ccu", 0)
    ))

    # Always mark metrics_updated_at even if it was a non-game or incomplete row
    cursor.execute("UPDATE games SET metrics_updated_at = NOW() WHERE app_id = %s;", (app_id,))
    conn.commit()

# --- MAIN EXECUTION ---

def run_enrichment_pipeline():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print(f"[INFO] Fetching batch of {BATCH_SIZE} un-enriched games from database queue...")
    
    # Pull targets from queue
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
            # If invalid/delisted, mark timestamp so queue moves forward
            cursor.execute("UPDATE games SET metrics_updated_at = NOW() WHERE app_id = %s;", (app_id,))
            conn.commit()
            status = "SKIPPED/NO_DATA"

        elapsed = time.time() - start_time
        print(f"[{idx}/{len(app_ids)}] App ID {app_id} -> {status} ({elapsed:.2f}s)")
        
        # 1-second pause between games to keep Steam happy
        time.sleep(2.5)

    cursor.close()
    conn.close()
    print(f"[SUCCESS] Pipeline complete! Successfully enriched {successful_count} games.")

if __name__ == "__main__":
    run_enrichment_pipeline()
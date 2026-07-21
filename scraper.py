import time
import json
import re
import requests
import psycopg2
import random
import os
from datetime import datetime, timezone

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def fetch_master_app_list():
    """Fetches the master list of app IDs using SteamSpy, which allows cloud/datacenter IPs."""
    url = "https://steamspy.com/api.php?request=all"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        print("Fetching app list from SteamSpy...")
        response = requests.get(url, headers=headers, timeout=30)
        print(f"SteamSpy App List Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            app_ids = [int(appid) for appid in data.keys()]
            if app_ids:
                print(f"Successfully retrieved {len(app_ids)} apps from SteamSpy.")
                return app_ids
    except Exception as e:
        print(f"SteamSpy App List Error: {e}")

    # Fallback to Valve's official API just in case
    valve_url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    try:
        print("Falling back to Valve's official App List API...")
        response = requests.get(valve_url, headers=headers, timeout=30)
        print(f"Valve API Status Code: {response.status_code}")
        if response.status_code == 200:
            apps = response.json().get("applist", {}).get("apps", [])
            return [app["appid"] for app in apps]
    except Exception as e:
        print(f"Valve API Error: {e}")

    return []

def fetch_top_sellers():
    url = "https://store.steampowered.com/api/featuredcategories/"
    ranks = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            top_sellers = data.get("top_sellers", {}).get("items", [])
            for index, item in enumerate(top_sellers):
                ranks[item.get("id")] = index + 1
    except Exception as e:
        print(f"Top Sellers API Error: {e}")
    return ranks

def fetch_steam_store_data(app_id):
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and str(app_id) in data and data[str(app_id)]["success"]:
                return data[str(app_id)]["data"]
    except Exception:
        pass
    return {}

def fetch_steam_players(app_id):
    url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get("response", {}).get("result") == 1:
                return data["response"].get("player_count", 0)
    except Exception:
        pass
    return None

def fetch_steamspy_data(app_id):
    url = f"https://steamspy.com/api.php?request=appdetails&appid={app_id}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return {}

def fetch_steam_reviews(app_id):
    url = f"https://store.steampowered.com/appreviews/{app_id}?json=1&language=all&purchase_type=all"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get("query_summary", {})
    except Exception:
        pass
    return {}

def fetch_steam_followers(app_id):
    url = f"https://steamcommunity.com/games/{app_id}/memberslistxml/?xml=1"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            match = re.search(r'<memberCount>(\d+)</memberCount>', response.text)
            if match:
                return int(match.group(1))
    except Exception:
        pass
    return None

def upsert_game_metadata(cursor, app_id, store_data):
    title = store_data.get("name", "Unknown")
    game_type = store_data.get("type", "game")
    is_free = store_data.get("is_free", False)
    required_age = store_data.get("required_age", 0)
    short_desc = store_data.get("short_description", "")
    
    price_overview = store_data.get("price_overview", {})
    base_price = price_overview.get("initial", 0) if price_overview else 0

    platforms = store_data.get("platforms", {})
    mac_support = platforms.get("mac", False)
    linux_support = platforms.get("linux", False) 

    controller_support = store_data.get("controller_support", None)
    if not controller_support:
        categories = store_data.get("categories", [])
        for cat in categories:
            if str(cat.get("id")) == "17":
                controller_support = "partial"
                break

    metacritic_score = store_data.get("metacritic", {}).get("score", None)
    languages_raw = store_data.get("supported_languages", "")
    supported_languages = json.dumps({"raw_html": languages_raw}) if languages_raw else None

    release_date_str = store_data.get("release_date", {}).get("date", "")
    release_date_obj = None
    if release_date_str:
        for fmt in ("%d %b, %Y", "%b %d, %Y", "%d %B, %Y", "%B %d, %Y", "%Y-%m-%d"):
            try:
                release_date_obj = datetime.strptime(release_date_str, fmt).date()
                break
            except ValueError:
                continue

    sql = """
        INSERT INTO games (
            app_id, title, type, is_free, required_age, short_description, 
            release_date, base_price, controller_support, metacritic_score, 
            supported_languages, mac_support, linux_support, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (app_id) DO UPDATE SET
            title = EXCLUDED.title,
            type = EXCLUDED.type,
            is_free = EXCLUDED.is_free,
            required_age = EXCLUDED.required_age,
            short_description = EXCLUDED.short_description,
            release_date = EXCLUDED.release_date,
            base_price = EXCLUDED.base_price,
            controller_support = EXCLUDED.controller_support,
            metacritic_score = EXCLUDED.metacritic_score,
            supported_languages = EXCLUDED.supported_languages,
            mac_support = EXCLUDED.mac_support,
            linux_support = EXCLUDED.linux_support,
            updated_at = NOW();
    """
    cursor.execute(sql, (
        app_id, title, game_type, is_free, required_age, short_desc, 
        release_date_obj, base_price, controller_support, metacritic_score, 
        supported_languages, mac_support, linux_support
    ))

def upsert_companies(cursor, app_id, store_data):
    developers = store_data.get("developers", [])
    publishers = store_data.get("publishers", [])

    def link_company(name, role):
        if not name:
            return
        cursor.execute("""
            INSERT INTO companies (name) VALUES (%s)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING company_id;
        """, (name,))
        company_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO game_companies (app_id, company_id, role_type)
            VALUES (%s, %s, %s)
            ON CONFLICT (app_id, company_id, role_type) DO NOTHING;
        """, (app_id, company_id, role))

    for dev in developers:
        link_company(dev, "developer")
    for pub in publishers:
        link_company(pub, "publisher")

def upsert_tags(cursor, app_id, spy_data):
    tags = spy_data.get("tags", {})
    if not isinstance(tags, dict):
        return

    for tag_name, votes in tags.items():
        cursor.execute("""
            INSERT INTO tags (name) VALUES (%s)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING tag_id;
        """, (tag_name,))
        tag_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO game_tags (app_id, tag_id, votes)
            VALUES (%s, %s, %s)
            ON CONFLICT (app_id, tag_id) DO UPDATE SET votes = EXCLUDED.votes;
        """, (app_id, tag_id, votes))

def insert_weekly_metric(cursor, app_id, store_data, player_count, spy_data, review_data, followers, top_seller_rank):
    recorded_at = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    price_overview = store_data.get("price_overview", {})
    current_price = price_overview.get("final", 0) if price_overview else 0
    discount_percent = price_overview.get("discount_percent", 0) if price_overview else 0
    
    positive_reviews = review_data.get("total_positive")
    negative_reviews = review_data.get("total_negative")
    review_score_desc = review_data.get("review_score_desc")
    
    avg_playtime = spy_data.get("average_2weeks", None)
    med_playtime = spy_data.get("median_2weeks", None)
    
    owners_str = spy_data.get("owners", "")
    min_owners, max_owners = None, None
    if " .. " in owners_str:
        parts = owners_str.replace(",", "").split(" .. ")
        min_owners, max_owners = int(parts[0]), int(parts[1])

    sql = """
        INSERT INTO weekly_metrics 
        (app_id, recorded_at, current_price, discount_percent, concurrent_players, 
         total_positive_reviews, total_negative_reviews, review_score_desc, steam_followers, 
         estimated_owners_min, estimated_owners_max, average_playtime_2weeks, median_playtime_2weeks,
         top_seller_rank)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (app_id, recorded_at) DO UPDATE SET
            current_price = EXCLUDED.current_price,
            discount_percent = EXCLUDED.discount_percent,
            concurrent_players = EXCLUDED.concurrent_players,
            total_positive_reviews = EXCLUDED.total_positive_reviews,
            total_negative_reviews = EXCLUDED.total_negative_reviews,
            review_score_desc = EXCLUDED.review_score_desc,
            steam_followers = EXCLUDED.steam_followers,
            estimated_owners_min = EXCLUDED.estimated_owners_min,
            estimated_owners_max = EXCLUDED.estimated_owners_max,
            average_playtime_2weeks = EXCLUDED.average_playtime_2weeks,
            median_playtime_2weeks = EXCLUDED.median_playtime_2weeks,
            top_seller_rank = EXCLUDED.top_seller_rank;
    """
    cursor.execute(sql, (
        app_id, recorded_at, current_price, discount_percent, player_count,
        positive_reviews, negative_reviews, review_score_desc, followers, 
        min_owners, max_owners, avg_playtime, med_playtime, top_seller_rank
    ))

def run_scraper():
    conn = get_db_connection()
    cursor = conn.cursor()

    print("Fetching Global Top Sellers Chart...")
    global_top_sellers = fetch_top_sellers()

    print("Fetching Master App List...")
    all_app_ids = fetch_master_app_list()
    
    if not all_app_ids:
        print("Failed to retrieve master list. Exiting.")
        return

    target_apps = random.sample(all_app_ids, 100) if len(all_app_ids) > 100 else all_app_ids
    print(f"Randomly selected {len(target_apps)} games to process.")

    for idx, app_id in enumerate(target_apps, 1):
        print(f"[{idx}/100] Fetching Data for App ID: {app_id}...")
        
        store_data = fetch_steam_store_data(app_id)
        
        if store_data:
            player_count = fetch_steam_players(app_id)
            spy_data = fetch_steamspy_data(app_id)
            review_data = fetch_steam_reviews(app_id)
            followers = fetch_steam_followers(app_id)
            top_seller_rank = global_top_sellers.get(app_id, None)

            upsert_game_metadata(cursor, app_id, store_data)
            upsert_companies(cursor, app_id, store_data)
            upsert_tags(cursor, app_id, spy_data)
            insert_weekly_metric(
                cursor, app_id, store_data, player_count, 
                spy_data, review_data, followers, top_seller_rank
            )
            
            conn.commit()
            print(f"   -> Successfully saved: {store_data.get('name', 'Unknown')}")
        else:
            print("   -> Skipped (No store data or invalid ID)")

        time.sleep(3)

    cursor.close()
    conn.close()
    print("Batch processing completed successfully!")

if __name__ == "__main__":
    run_scraper()
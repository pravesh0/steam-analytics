import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch

# 1. Load Environment Variables
STEAM_API_KEY = os.environ.get("STEAM_API_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

if not STEAM_API_KEY or not DATABASE_URL:
    print("Error: Missing STEAM_API_KEY or DATABASE_URL environment variables.")
    exit(1)

def fetch_all_steam_apps():
    """Fetches the complete list of Steam games using the modern IStoreService API."""
    print("[INFO] Starting Steam Catalog Discovery...")
    
    apps_discovered = []
    last_appid = 0
    has_more = True
    
    while has_more:
        # We request up to 50,000 games at a time, paginating with last_appid
        url = (
            f"https://api.steampowered.com/IStoreService/GetAppList/v1/"
            f"?key={STEAM_API_KEY}&max_results=50000&last_appid={last_appid}"
        )
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json().get("response", {})
            
            apps = data.get("apps", [])
            if not apps:
                break
                
            apps_discovered.extend(apps)
            
            # The API natively tells us if there are more pages
            has_more = data.get("have_more_results", False)
            last_appid = data.get("last_appid", apps[-1]["appid"])
            
            print(f"[SUCCESS] Fetched page. Total apps found so far: {len(apps_discovered)}")
            time.sleep(1.5) # Small 1-second safety pause between pages
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch app list at last_appid {last_appid}: {e}")
            break
            
    return apps_discovered

def save_apps_to_db(apps):
    """Saves discovered games to the database without overwriting existing metrics."""
    if not apps:
        print("[WARNING] No apps to save.")
        return

    print(f"[INFO] Connecting to database to save {len(apps)} games...")
    
    # Prepare data for batch insert: (app_id, title)
    records = [(app.get("appid"), app.get("name", "Unknown Title")) for app in apps]
    
    # Notice the ON CONFLICT logic: It only updates the title. 
    # It deliberately leaves your deep metrics and timestamps completely untouched!
    insert_query = """
        INSERT INTO games (app_id, title) 
        VALUES (%s, %s)
        ON CONFLICT (app_id) DO UPDATE 
        SET title = EXCLUDED.title;
    """
    
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # We use psycopg2's execute_batch to push thousands of rows per second
        execute_batch(cursor, insert_query, records, page_size=5000)
        
        conn.commit()
        print(f"[SUCCESS] Master catalog successfully updated with {len(apps)} games!")
        
    except Exception as e:
        print(f"[ERROR] Database insert failed: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    steam_apps = fetch_all_steam_apps()
    save_apps_to_db(steam_apps)
    print("[INFO] Discovery Pass 1 Complete.")
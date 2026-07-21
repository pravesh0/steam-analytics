import requests

app_id = 730
url = f"https://steamcommunity.com/games/{app_id}/memberslistxml/?xml=1"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

response = requests.get(url, headers=headers)
print(f"Status Code: {response.status_code}")
print(f"Server Response: {response.text[:250]}") # Prints the first 250 characters
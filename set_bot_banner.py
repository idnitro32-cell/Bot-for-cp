import requests

# --- ONLY CHANGE THE VALUES BELOW ---
# 1. Your Bot's Token
DISCORD_BOT_TOKEN = "MTQ5NjA2OTkyNTMwMTY1MzUzNA.GGuuP9.Gp51luU9-z-_rTxf9TMUBET-CxGWJYgonUcpEspython"
# 2. The URL or Local Path to your banner image
# You can use a direct URL, or point to a file on your computer.
# Example URL: "https://example.com/my_cool_banner.png"
# Example File Path (Windows): r"C:\Users\YourName\Desktop\my_banner.png"
BANNER_SOURCE = "https://i.pinimg.com/736x/c8/23/07/c82307e84b0f57fc08da521a1f97470b.jpg"
# -----------------------------------

# 1. Get the banner image data
try:
    # Check if BANNER_SOURCE is a URL or a local file
    if BANNER_SOURCE.startswith(("http://", "https://")):
        response = requests.get(BANNER_SOURCE)
        response.raise_for_status()
        image_data = response.content
        print("✅ Banner image downloaded from URL.")
    else:
        with open(BANNER_SOURCE, "rb") as f:
            image_data = f.read()
        print("✅ Banner image loaded from local file.")
except Exception as e:
    print(f"❌ Failed to get banner image: {e}")
    exit()

# 2. Encode the image to Base64
import base64
banner_base64 = base64.b64encode(image_data).decode('utf-8')

# 3. Prepare the API request
# Determine the correct data header for static or animated images
if BANNER_SOURCE.lower().endswith('.gif'):
    data_uri = f"data:image/gif;base64,{banner_base64}"
else:
    data_uri = f"data:image/png;base64,{banner_base64}"

payload = {
    "banner": data_uri
}

headers = {
    'Authorization': f'Bot {DISCORD_BOT_TOKEN}',
    'Content-Type': 'application/json'
}

# 4. Send the request to update the bot's profile
print("🔄 Sending request to Discord API...")
api_response = requests.patch('https://discord.com/api/v10/users/@me', headers=headers, json=payload)

# 5. Check the result
if api_response.status_code == 200:
    print("✨ Success! Your bot's banner has been updated. It might take a minute to show up in Discord.")
else:
    print(f"❌ Failed to update banner. Status Code: {api_response.status_code}")
    print(f"Response: {api_response.text}")
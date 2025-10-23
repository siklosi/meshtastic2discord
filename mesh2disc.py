import json
import sqlite3
import os
import paho.mqtt.client as mqtt
import requests

# --- Configuration ---
MQTT_BROKER = "192.168.1.2"
MQTT_PORT = 1883
MQTT_USERNAME = "user"
MQTT_PASSWORD = "password"
MQTT_TOPIC = "msh/EU_868/2/json/#"

# Discord webhooks per channel
DISCORD_WEBHOOKS = {
    0: "https://discord.com/api/webhooks/1430820861119692832/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX0",  # Channel 0
    1: "https://discord.com/api/webhooks/1430813103926280293/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX1",  # Channel 1
}

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "meshtastic_nodes.db")

# --- Database helpers ---

def init_db():
    """Create the SQLite DB if it doesn't exist."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            node_id INTEGER PRIMARY KEY,
            longname TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def count_nodes():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM nodes")
        count = c.fetchone()[0]
        conn.close()
        return count
    except sqlite3.Error as e:
        print(f"An error occurred while counting nodes: {e}")
        return 0
    
def update_nodeinfo(node_id: int, longname: str):
    """Insert or update node info if changed."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT longname FROM nodes WHERE node_id = ?", (node_id,))
    row = c.fetchone()
    if row is None:
        c.execute("INSERT INTO nodes (node_id, longname) VALUES (?, ?)", (node_id, longname))
        print(f"🆕 Added new node: {node_id} → {longname}")
    elif row[0] != longname:
        c.execute("UPDATE nodes SET longname = ? WHERE node_id = ?", (longname, node_id))
        print(f"🔁 Updated node {node_id} name to {longname}")
    conn.commit()
    conn.close()

def get_longname(node_id: int) -> str:
    """Get longname from DB or return numeric ID if not found."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT longname FROM nodes WHERE node_id = ?", (node_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else str(node_id)

# --- Discord webhook sender ---

def post_to_discord(sender_name, channel, message):
    webhook_url = DISCORD_WEBHOOKS.get(channel)
    if not webhook_url:
        print(f"⚠️ No webhook configured for channel {channel}")
        return

    content = f"📡 From {sender_name}:**\n{message}"
    try:
        r = requests.post(webhook_url, json={"content": content}, timeout=5)
        if r.status_code in (200, 204):
            print(f"✅ Sent to Discord from {sender_name}: {message}")
        else:
            print(f"⚠️ Discord webhook error {r.status_code}: {r.text}")
    except Exception as e:
        print(f"⚠️ Failed to send to Discord: {e}")

# --- MQTT event handlers ---

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
        print(f"📡 Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"❌ Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload)

        msg_type = data.get("type")
        node_id = data.get("from")

        if msg_type == "nodeinfo" and node_id is not None:
            longname = data.get("payload", {}).get("longname")
            if longname:
                update_nodeinfo(node_id, longname)

        elif msg_type == "text" and node_id is not None:
            channel = data.get("channel")
            message_text = data.get("payload", {}).get("text", "")
            if channel in DISCORD_WEBHOOKS and message_text:
                sender_name = get_longname(node_id)
                post_to_discord(sender_name, channel, message_text)
            else:
                print(f"ℹ️ Ignored text message (missing channel or text): {data}")

        else:
            # Ignore telemetry, position, etc.
            pass

    except json.JSONDecodeError:
        print("⚠️ Non-JSON MQTT payload received, skipping...")
    except Exception as e:
        print(f"⚠️ Error processing message: {e}")

# --- Main execution ---

if __name__ == "__main__":
    init_db()
    print("🚀 Starting Meshtastic MQTT → Discord forwarder with SQLite node tracking...")
    node_count = count_nodes()
    print(f"Nodes in db: {node_count}")

    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

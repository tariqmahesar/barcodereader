import snap7

PLC_IP = "192.168.0.0"
RACK = 0
SLOT = 1   # S7-1200

def read_db():
    client = snap7.client.Client()
    client.connect(PLC_IP, RACK, SLOT)

    if client.get_connected():
        print("[✔] Connected, reading DB483...")

        # Read 24 bytes starting from byte 0
        data = client.db_read(48, 0, 24)
        print("Raw Data (bytes):", data)

        client.disconnect()
    else:
        print("[✖] PLC not connected")


read_db()



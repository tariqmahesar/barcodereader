import snap7
from snap7.util import get_string

PLC_IP = "192.168.0.0"
RACK = 0
SLOT = 1
DB_NUMBER = 480
DB_SIZE = 4096

def read_plc_strings(client):
    """
    Read data from PLC (DB480) and extract strings at specific offsets.
    """
    data = client.db_read(DB_NUMBER, 0, DB_SIZE)
    strings = [
        get_string(data, 0),
        get_string(data, 256),
        get_string(data, 512),
        get_string(data, 768),
        get_string(data, 1024),
        get_string(data, 1280),
        get_string(data, 1536)
    ]
    return strings

def connect_to_plc():
    """
    Connect to the PLC and return the client object.
    """
    client = snap7.client.Client()
    client.connect(PLC_IP, RACK, SLOT)
    if not client.get_connected():
        raise Exception("Failed to connect to PLC.")
    return client
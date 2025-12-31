"""
PLC to SAP-ME Complete Integration System
Version: 1.0
Date: 2024-12-11
"""

import snap7
from snap7.util import get_string, set_bool, get_bool
import xml.etree.ElementTree as ET
from xml.dom import minidom
import pandas as pd
from openpyxl import load_workbook
import time
import os
import paramiko
from datetime import datetime
import logging
from pathlib import Path
import sys

# ============================================
# CONFIGURATION
# ============================================

# PLC Configuration
PLC_IP = "192.168.0.0"
RACK = 0
SLOT = 1
DB_NUMBER = 3000
DB_SIZE = 1000  # Increased for DB3000
POLL_INTERVAL = 0.5  # Fast polling for handshake

# Data Structure (from PLC developer)
BOX_LID_START = 94  # DBB94
BOX_LID_LENGTH = 15  # DBB94-108
PRODUCT_COUNT = 20  # Max products per box
BYTES_PER_PRODUCT = 60

# Product field offsets (relative to product start)
OFFSET_SPOOL = 0  # 15 bytes
OFFSET_MATERIAL = 16  # 7 bytes (DBB26)
OFFSET_MANUF_DATE = 24  # 8 bytes (DBB34)
OFFSET_EXP_DATE = 32  # 8 bytes (DBB42)
OFFSET_BATCH = 40  # 10 bytes (DBB50)
OFFSET_SHOP_ORDER = 50  # 10 bytes (DBB60)

# File Configuration
PROJECT_DIR = r"G:\TestProject"
LOCAL_XML_PATH = os.path.join(PROJECT_DIR, "Scandata.xml")
EXCEL_FILE = os.path.join(PROJECT_DIR, "Production_Log.xlsx")
BACKUP_DIR = os.path.join(PROJECT_DIR, "backups")
LOG_FILE = os.path.join(PROJECT_DIR, "plc_sap.log")

# SAP SFTP Configuration (TEST ENVIRONMENT - Q07-CU)
SFTP_HOST = ""
SFTP_PORT = “”
SFTP_USER = ""
SFTP_PASS = ""  # GET FROM YOUR TEAM!
REMOTE_PATH = "Scandata.xml"  # Test folder

# XML Settings
RDA_VALUE = "1"
START_VALUE = "R"


# ============================================
# LOGGING SETUP
# ============================================

def setup_logging():
    os.makedirs(PROJECT_DIR, exist_ok=True)

    logger = logging.getLogger('PLC_SAP')
    logger.setLevel(logging.INFO)

    # Clear existing handlers
    logger.handlers.clear()

    # File handler
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formatters
    file_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_formatter = logging.Formatter('%(message)s')

    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logging()


# ============================================
# PLC COMMUNICATION FUNCTIONS
# ============================================

def connect_to_plc():
    """Connect to Siemens S7-1200 PLC."""
    try:
        logger.info(f"Connecting to PLC at {PLC_IP}...")
        client = snap7.client.Client()
        client.connect(PLC_IP, RACK, SLOT)

        if client.get_connected():
            logger.info(f"✓ Connected to PLC")
            return client
        else:
            raise Exception("Connection failed")

    except Exception as e:
        logger.error(f"✗ PLC connection failed: {e}")
        return None


def read_plc_string(client, start_byte, length):
    """Read string from PLC data block."""
    try:
        data = client.db_read(DB_NUMBER, start_byte, length)
        # Convert bytes to string, remove null characters
        string_data = bytes(data).decode('ascii', errors='ignore').rstrip('\x00').strip()
        return string_data
    except Exception as e:
        logger.warning(f"Failed to read string at byte {start_byte}: {e}")
        return ""


def read_plc_bit(client, byte_index, bit_index):
    """Read boolean bit from PLC."""
    try:
        data = client.db_read(DB_NUMBER, byte_index, 1)
        return get_bool(data, 0, bit_index)
    except Exception as e:
        logger.error(f"Failed to read bit {byte_index}.{bit_index}: {e}")
        return False


def write_plc_bit(client, byte_index, bit_index, value):
    """Write boolean bit to PLC."""
    try:
        data = client.db_read(DB_NUMBER, byte_index, 1)
        set_bool(data, 0, bit_index, value)
        client.db_write(DB_NUMBER, byte_index, data)
        return True
    except Exception as e:
        logger.error(f"Failed to write bit {byte_index}.{bit_index}: {e}")
        return False


# ============================================
# DATA READING FUNCTIONS
# ============================================

def read_box_data(client):
    """
    Read complete box data from PLC DB300.
    Returns: {
        'box_lid': 'BOX123456789',
        'products': [
            {
                'spool_id': 'SPOOL001',
                'material': 'MATERIAL1',
                'manuf_date': '01/01/2024',
                'exp_date': '30/06/2024',
                'batch': 'BATCH001',
                'shop_order': 'SO001'
            },
            ... (up to 20 products)
        ]
    }
    """
    logger.info("Reading box data from PLC...")

    # Read box lid
    box_lid = read_plc_string(client, BOX_LID_START, BOX_LID_LENGTH)
    logger.info(f"Box Lid: {box_lid}")

    products = []

    # Read all possible products (max 20)
    for product_num in range(PRODUCT_COUNT):
        # Calculate start byte for this product
        start_byte = 10 + (product_num * BYTES_PER_PRODUCT)

        # Read spool ID - if empty, skip this product
        spool_id = read_plc_string(client, start_byte + OFFSET_SPOOL, 15)
        if not spool_id or spool_id.isspace():
            continue  # Empty product slot, skip

        # Read other product fields
        material = read_plc_string(client, start_byte + OFFSET_MATERIAL, 7)
        manuf_date = read_plc_string(client, start_byte + OFFSET_MANUF_DATE, 8)
        exp_date = read_plc_string(client, start_byte + OFFSET_EXP_DATE, 8)
        batch = read_plc_string(client, start_byte + OFFSET_BATCH, 10)
        shop_order = read_plc_string(client, start_byte + OFFSET_SHOP_ORDER, 10)

        # Format dates if needed (assuming PLC sends DDMMYYYY)
        if len(manuf_date) == 8 and manuf_date.isdigit():
            manuf_date = f"{manuf_date[:2]}/{manuf_date[2:4]}/{manuf_date[4:]}"
        if len(exp_date) == 8 and exp_date.isdigit():
            exp_date = f"{exp_date[:2]}/{exp_date[2:4]}/{exp_date[4:]}"

        product_data = {
            'spool_id': spool_id,
            'material': material,
            'manuf_date': manuf_date,
            'exp_date': exp_date,
            'batch': batch,
            'shop_order': shop_order
        }

        products.append(product_data)
        logger.debug(f"Product {product_num + 1}: {spool_id}")

    logger.info(f"Found {len(products)} products in box")
    return {'box_lid': box_lid, 'products': products}


# ============================================
# EXCEL LOGGING FUNCTIONS
# ============================================

def save_to_excel(box_data):
    """Save box data to Excel file (append mode, no overwrite)."""
    try:
        # Create DataFrame for this box
        rows = []
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for product in box_data['products']:
            row = {
                'Timestamp': timestamp,
                'Box_Lid': box_data['box_lid'],
                'Spool_ID': product['spool_id'],
                'Material': product['material'],
                'Manufacturing_Date': product['manuf_date'],
                'Expiration_Date': product['exp_date'],
                'Batch': product['batch'],
                'Shop_Order': product['shop_order']
            }
            rows.append(row)

        df = pd.DataFrame(rows)

        # Check if Excel file exists
        if os.path.exists(EXCEL_FILE):
            # Load existing file and append
            with pd.ExcelWriter(EXCEL_FILE, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                # Read existing data
                try:
                    existing_df = pd.read_excel(EXCEL_FILE, sheet_name='Production')
                    # Append new data
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df.to_excel(writer, sheet_name='Production', index=False)
                except:
                    # Sheet doesn't exist, create new
                    df.to_excel(writer, sheet_name='Production', index=False)
        else:
            # Create new Excel file
            df.to_excel(EXCEL_FILE, sheet_name='Production', index=False)

        logger.info(f"✓ Saved {len(rows)} products to Excel: {EXCEL_FILE}")
        return True

    except Exception as e:
        logger.error(f"✗ Failed to save to Excel: {e}")
        return False


# ============================================
# XML CREATION FUNCTIONS
# ============================================

def create_sap_xml(products):
    """
    Create SAP-ME XML file (Scandata.xml format).
    Note: Box lid NOT included in XML - only product data.
    """
    try:
        if not products:
            logger.warning("No products to create XML")
            return None

        # Create root element
        scans = ET.Element("SCANS")

        # Add each product as SCAN element
        for product in products:
            scan = ET.SubElement(scans, "SCAN")

            # Add RDA element
            rda = ET.SubElement(scan, "RDA")
            rda.text = RDA_VALUE

            # Add SEELAL element with all 6 fields
            seelal = ET.SubElement(scan, "SEELAL")
            seelal_data = f"{product['spool_id']},{product['material']},{product['manuf_date']},{product['exp_date']},{product['batch']},{product['shop_order']}"
            seelal.text = seelal_data

        # Add START element
        start = ET.SubElement(scans, "START")
        start.text = START_VALUE

        # Add TRAYTYPE element (number of products)
        traytype = ET.SubElement(scans, "TRAYTYPE")
        traytype.text = str(len(products))

        # Convert to pretty XML
        xml_str = ET.tostring(scans, encoding='utf-8')
        pretty_xml = minidom.parseString(xml_str).toprettyxml(indent="    ")

        logger.info(f"Created XML with {len(products)} products")
        return pretty_xml

    except Exception as e:
        logger.error(f"Failed to create XML: {e}")
        return None


def save_xml_file(xml_content):
    """Save XML to local file."""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(LOCAL_XML_PATH), exist_ok=True)

        # Save file
        with open(LOCAL_XML_PATH, 'w', encoding='utf-8') as f:
            f.write(xml_content)

        file_size = os.path.getsize(LOCAL_XML_PATH)
        logger.info(f"✓ XML saved locally: {LOCAL_XML_PATH} ({file_size} bytes)")

        # Show preview
        print("\n" + "=" * 60)
        print("XML FILE PREVIEW (first 5 products):")
        print("=" * 60)
        lines = xml_content.split('\n')
        for i, line in enumerate(lines[:20]):  # Show first 20 lines
            if i < 20:
                print(line)
        print("...")
        print("=" * 60)

        return True

    except Exception as e:
        logger.error(f"✗ Failed to save XML file: {e}")
        return False


# ============================================
# SFTP UPLOAD FUNCTIONS
# ============================================

def upload_to_sftp():
    """Upload Scandata.xml to SAP SFTP server."""
    if not SFTP_PASS:
        logger.error("SFTP password not configured")
        print("\n⚠ SFTP PASSWORD REQUIRED!")
        print(f"Server: {SFTP_HOST}")
        print(f"Username: {SFTP_USER}")
        print(f"Folder: {REMOTE_PATH}")
        print("\nPlease add password to configuration and restart.")
        return False

    if not os.path.exists(LOCAL_XML_PATH):
        logger.error(f"Local file not found: {LOCAL_XML_PATH}")
        return False

    try:
        logger.info(f"Uploading to {SFTP_HOST}:{REMOTE_PATH}")

        # Create transport
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASS)

        # Create SFTP client
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Upload file (overwrites existing)
        sftp.put(LOCAL_XML_PATH, REMOTE_PATH)

        # Verify upload
        remote_stat = sftp.stat(REMOTE_PATH)
        local_stat = os.stat(LOCAL_XML_PATH)

        if remote_stat.st_size == local_stat.st_size:
            logger.info(f"✓ Upload successful! {remote_stat.st_size} bytes")
            logger.info(f"✓ File now in SAP folder: {REMOTE_PATH}")
            success = True
        else:
            logger.warning(f"Size mismatch: Local={local_stat.st_size}, Remote={remote_stat.st_size}")
            success = False

        # Close connections
        sftp.close()
        transport.close()

        return success

    except paramiko.AuthenticationException:
        logger.error("✗ SFTP Authentication failed. Wrong password?")
        return False
    except Exception as e:
        logger.error(f"✗ SFTP upload failed: {e}")
        return False


# ============================================
# MAIN PROCESSING FUNCTION
# ============================================

def process_box(client):
    """
    Complete processing for one box:
    1. Read data from PLC
    2. Save to Excel
    3. Create XML
    4. Upload to SAP
    5. Acknowledge to PLC
    """
    try:
        logger.info("\n" + "=" * 60)
        logger.info("STARTING BOX PROCESSING")
        logger.info("=" * 60)

        # Step 1: Read box data from PLC
        box_data = read_box_data(client)
        if not box_data['products']:
            logger.warning("No products found in box")
            return False

        # Step 2: Save to Excel (with box lid)
        excel_success = save_to_excel(box_data)
        if not excel_success:
            logger.error("Failed to save to Excel")
            return False

        # Step 3: Create SAP XML (without box lid)
        xml_content = create_sap_xml(box_data['products'])
        if not xml_content:
            logger.error("Failed to create XML")
            return False

        # Step 4: Save XML locally
        if not save_xml_file(xml_content):
            return False

        # Step 5: Upload to SAP SFTP
        logger.info("Uploading to SAP SFTP...")
        upload_success = upload_to_sftp()

        if upload_success:
            # Step 6: Acknowledge to PLC
            logger.info("Sending acknowledgment to PLC...")
            write_success = write_plc_bit(client, 0, 1, True)  # DB300.DBX0.1 = TRUE

            if write_success:
                logger.info("✓ Acknowledgment sent to PLC")

                # Brief pause, then reset
                time.sleep(0.3)
                write_plc_bit(client, 0, 1, False)

                logger.info("✓ BOX PROCESSING COMPLETE")
                logger.info(f"  Box Lid: {box_data['box_lid']}")
                logger.info(f"  Products: {len(box_data['products'])}")
                logger.info(f"  Excel: {EXCEL_FILE}")
                logger.info(f"  SAP File: {REMOTE_PATH}")

                return True
            else:
                logger.error("Failed to acknowledge PLC")
                return False
        else:
            logger.error("SAP upload failed")
            return False

    except Exception as e:
        logger.error(f"Box processing error: {e}")
        return False


# ============================================
# MAIN LOOP
# ============================================

def main_loop():
    """
    Main loop that continuously monitors PLC for new data.
    Implements handshake protocol with PLC.
    """
    logger.info("\n" + "=" * 60)
    logger.info("PLC TO SAP-ME INTEGRATION STARTED")
    logger.info("=" * 60)
    logger.info(f"PLC IP: {PLC_IP}")
    logger.info(f"DB Block: {DB_NUMBER}")
    logger.info(f"Excel Log: {EXCEL_FILE}")
    logger.info(f"SAP Folder: {REMOTE_PATH}")
    logger.info("=" * 60)
    logger.info("Waiting for PLC data...")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)

    client = None
    boxes_processed = 0

    try:
        # Connect to PLC
        client = connect_to_plc()
        if not client:
            return

        # Main monitoring loop
        while True:
            try:
                # Check if PLC has data ready
                data_ready = read_plc_bit(client, 0, 0)  # DB300.DBX0.0

                if data_ready:
                    logger.info("\n⚠ PLC SIGNAL RECEIVED: Data ready for upload!")
                    boxes_processed += 1

                    # Process the box
                    success = process_box(client)

                    if success:
                        logger.info(f"✓ Box {boxes_processed} processed successfully")
                    else:
                        logger.error(f"✗ Box {boxes_processed} processing failed")

                    # Small delay before next check
                    time.sleep(1)

                # Quick polling
                time.sleep(POLL_INTERVAL)

            except Exception as e:
                logger.error(f"Loop error: {e}")
                time.sleep(1)

    except KeyboardInterrupt:
        logger.info("\n" + "=" * 60)
        logger.info("PROGRAM STOPPED BY USER")
        logger.info(f"Total boxes processed: {boxes_processed}")
        logger.info(f"Log file: {LOG_FILE}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Fatal error: {e}")

    finally:
        if client:
            client.disconnect()
            logger.info("Disconnected from PLC")


# ============================================
# TESTING FUNCTIONS
# ============================================

def test_plc_connection():
    """Test PLC connection and data reading."""
    print("\n" + "=" * 60)
    print("TESTING PLC CONNECTION")
    print("=" * 60)

    client = None
    try:
        client = connect_to_plc()
        if not client:
            return False

        # Test reading a sample
        print("\nReading test data from PLC...")

        # Read box lid
        box_lid = read_plc_string(client, BOX_LID_START, BOX_LID_LENGTH)
        print(f"Box Lid: {box_lid}")

        # Read first product
        spool_id = read_plc_string(client, 10, 15)
        print(f"Spool ID (Product 1): {spool_id}")

        if spool_id:
            print("✓ PLC connection and data reading successful!")
            return True
        else:
            print("⚠ No data in PLC or reading issue")
            return False

    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False
    finally:
        if client:
            client.disconnect()


def test_sftp_connection():
    """Test SFTP connection."""
    print("\n" + "=" * 60)
    print("TESTING SFTP CONNECTION")
    print("=" * 60)

    if not SFTP_PASS:
        print("⚠ SFTP password not set")
        print("Please add password to configuration")
        return False

    return upload_to_sftp()


# ============================================
# MAIN MENU
# ============================================

def main():
    """Main program menu."""
    print("\n" + "=" * 60)
    print("PLC TO SAP-ME INTEGRATION SYSTEM")
    print("=" * 60)
    print("Configuration:")
    print(f"  PLC: {PLC_IP} (DB{DB_NUMBER})")
    print(f"  SAP SFTP: {SFTP_HOST}")
    print(f"  Test Folder: {REMOTE_PATH}")
    print(f"  Excel Log: {EXCEL_FILE}")
    print("=" * 60)

    # Check dependencies
    try:
        import snap7
        import paramiko
        import pandas
        import openpyxl
    except ImportError as e:
        print(f"\n❌ Missing package: {e}")
        print("\nInstall required packages:")
        print("pip install python-snap7 paramiko pandas openpyxl")
        return

    while True:
        print("\nSelect operation:")
        print("1. Start main loop (continuous monitoring)")
        print("2. Test PLC connection")
        print("3. Test SFTP connection")
        print("4. View log file")
        print("5. Open project folder")
        print("6. Exit")

        try:
            choice = input("\nChoice (1-6): ").strip()

            if choice == "1":
                print("\n" + "=" * 60)
                print("STARTING MAIN LOOP")
                print("=" * 60)
                print("The system will:")
                print("1. Monitor PLC for data ready signal")
                print("2. Read box data when available")
                print("3. Save to Excel (with box lid)")
                print("4. Upload to SAP (without box lid)")
                print("5. Acknowledge to PLC")
                print("=" * 60)
                main_loop()
                break

            elif choice == "2":
                test_plc_connection()

            elif choice == "3":
                test_sftp_connection()

            elif choice == "4":
                if os.path.exists(LOG_FILE):
                    os.startfile(LOG_FILE)
                    print(f"Opened: {LOG_FILE}")
                else:
                    print("Log file not found yet")

            elif choice == "5":
                os.startfile(PROJECT_DIR)
                print(f"Opened: {PROJECT_DIR}")

            elif choice == "6":
                print("Exiting...")
                break

            else:
                print("Invalid choice")

        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


# ============================================
# ENTRY POINT
# ============================================

if __name__ == "__main__":
    # Create project directory
    os.makedirs(PROJECT_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)

    # Display welcome
    print("\n" + "=" * 60)
    print("FINAL PLC-SAP INTEGRATION PROGRAM")
    print("=" * 60)
    print("Based on all requirements:")
    print("1. PLC DB300 handshake protocol ✓")
    print("2. Box lid + 10-20 products ✓")
    print("3. Excel logging (append) ✓")
    print("4. SAP-ME XML format ✓")
    print("5. SFTP upload to Q07-CU ✓")
    print("=" * 60)

    # Check for missing SFTP password
    if not SFTP_PASS:
        print("\n⚠ ATTENTION: SFTP password not configured!")
        print(f"Username: {SFTP_USER}")
        print(f"Server: {SFTP_HOST}")
        print(f"Test folder: {REMOTE_PATH}")
        print("\nPlease:")
        print("1. Get password from your team")
        print("2. Add to SFTP_PASS variable")
        print("3. Run Test SFTP connection first")

    # Run main menu
    main()
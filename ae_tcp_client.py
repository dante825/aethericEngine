import socket
import sys
import sqlite3
import time
from io import BytesIO
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# AI-Assisted: The following code was generated with the assistance of an AI.

SERVER_IP = os.getenv('SERVER_IP')
SERVER_PORT = os.getenv('SERVER_PORT')
CURATOR_JWT = os.getenv('CURATOR_JWT')

MAX_MESSAGES = 600
DB_NAME = "aetheric_engine_data.db"

# Protocol Markers (From Prof Oshibotsu's Journal)
ASCII_START = b'$'
ASCII_END = b';'
BINARY_HEADER = b'\xAA'
BINARY_LENGTH_SIZE = 5 # The size of the payload length field (5 bytes)
TIMEOUT = 10

class AethericEngineClient:
    """A TCP client for connecting to and parsing messages from the Aetheric Engine."""
    
    def __init__(self, ip, port, jwt, db_name, max_msgs):
        self.ip = ip
        self.port = port
        self.jwt = jwt
        self.db_name = db_name
        self.max_msgs = max_msgs
        self.message_count = 0
        self.conn = None
        self.cursor = None

    def setup_database(self):
        """Initializes the SQLite database tables."""
        print("Setting up database...")
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS msgascii (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS msgbinary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload BLOB,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()
        print("Database ready.")

    def store_ascii(self, payload):
        """Stores an ASCII message payload."""
        self.cursor.execute(
            "INSERT INTO msgascii (payload) VALUES (?)", (payload.decode('ascii'),)
        )
        self.conn.commit()
        self.message_count += 1
        print(f"[{self.message_count}/{self.max_msgs}] Stored ASCII Message. Payload length: {len(payload)} bytes.")

    def store_binary(self, payload):
        """Stores a binary message payload."""
        self.cursor.execute(
            "INSERT INTO msgbinary (payload) VALUES (?)", (payload,)
        )
        self.conn.commit()
        self.message_count += 1
        print(f"[{self.message_count}/{self.max_msgs}] Stored Binary Message. Payload length: {len(payload)} bytes.")
        
    def try_parse_message(self, buffer: BytesIO) -> bool:
        """
        Attempts to read a complete message (ASCII or Binary) from the buffer.
        Returns True if a message was successfully parsed and stored, False otherwise.
        """
        # Read all data currently in the buffer
        data = buffer.getvalue()
        current_pos = buffer.tell()
        
        # Must have at least 1 byte to check for header
        if len(data) <= current_pos:
            return False

        # --- 1. Attempt to parse ASCII message ($...;) ---
        if data[current_pos:current_pos+1] == ASCII_START:
            try:
                # Find the end marker ';'
                end_index = data.index(ASCII_END, current_pos + 1)
                
                # Payload is between $ and ;
                payload = data[current_pos + 1 : end_index]
                
                # Check for minimum length constraint (5 or more random printable ASCII)
                if len(payload) >= 5:
                    self.store_ascii(payload)
                    buffer.seek(end_index + 1) # Advance buffer position past ';'
                    return True
                # Else: If payload is too short, treat as an error or unexpected format and skip
                # For this implementation, we assume a valid message will meet the minimum length.
                
            except ValueError:
                # End marker not found in current buffer chunk, need more data
                return False

        # --- 2. Attempt to parse Binary message (0xAA + 5-byte length + payload) ---
        elif data[current_pos:current_pos+1] == BINARY_HEADER:
            
            # Check for enough data to read header (1 byte) + length field (5 bytes)
            required_for_length = current_pos + 1 + BINARY_LENGTH_SIZE
            if len(data) < required_for_length:
                return False 
            
            # Read the 5-byte payload size
            length_bytes = data[current_pos + 1 : required_for_length]
            
            # Manual 5-byte Big-Endian (Network Byte Order) conversion to integer
            payload_size = 0
            for byte in length_bytes:
                payload_size = (payload_size << 8) | byte
                
            # Check if the entire payload is present
            total_message_size = 1 + BINARY_LENGTH_SIZE + payload_size
            
            if len(data) < current_pos + total_message_size:
                return False # Not enough data for the full payload
                
            # Read the payload
            payload_start = required_for_length
            payload_end = current_pos + total_message_size
            payload = data[payload_start : payload_end]
            
            self.store_binary(payload)
            buffer.seek(payload_end) # Advance buffer position past the message
            return True

        # --- 3. Neither message type found at current position ---
        # If the current byte is not a known start marker, it could be a piece of server ACK/NACK
        # from the 'AUTH' command. We will skip it to try and find the next message start.
        buffer.seek(current_pos + 1)
        print(f"Warning: Skipping unknown byte at position {current_pos}.")
        return False
        
    def run(self):
        """Connects, authenticates, listens, parses, and stores messages."""
        
        # Check if JWT token is loaded
        if not CURATOR_JWT:
            print("ERROR: CURATOR_JWT environment variable not set. Please create a .env file with CURATOR_JWT='your_token'")
            sys.exit(1)

        self.setup_database()
        
        try:
            start_time = time.time()
            print(f"[{time.time():.2f}] Connecting to {self.ip}:{self.port}...")
            # sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock = socket.create_connection((self.ip, self.port), timeout=TIMEOUT)
            connect_time = time.time() - start_time
            print(f"[{time.time():.2f}] Connection established in {connect_time:.2f}s. Authenticating...")

            
            # 1. Authenticate (Command format: "AUTH JWT_Token" followed by CRLF)
            auth_command = f"AUTH {self.jwt}"
            sock.send(auth_command.encode('ascii'))
            print(f"[{time.time():.2f}] Sent: '{auth_command}'")
            
            # # Use BytesIO to handle the continuous stream of incoming data
            buffer = BytesIO()
            first_message_time = None
                        
            # 2. Continuous Listening and Parsing Loop
            while self.message_count < self.max_msgs:
                try:
                    # Read data from the socket
                    # This is the line that was 'waiting' in the previous conversation
                    chunk = sock.recv(4096) 
                    if first_message_time is None:
                        first_message_time = time.time() - start_time
                        print(f"[{time.time():.2f}] First data received after {first_message_time:.2f}s")
                    if not chunk:
                        print("Server closed the connection unexpectedly.")
                        break
                    
                    # Add new data to the buffer and rewind to start processing
                    buffer.write(chunk)
                    buffer.seek(0) 
                    
                    messages_processed_in_chunk = 0
                    while True:
                        current_pos = buffer.tell()
                        
                        if current_pos >= len(buffer.getvalue()):
                            # Reached the end of the data we've read so far
                            break
                            
                        if self.try_parse_message(buffer):
                            messages_processed_in_chunk += 1
                            continue # Successfully parsed a message, check for another immediately
                        
                        # No complete message found at current_pos
                        remaining_data = buffer.read()
                        
                        # Clear and refill the buffer with only the remaining data
                        buffer.seek(0)
                        buffer.truncate(0)
                        buffer.write(remaining_data)
                        break 
                        
                except socket.timeout:
                    print("Socket read timed out (no data received). Attempting to gracefully close.")
                    break
                    
                except ConnectionResetError:
                    print("Connection reset by peer. Disconnecting.")
                    break
                
            
            # 3. Stop communication and disconnect
            # print(f"\nTarget of {self.max_msgs} messages reached. Sending 'STATUS' command to stop messages.")
            print('Sending STATUS command to server before disconnecting.')
            sock.sendall(b"STATUS\r\n")
            
            # Drain the pipe before disconnecting (Archaeologist's Observation)
            print("Draining TCP pipe for 5 seconds...")
            sock.settimeout(5) 
            while True:
                try:
                    drain_chunk = sock.recv(4096)
                    if not drain_chunk:
                        break 
                    print(f"Drained {len(drain_chunk)} bytes.")
                except socket.timeout:
                    break 
                
            print("Drain complete. Disconnecting.")

        except socket.error as e:
            print(f"A critical socket error occurred: {e}. Check IP/Port and network connectivity.")
            
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        finally:
            if 'sock' in locals() and sock:
                sock.close()
            if self.conn:
                self.conn.close()
            print("\nClient script finished.")


if __name__ == "__main__":
    client = AethericEngineClient(
        ip=SERVER_IP, 
        port=SERVER_PORT, 
        jwt=CURATOR_JWT, 
        db_name=DB_NAME, 
        max_msgs=MAX_MESSAGES
    )
    client.run()
import socket
import sys
import sqlite3
import time
import logging
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s.%(msecs)02d] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('aetheric_engine_client.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
        logger.info("Setting up database...")
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
        logger.info("Database ready.")

    def store_ascii(self, payload):
        """Stores an ASCII message payload."""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        self.cursor.execute(
            "INSERT INTO msgascii (payload, timestamp) VALUES (?, ?)", (payload.decode('ascii'), timestamp)
        )
        self.conn.commit()
        self.message_count += 1
        logger.info(f"[{self.message_count}/{self.max_msgs}] Stored ASCII Message. Payload length: {len(payload)} bytes.")

    def store_binary(self, payload):
        """Stores a binary message payload."""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        self.cursor.execute(
            "INSERT INTO msgbinary (payload, timestamp) VALUES (?, ?)", (payload, timestamp)
        )
        self.conn.commit()
        self.message_count += 1
        logger.info(f"[{self.message_count}/{self.max_msgs}] Stored Binary Message. Payload length: {len(payload)} bytes.")
        
    def try_parse_message(self, buffer: bytearray, pos: int) -> tuple[bool, int]:
        """
        Attempts to read a complete message (ASCII or Binary) from the buffer starting at position pos.
        Returns (message_found: bool, bytes_consumed: int).
        If a message is found, bytes_consumed is the number of bytes to skip.
        If no message is found, returns (False, 0).
        """
        # Read all data currently in the buffer
        data = buffer
        current_pos = pos
        
        # Must have at least 1 byte to check for header
        if len(data) <= current_pos:
            return (False, 0)

        # Debug: Show what byte we're looking at
        current_byte = data[current_pos:current_pos+1]
        logger.debug(f"Position {current_pos}, Byte: {current_byte.hex() if current_byte else 'EMPTY'}, Total buffer: {len(data)} bytes")

        # --- 1. Attempt to parse ASCII message ($...;) ---
        if current_byte == ASCII_START:
            logger.debug(f"Found ASCII_START ($) at position {current_pos}")
            try:
                # Find the end marker ';'
                end_index = data.index(ASCII_END, current_pos + 1)
            except ValueError:
                # End marker not found in current buffer chunk, need more data
                logger.debug(f"ASCII_END marker not found, need more data")
                return (False, 0)
            
            # Payload is between $ and ;
            payload = data[current_pos + 1 : end_index]
            logger.debug(f"ASCII payload length: {len(payload)}")
            
            # Check for minimum length constraint (5 or more random printable ASCII)
            if len(payload) >= 5:
                try:
                    self.store_ascii(payload)
                    bytes_consumed = end_index + 1 - current_pos  # Advance past ';'
                    logger.debug(f"Stored ASCII message, bytes consumed: {bytes_consumed}")
                    return (True, bytes_consumed)
                except UnicodeDecodeError as e:
                    # Payload contains non-ASCII bytes, skip this malformed message
                    logger.warning(f"ASCII message at position {current_pos} contains non-ASCII bytes: {e}. Skipping.")
                    bytes_consumed = end_index + 1 - current_pos
                    return (False, bytes_consumed)  # Skip this malformed message
            else:
                logger.debug(f"ASCII payload too short: {len(payload)} bytes (need 5+)")
                # Skip the $ marker and continue looking
                return (False, 1)

        # --- 2. Attempt to parse Binary message (0xAA + 5-byte payload) ---
        elif current_byte == BINARY_HEADER:
            logger.debug(f"Found BINARY_HEADER (0xAA) at position {current_pos}")
            
            # Check for enough data: 1 byte header + 5 bytes payload = 6 bytes total
            total_message_size = 1 + BINARY_LENGTH_SIZE  # 1 (header) + 5 (payload)
            if len(data) < current_pos + total_message_size:
                logger.debug(f"Incomplete message: need {current_pos + total_message_size} bytes total, only have {len(data)}")
                return (False, 0)
            
            # Read exactly 5 bytes after the 0xAA header
            payload_start = current_pos + 1
            payload_end = payload_start + BINARY_LENGTH_SIZE  # 5 bytes
            
            payload = data[payload_start : payload_end]
            logger.debug(f"Extracted 5-byte binary payload: {payload.hex()}")
            
            self.store_binary(payload)
            # Return bytes consumed (header + payload = 6 bytes)
            bytes_consumed = total_message_size
            return (True, bytes_consumed)

        # --- 3. Neither message type found at current position ---
        else:
            # If the current byte is not a known start marker, it could be a piece of server ACK/NACK
            # from the 'AUTH' command. We will skip ahead to find the next message start marker.
            try:
                # Find the next $ or 0xAA marker
                next_ascii = data.find(ASCII_START, current_pos + 1)
                next_binary = data.find(BINARY_HEADER, current_pos + 1)
                
                # Find which comes first
                next_pos = None
                if next_ascii != -1 and next_binary != -1:
                    next_pos = min(next_ascii, next_binary)
                elif next_ascii != -1:
                    next_pos = next_ascii
                elif next_binary != -1:
                    next_pos = next_binary
                
                if next_pos is not None:
                    bytes_to_skip = next_pos - current_pos
                    logger.debug(f"Skipping {bytes_to_skip} unknown bytes at position {current_pos} to reach next marker at {next_pos}")
                    return (False, bytes_to_skip)
                else:
                    # No marker found ahead, skip rest of buffer
                    logger.debug(f"No message markers found ahead, skipping remaining {len(data) - current_pos} bytes")
                    return (False, len(data) - current_pos)
            except Exception as e:
                logger.warning(f"Error while looking for next marker: {e}. Skipping 1 byte.")
                return (False, 1)

    def run(self):
        """Connects, authenticates, listens, parses, and stores messages."""
        
        # Check if JWT token is loaded
        if not CURATOR_JWT:
            logger.error("CURATOR_JWT environment variable not set. Please create a .env file with CURATOR_JWT='your_token'")
            sys.exit(1)

        self.setup_database()
        
        try:
            start_time = time.time()
            logger.info(f"Connecting to {self.ip}:{self.port}...")
            # sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock = socket.create_connection((self.ip, self.port), timeout=TIMEOUT)
            sock.settimeout(TIMEOUT)  # Ensure socket timeout is set for all recv calls
            connect_time = time.time() - start_time
            logger.info(f"Connection established in {connect_time:.2f}s. Authenticating...")

            
            # 1. Authenticate (Command format: "AUTH JWT_Token" 
            auth_command = f"AUTH {self.jwt}"
            sock.send(auth_command.encode('ascii'))
            logger.info(f"Sent: '{auth_command}'")
            
            # Use bytearray to handle the continuous stream of incoming data
            buffer = bytearray()
            buffer_pos = 0
            first_message_time = None
            last_message_time = time.time()
                        
            # 2. Continuous Listening and Parsing Loop
            while self.message_count < self.max_msgs:
                try:
                    # Read data from the socket with timeout
                    logger.debug(f"Waiting for message {self.message_count + 1}/{self.max_msgs}...")
                    chunk = sock.recv(4096) 
                    if first_message_time is None:
                        first_message_time = time.time() - start_time
                        logger.info(f"First data received after {first_message_time:.2f}s")
                    if not chunk:
                        logger.info("Server closed the connection unexpectedly.")
                        break
                    
                    last_message_time = time.time()
                    logger.debug(f"Received {len(chunk)} bytes")
                    
                    # Add new data to the buffer
                    buffer.extend(chunk)
                    
                    messages_processed_in_chunk = 0
                    while buffer_pos < len(buffer):
                        # Try to parse a message at current position
                        found, bytes_consumed = self.try_parse_message(buffer, buffer_pos)
                        
                        if not found and bytes_consumed == 0:
                            # No complete message found, need more data
                            logger.debug(f"Processed {messages_processed_in_chunk} message(s) from this chunk")
                            break
                        
                        buffer_pos += bytes_consumed
                        if found:
                            messages_processed_in_chunk += 1
                    
                    # Remove processed data from buffer
                    if buffer_pos > 0:
                        del buffer[:buffer_pos]
                        buffer_pos = 0
                        logger.debug(f"Removed {buffer_pos} bytes from buffer, {len(buffer)} bytes remaining") 
                        
                except socket.timeout:
                    logger.warning(f"Socket read timed out after {TIMEOUT}s with no data. Total messages received: {self.message_count}")
                    break
                    
                except ConnectionResetError:
                    logger.error("Connection reset by peer. Disconnecting.")
                    break
                
            
            # 3. Stop communication and disconnect
            # logger.info(f"Target of {self.max_msgs} messages reached. Sending 'STATUS' command to stop messages.")
            logger.info('Sending STATUS command to server before disconnecting.')
            sock.sendall(b"STATUS\r\n")
            
            # Drain the pipe before disconnecting (Archaeologist's Observation)
            logger.info("Draining TCP pipe for 5 seconds...")
            sock.settimeout(5) 
            while True:
                try:
                    drain_chunk = sock.recv(4096)
                    if not drain_chunk:
                        break 
                    logger.debug(f"Drained {len(drain_chunk)} bytes.")
                except socket.timeout:
                    break 
                
            logger.info("Drain complete. Disconnecting.")

        except socket.error as e:
            logger.error(f"A critical socket error occurred: {e}. Check IP/Port and network connectivity.")
            
        except KeyboardInterrupt:
            logger.warning("Interrupted by user (Ctrl-C). Cleaning up gracefully...")
            try:
                logger.info('Sending STATUS command to server.')
                sock.sendall(b"STATUS\r\n")
                logger.info("Draining TCP pipe for 2 seconds...")
                sock.settimeout(2) 
                while True:
                    try:
                        drain_chunk = sock.recv(4096)
                        if not drain_chunk:
                            break 
                        logger.debug(f"Drained {len(drain_chunk)} bytes.")
                    except socket.timeout:
                        break 
            except Exception as e:
                logger.debug(f"Error during graceful shutdown: {e}")
            
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")

        finally:
            if 'sock' in locals() and sock:
                sock.close()
            if self.conn:
                self.conn.close()
            logger.info("Client script finished.")


if __name__ == "__main__":
    client = AethericEngineClient(
        ip=SERVER_IP, 
        port=SERVER_PORT, 
        jwt=CURATOR_JWT, 
        db_name=DB_NAME, 
        max_msgs=MAX_MESSAGES
    )
    client.run()
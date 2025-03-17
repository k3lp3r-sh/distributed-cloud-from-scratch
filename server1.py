import socket
import threading
import os
import time

# ----- CONFIGURATION -----
SERVER_ID = "server1"
OWN_IP = "127.0.0.1"      # Change if needed
OWN_PORT = 8001
PEER_IP = "127.0.0.1"     # Peer server IP
PEER_PORT = 8002
DATA_DIR = "data"         # Directory where files will be stored
THROTTLE_DELAY = 0.1      # Delay (in seconds) per chunk if peer is down

# ----- GLOBALS -----
current_client = None
client_lock = threading.Lock()


def ensure_user_dir(user_id):
    user_dir = os.path.join(DATA_DIR, user_id)
    os.makedirs(user_dir, exist_ok=True)
    return user_dir


def is_peer_up():
    # Try to quickly connect to the peer server
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((PEER_IP, PEER_PORT))
        s.close()
        return True
    except Exception:
        return False


def replicate_file(user_id, filename, file_path):
    try:
        filesize = os.path.getsize(file_path)
        # Connect to the peer server
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((PEER_IP, PEER_PORT))
        # Identify as replication traffic
        s.sendall("TYPE:REPL".encode())
        time.sleep(0.1)
        header = f"REPL|{user_id}|{filename}|{filesize}"
        s.sendall(header.encode())
        time.sleep(0.1)
        with open(file_path, "rb") as f:
            while (chunk := f.read(4096)):
                s.sendall(chunk)
        s.close()
        print(f"[{SERVER_ID}] Replicated file '{filename}' for user '{user_id}' to peer.")
    except Exception as e:
        print(f"[{SERVER_ID}] Replication failed: {e}")


def handle_connection(conn, addr):
    global current_client
    try:
        # Read initial header to determine connection type
        data = conn.recv(1024).decode()
        if data.startswith("TYPE:CLIENT"):
            # For client connections, enforce one active client if peer is up
            with client_lock:
                if current_client is not None:
                    if is_peer_up():
                        conn.sendall("BUSY".encode())
                        conn.close()
                        return
                    else:
                        conn.sendall("SLOW".encode())
                else:
                    current_client = addr
                    conn.sendall("OK".encode())

            # Now receive the upload command
            header = conn.recv(1024).decode()  # Expected format: UPLOAD|<user_id>|<filename>|<filesize>
            parts = header.split("|")
            if len(parts) != 4 or parts[0] != "UPLOAD":
                conn.sendall("ERROR: Invalid command".encode())
                return
            user_id, filename, filesize_str = parts[1], parts[2], parts[3]
            try:
                filesize = int(filesize_str)
            except ValueError:
                conn.sendall("ERROR: Invalid file size".encode())
                return

            user_dir = ensure_user_dir(user_id)
            file_path = os.path.join(user_dir, filename)
            routing_info = f"{OWN_IP}:{OWN_PORT},{PEER_IP}:{PEER_PORT}"
            routing_path = file_path + ".routing"

            # Receive file data in binary chunks
            received = 0
            with open(file_path, "wb") as f:
                while received < filesize:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    f.write(chunk)
                    received += len(chunk)
                    # If peer is down, throttle the transfer
                    if not is_peer_up():
                        time.sleep(THROTTLE_DELAY)

            # Save routing metadata
            with open(routing_path, "w") as meta:
                meta.write(routing_info)
            conn.sendall("UPLOAD SUCCESS".encode())
            print(f"[{SERVER_ID}] Received file '{filename}' from user '{user_id}'.")

            # Replicate the file to the peer
            replicate_file(user_id, filename, file_path)

        elif data.startswith("TYPE:REPL"):
            # This is a replication connection from the peer server
            header = conn.recv(1024).decode()  # Expected: REPL|<user_id>|<filename>|<filesize>
            parts = header.split("|")
            if len(parts) != 4 or parts[0] != "REPL":
                conn.close()
                return
            user_id, filename, filesize_str = parts[1], parts[2], parts[3]
            try:
                filesize = int(filesize_str)
            except ValueError:
                conn.close()
                return

            user_dir = ensure_user_dir(user_id)
            file_path = os.path.join(user_dir, filename)
            routing_info = f"{OWN_IP}:{OWN_PORT},{PEER_IP}:{PEER_PORT}"
            routing_path = file_path + ".routing"

            received = 0
            with open(file_path, "wb") as f:
                while received < filesize:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    f.write(chunk)
                    received += len(chunk)
            with open(routing_path, "w") as meta:
                meta.write(routing_info)
            print(f"[{SERVER_ID}] Replicated file '{filename}' stored for user '{user_id}'.")
        else:
            conn.sendall("ERROR: Unknown connection type".encode())
    except Exception as e:
        print(f"[{SERVER_ID}] Error handling connection from {addr}: {e}")
    finally:
        with client_lock:
            if current_client == addr:
                current_client = None
        conn.close()


def server_main():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((OWN_IP, OWN_PORT))
    server_socket.listen(5)
    print(f"[{SERVER_ID}] Listening on {OWN_IP}:{OWN_PORT}")
    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    server_main()


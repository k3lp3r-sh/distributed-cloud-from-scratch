import socket
import os
import time

# ----- CONFIGURATION -----
SERVERS = [("127.0.0.1", 8001), ("127.0.0.1", 8002)]
USER_ID = "user1"
FILE_TO_UPLOAD = "file1.txt"  # Make sure this file exists in the same folder

def upload_file(server_ip, server_port, user_id, file_path):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((server_ip, server_port))
        # Identify as a client
        s.sendall("TYPE:CLIENT".encode())
        response = s.recv(1024).decode()
        if response == "BUSY":
            s.close()
            return False, "BUSY"
        elif response == "SLOW":
            print(f"[CLIENT {user_id}] Connected to {server_ip}:{server_port} in SLOW mode.")
        elif response == "OK":
            print(f"[CLIENT {user_id}] Connected to {server_ip}:{server_port}.")

        filesize = os.path.getsize(file_path)
        filename = os.path.basename(file_path)
        header = f"UPLOAD|{user_id}|{filename}|{filesize}"
        s.sendall(header.encode())
        time.sleep(0.1)  # small pause before sending file data
        with open(file_path, "rb") as f:
            while (chunk := f.read(4096)):
                s.sendall(chunk)
        response = s.recv(1024).decode()
        print(f"[CLIENT {user_id}] Server response: {response}")
        s.close()
        return True, response
    except Exception as e:
        print(f"[CLIENT {user_id}] Error connecting to {server_ip}:{server_port}: {e}")
        return False, str(e)

def main():
    for ip, port in SERVERS:
        print(f"[CLIENT {USER_ID}] Trying server {ip}:{port}")
        success, message = upload_file(ip, port, USER_ID, FILE_TO_UPLOAD)
        if success:
            break
        else:
            print(f"[CLIENT {USER_ID}] Could not use server {ip}:{port} ({message}). Trying next server...")
    else:
        print(f"[CLIENT {USER_ID}] No server available to process the upload.")

if __name__ == "__main__":
    main()

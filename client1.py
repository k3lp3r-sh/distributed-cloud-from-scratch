import socket
import json
import os
import logging
from pathlib import Path
import time

# Client Configuration
USER_ID = 'user1'
SERVER_HOSTS = ['127.0.0.1', '127.0.0.1']
SERVER_PORTS = [5000, 5001]
DOWNLOAD_PATH = Path('./client1_downloads')

# Create download directory if it doesn't exist
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("client1_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Client:
    def __init__(self, user_id, server_hosts, server_ports):
        self.user_id = user_id
        self.server_hosts = server_hosts
        self.server_ports = server_ports
        self.current_server_index = 0

    def connect_to_available_server(self):
        """Try to connect to an available server."""
        for i in range(len(self.server_hosts)):
            idx = (self.current_server_index + i) % len(self.server_hosts)
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect((self.server_hosts[idx], self.server_ports[idx]))
                self.current_server_index = idx
                logger.info(f"Connected to server at {self.server_hosts[idx]}:{self.server_ports[idx]}")
                return s
            except (socket.timeout, ConnectionRefusedError):
                logger.warning(f"Server at {self.server_hosts[idx]}:{self.server_ports[idx]} is not available")
                continue
        raise ConnectionError("All servers are unavailable")

    def request_lock(self, resource_id):
        """Request a lock for a specific resource using Maekawa's algorithm."""
        logger.info(f"Requesting lock for resource: {resource_id}")
        try:
            with self.connect_to_available_server() as s:
                request = {
                    'user_id': self.user_id,
                    'action': 'request_lock',
                    'resource_id': resource_id
                }
                
                s.sendall(json.dumps(request).encode('utf-8'))
                s.settimeout(10)
                response = s.recv(1024).decode('utf-8')
                response_data = json.loads(response)
                
                if response_data.get('status') == 'redirect':
                    host = response_data.get('host')
                    port = response_data.get('port')
                    logger.info(f"Redirected to server at {host}:{port}")
                    try:
                        idx = self.server_hosts.index(host)
                        if self.server_ports[idx] == port:
                            self.current_server_index = idx
                    except ValueError:
                        pass
                    return self.request_lock(resource_id)
                
                if response_data.get('status') == 'success':
                    logger.info(f"Lock acquired for resource: {resource_id}")
                    return True
                else:
                    logger.warning(f"Failed to acquire lock: {response_data.get('message')}")
                    return False
        except Exception as e:
            logger.error(f"Error requesting lock: {e}")
            return False

    def release_lock(self, resource_id):
        """Release a lock for a specific resource."""
        logger.info(f"Releasing lock for resource: {resource_id}")
        try:
            with self.connect_to_available_server() as s:
                request = {
                    'user_id': self.user_id,
                    'action': 'release_lock',
                    'resource_id': resource_id
                }
                s.sendall(json.dumps(request).encode('utf-8'))
                
                response = s.recv(1024).decode('utf-8')
                response_data = json.loads(response)
                
                if response_data.get('status') == 'redirect':
                    host = response_data.get('host')
                    port = response_data.get('port')
                    logger.info(f"Redirected to server at {host}:{port}")
                    try:
                        idx = self.server_hosts.index(host)
                        if self.server_ports[idx] == port:
                            self.current_server_index = idx
                    except ValueError:
                        pass
                    return self.release_lock(resource_id)
                
                logger.info(f"Lock released for resource: {resource_id}")
                return response_data.get('status') == 'success'
        except Exception as e:
            logger.error(f"Error releasing lock: {e}")
            return False

    def list_files(self):
        """List all files stored for this user on the server."""
        logger.info("LISTING: Retrieving file list from server")
        try:
            with self.connect_to_available_server() as s:
                request = {
                    'user_id': self.user_id,
                    'action': 'list'
                }
                s.sendall(json.dumps(request).encode('utf-8'))
                
                response = s.recv(1024).decode('utf-8')
                response_data = json.loads(response)
                
                if response_data.get('status') == 'redirect':
                    host = response_data.get('host')
                    port = response_data.get('port')
                    logger.info(f"Redirected to server at {host}:{port}")
                    try:
                        idx = self.server_hosts.index(host)
                        if self.server_ports[idx] == port:
                            self.current_server_index = idx
                    except ValueError:
                        pass
                    return self.list_files()
                
                files = response_data.get('files', [])
                logger.info(f"LISTING SUCCESS: Found {len(files)} files")
                return files
        except Exception as e:
            logger.error(f"LISTING FAILED: {e}")
            return []

    def upload_file(self, file_path):
        """Upload a file to the server with improved error handling."""
        file_path = Path(file_path)
        logger.info(f"UPLOADING: {file_path} to server")
        
        if not file_path.exists():
            logger.error(f"UPLOAD FAILED: File not found: {file_path}")
            return False

        # First acquire lock for this file
        if not self.request_lock(file_path.name):
            logger.error(f"UPLOAD FAILED: Could not acquire lock for {file_path.name}")
            return False

        try:
            with open(file_path, 'rb') as f:
                file_data = f.read()
                
            with self.connect_to_available_server() as s:
                request = {
                    'user_id': self.user_id,
                    'action': 'upload',
                    'filename': file_path.name
                }
                s.sendall(json.dumps(request).encode('utf-8'))
                
                # Wait for server to be ready for data
                response = s.recv(1024)
                if response != b'READY_FOR_DATA':
                    response_data = json.loads(response.decode('utf-8'))
                    if response_data.get('status') == 'redirect':
                        host = response_data.get('host')
                        port = response_data.get('port')
                        logger.info(f"Redirected to server at {host}:{port}")
                        try:
                            idx = self.server_hosts.index(host)
                            if self.server_ports[idx] == port:
                                self.current_server_index = idx
                        except ValueError:
                            pass
                        self.release_lock(file_path.name)
                        return self.upload_file(file_path)
                    
                    logger.error(f"UPLOAD FAILED: Server not ready: {response.decode('utf-8')}")
                    return False
                
                # Send file data
                s.sendall(file_data)
                s.shutdown(socket.SHUT_WR)
                
                # Set a longer timeout for server response
                s.settimeout(30)
                
                try:
                    # Get confirmation
                    response = s.recv(1024).decode('utf-8')
                    response_data = json.loads(response)
                    logger.info(response_data.get('message', 'No message from server'))
                    success = response_data.get('status') == 'success'
                    
                    if success:
                        logger.info(f"UPLOAD SUCCESS: {file_path.name} successfully uploaded")
                    else:
                        logger.error(f"UPLOAD FAILED: {file_path.name}")
                    
                    return success
                except socket.timeout:
                    # Verification step: check if file was uploaded despite timeout
                    logger.warning("Connection timed out, verifying upload status...")
                    time.sleep(1)  # Give server a moment to process
                    files = self.list_files()
                    if file_path.name in files:
                        logger.info(f"UPLOAD SUCCESS: File appears in server listing, upload was successful")
                        return True
                    logger.error("UPLOAD FAILED: File not found in listing after timeout")
                    return False
        finally:
            # Always release the lock
            self.release_lock(file_path.name)

    def download_file(self, filename):
        """Download a file from the server."""
        logger.info(f"DOWNLOADING: {filename} from server")
        try:
            with self.connect_to_available_server() as s:
                request = {
                    'user_id': self.user_id,
                    'action': 'download',
                    'filename': filename
                }
                s.sendall(json.dumps(request).encode('utf-8'))
                
                response = s.recv(1024).decode('utf-8')
                response_data = json.loads(response)
                
                if response_data.get('status') == 'redirect':
                    host = response_data.get('host')
                    port = response_data.get('port')
                    logger.info(f"Redirected to server at {host}:{port}")
                    try:
                        idx = self.server_hosts.index(host)
                        if self.server_ports[idx] == port:
                            self.current_server_index = idx
                    except ValueError:
                        pass
                    return self.download_file(filename)
                
                if response_data.get('status') != 'success':
                    logger.error(f"DOWNLOAD FAILED: {response_data.get('message', 'Unknown error')}")
                    return False
                
                # Acknowledge we're ready to receive the file
                s.sendall(b'READY_FOR_DATA')
                
                # Receive the file data
                file_data = b''
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    file_data += chunk
                
                # Save the file
                output_path = DOWNLOAD_PATH / filename
                with open(output_path, 'wb') as f:
                    f.write(file_data)
                
                logger.info(f"DOWNLOAD SUCCESS: {filename} saved to {output_path}")
                return True
        except Exception as e:
            logger.error(f"DOWNLOAD FAILED: {e}")
            return False

    def delete_file(self, filename):
        """Delete a file from the server."""
        logger.info(f"DELETING: {filename} from server")
        
        # First acquire lock for this file
        if not self.request_lock(filename):
            logger.error(f"DELETE FAILED: Could not acquire lock for {filename}")
            return False

        try:
            with self.connect_to_available_server() as s:
                request = {
                    'user_id': self.user_id,
                    'action': 'delete',
                    'filename': filename
                }
                s.sendall(json.dumps(request).encode('utf-8'))
                
                response = s.recv(1024).decode('utf-8')
                response_data = json.loads(response)
                
                if response_data.get('status') == 'redirect':
                    host = response_data.get('host')
                    port = response_data.get('port')
                    logger.info(f"Redirected to server at {host}:{port}")
                    try:
                        idx = self.server_hosts.index(host)
                        if self.server_ports[idx] == port:
                            self.current_server_index = idx
                    except ValueError:
                        pass
                    self.release_lock(filename)
                    return self.delete_file(filename)
                
                success = response_data.get('status') == 'success'
                if success:
                    logger.info(f"DELETE SUCCESS: {filename} removed from server")
                else:
                    logger.error(f"DELETE FAILED: {response_data.get('message')}")
                
                return success
        finally:
            # Always release the lock
            self.release_lock(filename)

def display_menu():
    """Display the main menu."""
    print("\n==== Distributed Cloud Client ====")
    print(f"User: {USER_ID}")
    print("1. List my files")
    print("2. Upload a file")
    print("3. Download a file")
    print("4. Delete a file")
    print("5. Exit")
    return input("Select an option (1-5): ")

def main():
    client = Client(USER_ID, SERVER_HOSTS, SERVER_PORTS)
    
    while True:
        choice = display_menu()
        
        if choice == '1':
            print("\nYour files:")
            files = client.list_files()
            if files:
                for i, file in enumerate(files, 1):
                    print(f"{i}. {file}")
            else:
                print("No files found.")
        
        elif choice == '2':
            file_path = input("\nEnter the path of the file to upload: ")
            client.upload_file(file_path)
        
        elif choice == '3':
            files = client.list_files()
            if not files:
                print("No files available to download.")
                continue
                
            print("\nYour files:")
            for i, file in enumerate(files, 1):
                print(f"{i}. {file}")
                
            try:
                file_idx = int(input("Enter the number of the file to download: ")) - 1
                if 0 <= file_idx < len(files):
                    client.download_file(files[file_idx])
                else:
                    print("Invalid file number.")
            except ValueError:
                print("Please enter a valid number.")
        
        elif choice == '4':
            files = client.list_files()
            if not files:
                print("No files available to delete.")
                continue
                
            print("\nYour files:")
            for i, file in enumerate(files, 1):
                print(f"{i}. {file}")
                
            try:
                file_idx = int(input("Enter the number of the file to delete: ")) - 1
                if 0 <= file_idx < len(files):
                    confirm = input(f"Are you sure you want to delete '{files[file_idx]}'? (y/n): ")
                    if confirm.lower() == 'y':
                        client.delete_file(files[file_idx])
                    else:
                        print("Deletion cancelled.")
                else:
                    print("Invalid file number.")
            except ValueError:
                print("Please enter a valid number.")
        
        elif choice == '5':
            print("\nExiting client. Goodbye!")
            break
            
        else:
            print("\nInvalid choice. Please try again.")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()

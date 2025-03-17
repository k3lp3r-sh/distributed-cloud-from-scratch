import socket
import json
import os
import time
from pathlib import Path

# Client Configuration
USER_ID = 'user1'  # Constant, unchanging user ID
SERVER_HOSTS = ['127.0.0.1', '127.0.0.1']  # IP addresses of both servers
SERVER_PORTS = [5000, 5001]  # Ports of both servers
DOWNLOAD_PATH = Path(f'./client1_downloads')  # Path to save downloaded files

# Create download directory if it doesn't exist
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

class Client:
    def __init__(self, user_id, server_hosts, server_ports):
        self.user_id = user_id
        self.server_hosts = server_hosts
        self.server_ports = server_ports
        self.current_server_index = 0  # Default to the first server
        
    def connect_to_available_server(self):
        """Try to connect to an available server."""
        # Try each server in order
        for i in range(len(self.server_hosts)):
            idx = (self.current_server_index + i) % len(self.server_hosts)
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)  # Set a timeout for connection attempts
                s.connect((self.server_hosts[idx], self.server_ports[idx]))
                self.current_server_index = idx  # Remember this working server for next time
                print(f"Connected to server at {self.server_hosts[idx]}:{self.server_ports[idx]}")
                return s
            except (socket.timeout, ConnectionRefusedError):
                print(f"Server at {self.server_hosts[idx]}:{self.server_ports[idx]} is not available")
                continue
        
        raise ConnectionError("All servers are unavailable")
    
    def list_files(self):
        """List all files stored for this user on the server."""
        try:
            with self.connect_to_available_server() as s:
                request = {
                    'user_id': self.user_id,
                    'action': 'list'
                }
                s.sendall(json.dumps(request).encode('utf-8'))
                
                # Receive response
                response = s.recv(1024).decode('utf-8')
                response_data = json.loads(response)
                
                # Check for redirect
                if response_data.get('status') == 'redirect':
                    # Connect to the redirected server
                    host = response_data.get('host')
                    port = response_data.get('port')
                    print(f"Redirected to server at {host}:{port}")
                    
                    # Update the current server index based on the redirect
                    try:
                        idx = self.server_hosts.index(host)
                        if self.server_ports[idx] == port:
                            self.current_server_index = idx
                    except ValueError:
                        # If we can't find the exact match, just continue with the next attempt
                        pass
                    
                    # Try again with the next server
                    return self.list_files()
                
                return response_data.get('files', [])
        
        except Exception as e:
            print(f"Error listing files: {e}")
            return []
    
    def upload_file(self, file_path):
        """Upload a file to the server."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                print(f"File not found: {file_path}")
                return False
            
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
                    
                    # Check for redirect
                    if response_data.get('status') == 'redirect':
                        # Connect to the redirected server
                        host = response_data.get('host')
                        port = response_data.get('port')
                        print(f"Redirected to server at {host}:{port}")
                        
                        # Update the current server index based on the redirect
                        try:
                            idx = self.server_hosts.index(host)
                            if self.server_ports[idx] == port:
                                self.current_server_index = idx
                        except ValueError:
                            # If we can't find the exact match, just continue with the next attempt
                            pass
                        
                        # Try again with the next server
                        return self.upload_file(file_path)
                    
                    print(f"Server not ready: {response.decode('utf-8')}")
                    return False
                
                # Send file data
                s.sendall(file_data)
                s.shutdown(socket.SHUT_WR)  # Signal that we're done sending
                
                # Get confirmation
                response = s.recv(1024).decode('utf-8')
                response_data = json.loads(response)
                
                print(response_data.get('message', 'No message from server'))
                return response_data.get('status') == 'success'
        
        except Exception as e:
            print(f"Error uploading file: {e}")
            return False
    
    def download_file(self, filename):
        """Download a file from the server."""
        try:
            with self.connect_to_available_server() as s:
                request = {
                    'user_id': self.user_id,
                    'action': 'download',
                    'filename': filename
                }
                s.sendall(json.dumps(request).encode('utf-8'))
                
                # Receive initial response
                response = s.recv(1024).decode('utf-8')
                response_data = json.loads(response)
                
                # Check for redirect
                if response_data.get('status') == 'redirect':
                    # Connect to the redirected server
                    host = response_data.get('host')
                    port = response_data.get('port')
                    print(f"Redirected to server at {host}:{port}")
                    
                    # Update the current server index based on the redirect
                    try:
                        idx = self.server_hosts.index(host)
                        if self.server_ports[idx] == port:
                            self.current_server_index = idx
                    except ValueError:
                        # If we can't find the exact match, just continue with the next attempt
                        pass
                    
                    # Try again with the next server
                    return self.download_file(filename)
                
                if response_data.get('status') != 'success':
                    print(f"Error: {response_data.get('message', 'Unknown error')}")
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
                
                print(f"File downloaded successfully: {output_path}")
                return True
        
        except Exception as e:
            print(f"Error downloading file: {e}")
            return False

def display_menu():
    """Display the main menu."""
    print("\n==== Distributed Cloud Client ====")
    print(f"User: {USER_ID}")
    print("1. List my files")
    print("2. Upload a file")
    print("3. Download a file")
    print("4. Exit")
    return input("Select an option (1-4): ")

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
            print("\nExiting client. Goodbye!")
            break
        
        else:
            print("\nInvalid choice. Please try again.")
        
        # Pause to let the user read the output
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()


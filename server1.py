import socket
import threading
import os
import time
import json
import shutil
from pathlib import Path

# Server configuration
SERVER_HOST = '127.0.0.1'  # localhost
SERVER_PORT = 5000
OTHER_SERVER_HOST = '127.0.0.1'  # localhost for other server too
OTHER_SERVER_PORT = 5001
SERVER_ID = 'server1'

# File storage path
BASE_STORAGE_PATH = Path('./server1_storage')

# Create storage directory if it doesn't exist
os.makedirs(BASE_STORAGE_PATH, exist_ok=True)

# Server state
server_state = {
    'is_running': True,
    'connected_users': set(),
    'transfer_speed_factor': 1.0  # 1.0 means full speed
}

def get_user_folder(user_id):
    """Get the folder path for a specific user."""
    user_folder = BASE_STORAGE_PATH / user_id
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def list_files(user_id):
    """List all files for a specific user."""
    user_folder = get_user_folder(user_id)
    files = [f.name for f in user_folder.iterdir() if f.is_file()]
    return files

def save_file(user_id, filename, data):
    """Save a file to the user's storage."""
    user_folder = get_user_folder(user_id)
    file_path = user_folder / filename
    
    with open(file_path, 'wb') as f:
        f.write(data)
    
    # Try to sync with the other server
    try:
        sync_with_other_server(user_id, filename, data)
    except Exception as e:
        print(f"Failed to sync with other server: {e}")

def get_file(user_id, filename):
    """Retrieve a file from the user's storage."""
    user_folder = get_user_folder(user_id)
    file_path = user_folder / filename
    
    if file_path.exists():
        with open(file_path, 'rb') as f:
            return f.read()
    else:
        return None

def sync_with_other_server(user_id, filename, data):
    """Synchronize a file with the other server."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((OTHER_SERVER_HOST, OTHER_SERVER_PORT))
            
            # Prepare sync message
            sync_msg = {
                'action': 'sync',
                'user_id': user_id,
                'filename': filename,
                'server_id': SERVER_ID
            }
            
            # Send sync metadata
            s.sendall(json.dumps(sync_msg).encode('utf-8'))
            
            # Wait for acknowledgment
            s.recv(1024)
            
            # Send the file data
            s.sendall(data)
            
            # Wait for confirmation
            response = s.recv(1024).decode('utf-8')
            
            if response != 'SYNC_SUCCESS':
                print(f"Sync failed: {response}")
    except Exception as e:
        print(f"Failed to connect to other server for sync: {e}")
        raise

def handle_sync_request(client_socket, request):
    """Handle a synchronization request from the other server."""
    user_id = request['user_id']
    filename = request['filename']
    
    # Acknowledge the sync request
    client_socket.sendall(b'SYNC_ACK')
    
    # Receive the file data
    data = b''
    while True:
        chunk = client_socket.recv(4096)
        if not chunk:
            break
        data += chunk
    
    # Save the file locally (without trying to sync back)
    user_folder = get_user_folder(user_id)
    file_path = user_folder / filename
    
    with open(file_path, 'wb') as f:
        f.write(data)
    
    # Send confirmation
    client_socket.sendall(b'SYNC_SUCCESS')

def handle_client(client_socket, addr):
    """Handle client connection."""
    try:
        # Receive initial message from client
        data = client_socket.recv(1024).decode('utf-8')
        request = json.loads(data)
        
        user_id = request.get('user_id')
        action = request.get('action')
        
        # If this is a ping request
        if action == 'ping':
            client_socket.sendall(b'PONG')
            return
        
        # If this is a sync request from the other server
        if action == 'sync':
            handle_sync_request(client_socket, request)
            return
        
        # Check if this server is already handling the max users
        other_server_up = is_other_server_running()
        
        if other_server_up and len(server_state['connected_users']) >= 1:
            # Redirect to other server
            response = {
                'status': 'redirect',
                'host': OTHER_SERVER_HOST,
                'port': OTHER_SERVER_PORT,
                'message': 'This server is at capacity. Redirecting to another server.'
            }
            client_socket.sendall(json.dumps(response).encode('utf-8'))
            return
        
        # Add user to connected users
        server_state['connected_users'].add(user_id)
        
        # Adjust transfer speed based on server availability
        if not other_server_up:
            server_state['transfer_speed_factor'] = 0.5  # Reduce speed to 50% when the other server is down
        else:
            server_state['transfer_speed_factor'] = 1.0
        
        # Process the client's request
        if action == 'list':
            files = list_files(user_id)
            response = {'status': 'success', 'files': files}
            client_socket.sendall(json.dumps(response).encode('utf-8'))
            
        elif action == 'upload':
            filename = request.get('filename')
            
            # Acknowledge the request
            client_socket.sendall(b'READY_FOR_DATA')
            
            # Receive the file data
            data = b''
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                
                # Simulate slower transfer if needed
                if server_state['transfer_speed_factor'] < 1.0:
                    time.sleep(0.01 / server_state['transfer_speed_factor'])
            
            # Save the file
            save_file(user_id, filename, data)
            
            response = {'status': 'success', 'message': f'File {filename} uploaded successfully'}
            client_socket.sendall(json.dumps(response).encode('utf-8'))
            
        elif action == 'download':
            filename = request.get('filename')
            data = get_file(user_id, filename)
            
            if data:
                # First send success message
                response = {'status': 'success', 'message': 'File found, preparing download'}
                client_socket.sendall(json.dumps(response).encode('utf-8'))
                
                # Wait for client acknowledgment
                client_socket.recv(1024)
                
                # Send the file data
                for i in range(0, len(data), 4096):
                    chunk = data[i:i+4096]
                    client_socket.sendall(chunk)
                    
                    # Simulate slower transfer if needed
                    if server_state['transfer_speed_factor'] < 1.0:
                        time.sleep(0.01 / server_state['transfer_speed_factor'])
            else:
                response = {'status': 'error', 'message': 'File not found'}
                client_socket.sendall(json.dumps(response).encode('utf-8'))
        
        else:
            response = {'status': 'error', 'message': 'Invalid action'}
            client_socket.sendall(json.dumps(response).encode('utf-8'))
    
    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    
    finally:
        # Remove user from connected users
        if 'user_id' in locals() and user_id:
            server_state['connected_users'].discard(user_id)
        
        client_socket.close()

def is_other_server_running():
    """Check if the other server is running."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect((OTHER_SERVER_HOST, OTHER_SERVER_PORT))
            ping_msg = {'action': 'ping', 'server_id': SERVER_ID}
            s.sendall(json.dumps(ping_msg).encode('utf-8'))
            response = s.recv(1024).decode('utf-8')
            return response == 'PONG'
    except:
        return False

def run_server():
    """Run the server and listen for connections."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind((SERVER_HOST, SERVER_PORT))
        server_socket.listen(5)
        print(f"Server running on {SERVER_HOST}:{SERVER_PORT}")
        
        while server_state['is_running']:
            try:
                client_socket, addr = server_socket.accept()
                print(f"Connection from {addr}")
                
                # Handle client in a new thread
                client_thread = threading.Thread(target=handle_client, args=(client_socket, addr))
                client_thread.daemon = True
                client_thread.start()
            
            except Exception as e:
                print(f"Error accepting connection: {e}")
    
    finally:
        server_socket.close()

if __name__ == "__main__":
    try:
        run_server()
    except KeyboardInterrupt:
        print("Server shutting down...")
        server_state['is_running'] = False


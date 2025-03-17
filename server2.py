import socket
import threading
import json
import os
from pathlib import Path
import time
import logging
from datetime import datetime
import math

# Server configuration
SERVER_ID = 1
HOST = '127.0.0.1'
PORT = 5001
STORAGE_DIR = Path('./storage')

# Server info for the whole cluster
SERVER_INFO = {
    0: ('127.0.0.1', 5000),
    1: ('127.0.0.1', 5001)
}

class FileOperationLogger:
    def __init__(self, server_id, log_file_path=None):
        """Initialize the operation logger."""
        # If no log file path specified, create one based on server ID
        if log_file_path is None:
            log_dir = Path('./logs')
            os.makedirs(log_dir, exist_ok=True)
            log_file_path = log_dir / f"server{server_id}_operations.log"
        
        self.log_file_path = log_file_path
        self.server_id = server_id
        self.lock = threading.Lock()
        
        # Configure regular logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - SERVER%(server_id)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"server{server_id}.log"),
                logging.StreamHandler()
            ],
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Add server_id as a logging filter
        class ServerIDFilter(logging.Filter):
            def __init__(self, server_id):
                super().__init__()
                self.server_id = server_id
                
            def filter(self, record):
                record.server_id = self.server_id
                return True
        
        self.logger = logging.getLogger()
        self.logger.addFilter(ServerIDFilter(server_id))
    
    def log_operation(self, operation, user_id=None, resource_id=None, remote_addr=None, success=True, details=None):
        """Log an operation to both console and the operations log file."""
        timestamp = datetime.now().isoformat()
        
        # Create a structured log entry
        log_entry = {
            "timestamp": timestamp,
            "server_id": self.server_id,
            "operation": operation,
            "success": success
        }
        
        if user_id:
            log_entry["user_id"] = user_id
        
        if resource_id:
            log_entry["resource_id"] = resource_id
        
        if remote_addr:
            log_entry["remote_addr"] = f"{remote_addr[0]}:{remote_addr[1]}"
        
        if details:
            log_entry["details"] = details
        
        # Log to the operations log file
        with self.lock:
            with open(self.log_file_path, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        
        # Create a descriptive message for the console/standard logger
        message_parts = [f"{operation.upper()}"]
        
        if user_id:
            message_parts.append(f"User:{user_id}")
        
        if resource_id:
            message_parts.append(f"Resource:{resource_id}")
        
        if remote_addr:
            message_parts.append(f"From:{remote_addr[0]}:{remote_addr[1]}")
        
        message = " - ".join(message_parts)
        
        if success:
            self.logger.info(f"{message} - SUCCESS")
        else:
            self.logger.error(f"{message} - FAILED")
            if details and 'error' in details:
                self.logger.error(f"Error details: {details['error']}")

class MaekawaLockManager:
    """Implementation of Maekawa's algorithm for distributed mutual exclusion."""
    
    def __init__(self, server_id, total_servers, operation_logger):
        """Initialize the Maekawa lock manager."""
        self.server_id = server_id
        self.total_servers = total_servers
        self.operation_logger = operation_logger
        
        # Generate quorum sets for each server
        self.quorum_size = int(math.sqrt(total_servers))
        self.quorum_sets = self._generate_quorum_sets()
        
        # Resources for which this server manages locks
        self.resources = {}  # {resource_id: {'owner': None, 'queue': []}}
        
        # Locks for thread safety
        self.resources_lock = threading.Lock()
    
    def _generate_quorum_sets(self):
        """Generate quorum sets for each server using a grid-based approach."""
        quorum_sets = {}
        for i in range(self.total_servers):
            # Server's own set includes itself and up to quorum_size-1 other servers
            quorum = {i}
            for j in range(1, self.quorum_size):
                quorum.add((i + j) % self.total_servers)
            quorum_sets[i] = quorum
        return quorum_sets
    
    def request_lock(self, resource_id, requester_id=None):
        """Request a lock for a resource."""
        requester_id = requester_id if requester_id is not None else self.server_id
        
        self.operation_logger.log_operation(
            "request_lock",
            resource_id=resource_id,
            details={"requester_id": requester_id}
        )
        
        with self.resources_lock:
            # Initialize resource state if it doesn't exist
            if resource_id not in self.resources:
                self.resources[resource_id] = {
                    "owner": None,
                    "queue": []
                }
            
            resource = self.resources[resource_id]
            
            # If the resource is not locked, grant the lock
            if resource["owner"] is None:
                resource["owner"] = requester_id
                self.operation_logger.log_operation(
                    "grant_lock",
                    resource_id=resource_id,
                    details={"requester_id": requester_id}
                )
                return True
            
            # If the resource is already locked, add the requester to the queue
            if requester_id not in resource["queue"]:
                resource["queue"].append(requester_id)
            
            self.operation_logger.log_operation(
                "queue_lock_request",
                resource_id=resource_id,
                details={"requester_id": requester_id, "queue_position": len(resource["queue"])}
            )
            
            return False
    
    def release_lock(self, resource_id, requester_id=None):
        """Release a lock for a resource."""
        requester_id = requester_id if requester_id is not None else self.server_id
        
        self.operation_logger.log_operation(
            "release_lock",
            resource_id=resource_id,
            details={"requester_id": requester_id}
        )
        
        with self.resources_lock:
            # Check if the resource exists and is locked by the requester
            if resource_id not in self.resources:
                self.operation_logger.log_operation(
                    "release_lock_failed",
                    resource_id=resource_id,
                    success=False,
                    details={"requester_id": requester_id, "reason": "Resource not found"}
                )
                return False
            
            resource = self.resources[resource_id]
            
            if resource["owner"] != requester_id:
                self.operation_logger.log_operation(
                    "release_lock_failed",
                    resource_id=resource_id,
                    success=False,
                    details={"requester_id": requester_id, "reason": "Not the owner"}
                )
                return False
            
            # Release the lock
            resource["owner"] = None
            
            # Grant the lock to the next requester in the queue, if any
            if resource["queue"]:
                next_requester = resource["queue"].pop(0)
                resource["owner"] = next_requester
                
                self.operation_logger.log_operation(
                    "grant_lock",
                    resource_id=resource_id,
                    details={"requester_id": next_requester}
                )
            
            return True

class ReplicationManager:
    """Manages data replication between servers."""
    
    def __init__(self, server_id, server_info, operation_logger):
        """Initialize the replication manager."""
        self.server_id = server_id
        self.server_info = server_info
        self.operation_logger = operation_logger
    
    def _connect_to_server(self, server_id):
        """Connect to another server."""
        if server_id == self.server_id:
            raise ValueError("Cannot connect to self")
        
        host, port = self.server_info[server_id]
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        
        return s
    
    def synchronize_with_replicas(self, user_id, filename, data=None, is_delete=False):
        """Synchronize file operation with all replicas."""
        operation = "sync_delete" if is_delete else "sync"
        
        self.operation_logger.log_operation(
            f"{operation}_start",
            user_id=user_id,
            resource_id=filename,
            details={"file_size": len(data) if data else 0} if not is_delete else None
        )
        
        # Track replica synchronization
        success_count = 0
        
        for server_id in self.server_info:
            if server_id == self.server_id:
                continue  # Skip self
                
            try:
                with self._connect_to_server(server_id) as s:
                    request = {
                        "action": operation,
                        "user_id": user_id,
                        "filename": filename,
                        "source_server": self.server_id
                    }
                    
                    s.sendall(json.dumps(request).encode('utf-8'))
                    
                    if not is_delete:
                        # Wait for server to be ready for data
                        response = s.recv(1024)
                        if response != b'READY_FOR_DATA':
                            self.operation_logger.log_operation(
                                f"{operation}_error",
                                user_id=user_id,
                                resource_id=filename,
                                success=False,
                                details={"server_id": server_id, "error": "Server not ready"}
                            )
                            continue
                        
                        # Send file data
                        s.sendall(data)
                        s.shutdown(socket.SHUT_WR)
                    
                    # Get confirmation
                    response = s.recv(1024).decode('utf-8')
                    response_data = json.loads(response)
                    
                    if response_data.get('status') == 'success':
                        success_count += 1
                        self.operation_logger.log_operation(
                            f"{operation}_success",
                            user_id=user_id,
                            resource_id=filename,
                            details={"server_id": server_id}
                        )
                    else:
                        self.operation_logger.log_operation(
                            f"{operation}_error",
                            user_id=user_id,
                            resource_id=filename,
                            success=False,
                            details={"server_id": server_id, "error": response_data.get('message', 'Unknown error')}
                        )
                        
            except Exception as e:
                self.operation_logger.log_operation(
                    f"{operation}_error",
                    user_id=user_id,
                    resource_id=filename,
                    success=False,
                    details={"server_id": server_id, "error": str(e)}
                )
        
        is_successful = success_count == len(self.server_info) - 1  # Quorum is all replicas
        if is_successful:
            self.operation_logger.log_operation(
                f"{operation}_all_replicas_success",
                user_id=user_id,
                resource_id=filename
            )
        else:
            self.operation_logger.log_operation(
                f"{operation}_overall_failure",
                user_id=user_id,
                resource_id=filename
            )
        
        return is_successful

class Server:
    """Main server class."""
    
    def __init__(self, server_id, host, port, server_info):
        """Initialize the server."""
        self.server_id = server_id
        self.host = host
        self.port = port
        self.server_info = server_info
        
        # Set up storage directory
        self.storage_dir = STORAGE_DIR / str(server_id)
        os.makedirs(self.storage_dir, exist_ok=True)
        
        # Logging and locking
        self.operation_logger = FileOperationLogger(server_id)
        self.lock_manager = MaekawaLockManager(server_id, len(server_info), self.operation_logger)
        self.replication_manager = ReplicationManager(server_id, server_info, self.operation_logger)
        
        self.operation_logger.logger.info(f"Server initialized on {host}:{port}")
    
    def _send_redirect(self, conn, server_id):
        """Redirect the client to another server."""
        host, port = self.server_info[server_id]
        response = {
            "status": "redirect",
            "host": host,
            "port": port
        }
        conn.sendall(json.dumps(response).encode('utf-8'))
        self.operation_logger.log_operation(
            "redirect",
            details={"target_server": server_id, "host": host, "port": port},
            remote_addr=conn.getpeername() if hasattr(conn, "getpeername") else None  # Access getpeername only if it exists
        )
    
    def handle_client(self, conn, addr):
        """Handle a client connection."""
        remote_addr = addr if addr else ("Unknown", "Unknown")  # Ensure addr is valid
        self.operation_logger.log_operation("connection_received", remote_addr=remote_addr)
        
        try:
            data = conn.recv(1024).decode('utf-8')
            request = json.loads(data)
            
            action = request.get('action')
            user_id = request.get('user_id')
            filename = request.get('filename')
            
            if not user_id:
                self.operation_logger.log_operation(
                    "invalid_request",
                    success=False,
                    details={"error": "User ID missing", "request": request}
                )
                conn.sendall(json.dumps({"status": "error", "message": "User ID missing"}).encode('utf-8'))
                return
            
            user_dir = self.storage_dir / user_id
            os.makedirs(user_dir, exist_ok=True)
            
            # Handle lock requests
            if action == "request_lock":
                resource_id = request.get('resource_id')
                if self.lock_manager.request_lock(resource_id):
                    conn.sendall(json.dumps({"status": "success", "message": "Lock acquired"}).encode('utf-8'))
                else:
                    conn.sendall(json.dumps({"status": "error", "message": "Lock not acquired"}).encode('utf-8'))
            
            # Handle release lock requests
            elif action == "release_lock":
                resource_id = request.get('resource_id')
                if self.lock_manager.release_lock(resource_id):
                    conn.sendall(json.dumps({"status": "success", "message": "Lock released"}).encode('utf-8'))
                else:
                    conn.sendall(json.dumps({"status": "error", "message": "Lock not released"}).encode('utf-8'))
            
            # Handle list files requests
            elif action == "list":
                files = [f.name for f in user_dir.iterdir() if f.is_file()]
                response = {"status": "success", "files": files}
                conn.sendall(json.dumps(response).encode('utf-8'))
                self.operation_logger.log_operation(
                    "list_files",
                    user_id=user_id,
                    resource_id="directory",
                    success=True,
                    remote_addr=remote_addr,
                    details={"files": files}
                )
            
            # Handle upload requests
            elif action == "upload":
                if not filename:
                    self.operation_logger.log_operation(
                        "invalid_request",
                        user_id=user_id,
                        success=False,
                        details={"error": "Filename missing", "request": request}
                    )
                    conn.sendall(json.dumps({"status": "error", "message": "Filename missing"}).encode('utf-8'))
                    return
                
                filepath = user_dir / filename
                
                # Send READY_FOR_DATA to acknowledge and start receiving the file
                conn.sendall(b'READY_FOR_DATA')
                
                file_data = b''
                while True:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    file_data += chunk
                
                # Write the file to disk
                try:
                    with open(filepath, "wb") as f:
                        f.write(file_data)
                    
                    # Replicate to other servers
                    if self.replication_manager.synchronize_with_replicas(user_id, filename, file_data):
                        self.operation_logger.log_operation(
                            "upload",
                            user_id=user_id,
                            resource_id=filename,
                            success=True,
                            remote_addr=remote_addr,
                            details={"file_size": len(file_data)}
                        )
                        conn.sendall(json.dumps({"status": "success", "message": "File uploaded successfully"}).encode('utf-8'))
                    else:
                        self.operation_logger.log_operation(
                            "upload_replication_failed",
                            user_id=user_id,
                            resource_id=filename,
                            success=False,
                            details={"file_size": len(file_data)}
                        )
                        conn.sendall(json.dumps({"status": "error", "message": "File uploaded, but replication failed"}).encode('utf-8'))
                except Exception as e:
                    self.operation_logger.log_operation(
                        "upload_failed",
                        user_id=user_id,
                        resource_id=filename,
                        success=False,
                        remote_addr=remote_addr,
                        details={"error": str(e)}
                    )
                    conn.sendall(json.dumps({"status": "error", "message": str(e)}).encode('utf-8'))
            
            # Handle download requests
            elif action == "download":
                if not filename:
                    self.operation_logger.log_operation(
                        "invalid_request",
                        user_id=user_id,
                        success=False,
                        details={"error": "Filename missing", "request": request}
                    )
                    conn.sendall(json.dumps({"status": "error", "message": "Filename missing"}).encode('utf-8'))
                    return
                
                filepath = user_dir / filename
                
                # Check if the file exists
                if not filepath.exists() or not filepath.is_file():
                    self.operation_logger.log_operation(
                        "download_file_not_found",
                        user_id=user_id,
                        resource_id=filename,
                        success=False,
                        remote_addr=remote_addr
                    )
                    conn.sendall(json.dumps({"status": "error", "message": "File not found"}).encode('utf-8'))
                    return
                
                # Send success status to the client
                conn.sendall(json.dumps({"status": "success", "message": "Ready to receive file"}).encode('utf-8'))
                
                # Wait for the client to acknowledge
                ack = conn.recv(1024)
                if ack != b'READY_FOR_DATA':
                    self.operation_logger.log_operation(
                        "download_client_not_ready",
                        user_id=user_id,
                        resource_id=filename,
                        success=False,
                        remote_addr=remote_addr
                    )
                    conn.sendall(json.dumps({"status": "error", "message": "Client not ready"}).encode('utf-8'))
                    return
                
                # Send the file data
                try:
                    with open(filepath, "rb") as f:
                        file_data = f.read()
                    conn.sendall(file_data)
                    conn.shutdown(socket.SHUT_WR)
                    self.operation_logger.log_operation(
                        "download",
                        user_id=user_id,
                        resource_id=filename,
                        success=True,
                        remote_addr=remote_addr,
                        details={"file_size": len(file_data)}
                    )
                except Exception as e:
                    self.operation_logger.log_operation(
                        "download_failed",
                        user_id=user_id,
                        resource_id=filename,
                        success=False,
                        remote_addr=remote_addr,
                        details={"error": str(e)}
                    )
                    conn.sendall(json.dumps({"status": "error", "message": str(e)}).encode('utf-8'))
            
            # Handle delete requests
            elif action == "delete":
                if not filename:
                    self.operation_logger.log_operation(
                        "invalid_request",
                        user_id=user_id,
                        success=False,
                        details={"error": "Filename missing", "request": request}
                    )
                    conn.sendall(json.dumps({"status": "error", "message": "Filename missing"}).encode('utf-8'))
                    return
                
                filepath = user_dir / filename
                
                # Check if the file exists
                if not filepath.exists() or not filepath.is_file():
                    self.operation_logger.log_operation(
                        "delete_file_not_found",
                        user_id=user_id,
                        resource_id=filename,
                        success=False,
                        remote_addr=remote_addr
                    )
                    conn.sendall(json.dumps({"status": "error", "message": "File not found"}).encode('utf-8'))
                    return
                
                # Delete the file
                try:
                    os.remove(filepath)
                    
                    # Replicate the deletion to other servers
                    if self.replication_manager.synchronize_with_replicas(user_id, filename, is_delete=True):
                        self.operation_logger.log_operation(
                            "delete",
                            user_id=user_id,
                            resource_id=filename,
                            success=True,
                            remote_addr=remote_addr
                        )
                        conn.sendall(json.dumps({"status": "success", "message": "File deleted successfully"}).encode('utf-8'))
                    else:
                        self.operation_logger.log_operation(
                            "delete_replication_failed",
                            user_id=user_id,
                            resource_id=filename,
                            success=False
                        )
                        conn.sendall(json.dumps({"status": "error", "message": "File deleted locally, but replication failed"}).encode('utf-8'))
                except Exception as e:
                    self.operation_logger.log_operation(
                        "delete_failed",
                        user_id=user_id,
                        resource_id=filename,
                        success=False,
                        remote_addr=remote_addr,
                        details={"error": str(e)}
                    )
                    conn.sendall(json.dumps({"status": "error", "message": str(e)}).encode('utf-8'))
            
            # Handle synchronize requests (internal server communication)
            elif action == "sync":
                filename = request.get('filename')
                user_id = request.get('user_id')
                source_server = request.get('source_server')
                filepath = user_dir / filename
                
                if not filename or not user_id or source_server is None:
                    self.operation_logger.log_operation(
                        "invalid_sync_request",
                        details={"request": request},
                        success=False,
                        remote_addr=remote_addr
                    )
                    conn.sendall(json.dumps({"status": "error", "message": "Invalid sync request"}).encode('utf-8'))
                    return
                
                # Send READY_FOR_DATA to acknowledge and start receiving the file
                conn.sendall(b'READY_FOR_DATA')
                
                # Receive file data
                file_data = b''
                while True:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    file_data += chunk
                
                # Write the file to disk
                try:
                    with open(filepath, "wb") as f:
                        f.write(file_data)
                    self.operation_logger.log_operation(
                        "sync_received",
                        user_id=user_id,
                        resource_id=filename,
                        success=True,
                        remote_addr=remote_addr,
                        details={"source_server": source_server, "file_size": len(file_data)}
                    )
                    conn.sendall(json.dumps({"status": "success", "message": "File synchronized successfully"}).encode('utf-8'))
                except Exception as e:
                    self.operation_logger.log_operation(
                        "sync_failed",
                        user_id=user_id,
                        resource_id=filename,
                        success=False,
                        remote_addr=remote_addr,
                        details={"source_server": source_server, "error": str(e)}
                    )
                    conn.sendall(json.dumps({"status": "error", "message": str(e)}).encode('utf-8'))
            
            elif action == "sync_delete":
                filename = request.get('filename')
                user_id = request.get('user_id')
                source_server = request.get('source_server')
                filepath = user_dir / filename
                
                if not filename or not user_id or source_server is None:
                    self.operation_logger.log_operation(
                        "invalid_sync_delete_request",
                        details={"request": request},
                        success=False
                    )
                    conn.sendall(json.dumps({"status": "error", "message": "Invalid sync delete request"}).encode('utf-8'))
                    return
                
                # Check if the file exists
                if filepath.exists():
                    try:
                        os.remove(filepath)
                        self.operation_logger.log_operation(
                            "sync_delete_success",
                            user_id=user_id,
                            resource_id=filename,
                            success=True,
                            details={"source_server": source_server}
                        )
                        conn.sendall(json.dumps({"status": "success", "message": "File deleted successfully via sync"}).encode('utf-8'))
                    except Exception as e:
                        self.operation_logger.log_operation(
                            "sync_delete_failed",
                            user_id=user_id,
                            resource_id=filename,
                            success=False,
                            details={"source_server": source_server, "error": str(e)}
                        )
                        conn.sendall(json.dumps({"status": "error", "message": str(e)}).encode('utf-8'))
                else:
                    self.operation_logger.log_operation(
                        "sync_delete_file_not_found",
                        user_id=user_id,
                        resource_id=filename,
                        details={"source_server": source_server},
                        success=True
                    )
                    conn.sendall(json.dumps({"status": "success", "message": "File not found, assuming already deleted"}).encode('utf-8'))
            
            else:
                self.operation_logger.log_operation(
                    "unknown_action",
                    success=False,
                    details={"action": action, "request": request}
                )
                conn.sendall(json.dumps({"status": "error", "message": "Unknown action"}).encode('utf-8'))
        
        except json.JSONDecodeError:
            self.operation_logger.log_operation(
                "invalid_json",
                success=False,
                remote_addr=remote_addr,
                details={"error": "Invalid JSON received"}
            )
            conn.sendall(json.dumps({"status": "error", "message": "Invalid JSON"}).encode('utf-8'))
        
        except Exception as e:
            self.operation_logger.log_operation(
                "unhandled_exception",
                success=False,
                remote_addr=remote_addr,
                details={"error": str(e)}
            )
            conn.sendall(json.dumps({"status": "error", "message": str(e)}).encode('utf-8'))
        
        finally:
            conn.close()
            self.operation_logger.log_operation("connection_closed", remote_addr=remote_addr)
    
    def run(self):
        """Run the server."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen(5)
            
            self.operation_logger.logger.info(f"Server listening on {self.host}:{self.port}")
            
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()

if __name__ == "__main__":
    server = Server(SERVER_ID, HOST, PORT, SERVER_INFO)
    server.run()

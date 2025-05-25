import subprocess
import signal
import time
import os
import sys

class ServerManager:
    def __init__(self, command):
        self.command = command
        self.process = None

    def start(self):
        """Start the server process"""
        if self.process and self.process.poll() is None:
            print("Server is already running")
            return

        print("Starting server...")
        self.process = subprocess.Popen(self.command, shell=True)

    def stop(self):
        """Stop the server process gracefully"""
        if not self.process:
            print("No server process to stop")
            return

        if self.process.poll() is None:  # If process is still running
            print("Stopping server gracefully...")
            try:
                # Send SIGTERM first
                self.process.terminate()
                # Wait for 5 seconds for graceful shutdown
                time.sleep(5)
                
                # If process is still running, send SIGKILL
                if self.process.poll() is None:
                    print("Server did not stop gracefully, forcing shutdown...")
                    self.process.kill()
            except ProcessLookupError:
                pass  # Process already terminated

        self.process = None

    def restart(self):
        """Restart the server process"""
        self.stop()
        time.sleep(1)  # Small delay before restarting
        self.start()

    def status(self):
        """Check server status"""
        if not self.process:
            print("Server is not running")
            return False

        if self.process.poll() is None:
            print("Server is running")
            return True
        else:
            print("Server has stopped")
            return False

    def wait_for_startup(self, timeout=10):
        """Wait for server to start up"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                import requests
                response = requests.get("http://localhost:8000/health")
                if response.status_code == 200:
                    print("Server is ready")
                    return True
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(1)
        
        print("Server did not start within timeout period")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python server_manager.py [start|stop|restart|status]")
        sys.exit(1)

    command = "/home/david-wilding/.local/bin/uv run uvicorn flight_server.main:app --reload"
    manager = ServerManager(command)

    action = sys.argv[1].lower()
    if action == "start":
        manager.start()
    elif action == "stop":
        manager.stop()
    elif action == "restart":
        manager.restart()
    elif action == "status":
        manager.status()
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)

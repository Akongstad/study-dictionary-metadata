# Initialization script for the postgres docker instance that includes configuration of max_locks_per_transaction

import subprocess
import sys
import time

import yaml

# Constants
CONFIG_FILE = ".config.yaml"
DOCKER_IMAGE = "postgres:latest"

# Load the YAML configuration
try:
    with open(CONFIG_FILE, "r") as file:
        config = yaml.safe_load(file)["postgres_conf"]
        print(config)
except FileNotFoundError:
    print(f"Error: {CONFIG_FILE} not found.")
    sys.exit(1)
except yaml.YAMLError as e:
    print(f"Error parsing YAML: {e}")
    sys.exit(1)

# Extract secrets

db_user = config.get("user")
db_password = config.get("password")
db_host = config.get("host")
db_name = config.get("database")
port = config.get("port", 5432)

# Ensure required secrets are present
if not all([db_user, db_password, db_host, db_name, port]):
    print("Error: Missing required secrets in config.yaml.")
    sys.exit(1)

# Check if the Docker image exists locally
try:
    result = subprocess.run(
        ["docker", "images", "-q", DOCKER_IMAGE], capture_output=True, text=True, check=True
    )
    if not result.stdout.strip():  # No output means the image doesn't exist
        print(f"Image '{DOCKER_IMAGE}' not found locally. Pulling from Docker Hub...")
        subprocess.run(["docker", "pull", DOCKER_IMAGE], check=True)
        print(f"Successfully pulled '{DOCKER_IMAGE}'.")
    else:
        print(f"Image '{DOCKER_IMAGE}' is already available locally.")
except subprocess.CalledProcessError as e:
    print(f"Error checking or pulling Docker image: {e}")
    sys.exit(1)

# If a container runs already, kill it.
try:
    subprocess.run(["docker", "stop", "postgres_container"], check=True)
    subprocess.run(["docker", "rm", "postgres_container"], check=True)
    print("Stopped and removed existing Docker container.")
except subprocess.CalledProcessError:
    print("No existing Docker container found.")

# Run the Docker container with environment variables
try:
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            "postgres_container",  # Change container name if needed
            "-e",
            f"POSTGRES_USER={db_user}",
            "-e",
            f"POSTGRES_PASSWORD={db_password}",
            "-e",
            f"POSTGRES_DB={db_name}",
            "-p",
            f"{port}:5432",
            "--shm-size=512m",
            DOCKER_IMAGE,
        ],
        check=True,
    )
    print("Docker container started successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error starting Docker container: {e}")
    sys.exit(1)

# Wait for the db to accept connections
time.sleep(1)

# Set max_locks_per_transaction
try:
    subprocess.run(
        [
            "docker",
            "exec",
            "-it",
            "postgres_container",
            "psql",
            "-U",
            db_user,
            "-d",
            db_name,
            "-c",
            "ALTER SYSTEM SET max_locks_per_transaction = 8192;",  # Change value if needed
        ],
        check=True,
    )
    print("max_locks_per_transaction set successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error setting max_locks_per_transaction: {e}")
    sys.exit(1)

# Restart the container
try:
    subprocess.run(["docker", "restart", "postgres_container"], check=True)
    print("Docker container restarted successfully.")

except subprocess.CalledProcessError as e:
    print(f"Error restarting Docker container: {e}")
    sys.exit(1)

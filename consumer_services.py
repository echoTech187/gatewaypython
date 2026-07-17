import os
import glob
import subprocess
import time
import sys
import threading
from dotenv import load_dotenv

# Load environment variables from .env file so child processes inherit them
load_dotenv()

def run_consumer(script_path):
    script_name = os.path.basename(script_path)
    while True:
        print(f"[SUPERVISOR] Starting {script_name}...", flush=True)
        try:
            # Start the script and wait for it to finish (or crash)
            process = subprocess.Popen([sys.executable, script_name], stdout=sys.stdout, stderr=sys.stderr)
            process.communicate() # wait
            
            # If it reached here, the script exited or crashed
            print(f"[SUPERVISOR] Warning: {script_name} crashed or exited with code {process.returncode}. Restarting in 5 seconds...", flush=True)
        except Exception as e:
            print(f"[SUPERVISOR] Failed to start {script_name}: {e}. Retrying in 5 seconds...", flush=True)
            
        time.sleep(5)

if __name__ == "__main__":
    print("===========================================", flush=True)
    print("      GIDI GATEWAY PYTHON SUPERVISOR       ", flush=True)
    print("===========================================", flush=True)
    print("[SUPERVISOR] Scanning for consumer scripts...", flush=True)
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    all_scripts = glob.glob(os.path.join(current_dir, "consumer_*.py"))
    consumer_scripts = [s for s in all_scripts if os.path.basename(s) != "consumer_services.py"]
    
    if not consumer_scripts:
        print("[SUPERVISOR] No consumer_*.py scripts found in the current directory.", flush=True)
        sys.exit(1)
        
    print(f"[SUPERVISOR] Found {len(consumer_scripts)} consumer scripts. Launching them in background threads...", flush=True)
    
    threads = []
    for script in consumer_scripts:
        # Create a thread for each script to manage its lifecycle
        t = threading.Thread(target=run_consumer, args=(script,), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.5) # stagger startups slightly
        
    print("[SUPERVISOR] All consumers are running. Do NOT close this window.", flush=True)
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[SUPERVISOR] Shutting down. The child processes will be terminated by Windows.", flush=True)
        sys.exit(0)

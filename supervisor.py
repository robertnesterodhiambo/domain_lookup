import os
import subprocess
import threading
import time
import sys

ROOT_DIR = "."
CURRENT_SCRIPT = os.path.abspath(__file__)
CHECK_INTERVAL = 1  # seconds between dashboard updates

class ScriptSupervisor:
    def __init__(self, script_path):
        self.script_path = script_path
        self.start_time = None
        self.process = None
        self.restarts = 0
        self.running = False
        self.lock = threading.Lock()

    def run(self):
        while True:
            with self.lock:
                self.start_time = time.time()
                self.running = True
                self.process = subprocess.Popen(
                    [sys.executable, self.script_path],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            self.process.wait()
            with self.lock:
                self.running = False
                self.restarts += 1
            time.sleep(1)

    def get_uptime(self):
        with self.lock:
            if not self.running:
                return "Not running"
            elapsed = int(time.time() - self.start_time)
            return time.strftime('%H:%M:%S', time.gmtime(elapsed))

    def get_restart_count(self):
        with self.lock:
            return self.restarts

def find_python_files(directory):
    py_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                full_path = os.path.abspath(os.path.join(root, file))
                if full_path != CURRENT_SCRIPT:
                    py_files.append(full_path)
    return py_files

def monitor(supervisors):
    print("\033[2J")  # Clear screen once at the start
    while True:
        print("\033[H", end="")  # Move cursor to top-left without clearing
        print("📊 Script Supervisor Status\n")
        for s in supervisors:
            print(f"🟢 {os.path.basename(s.script_path):<20} | Uptime: {s.get_uptime():<10} | Restarts: {s.get_restart_count()}")
        time.sleep(CHECK_INTERVAL)

def main():
    scripts = find_python_files(ROOT_DIR)
    supervisors = [ScriptSupervisor(path) for path in scripts]

    for s in supervisors:
        t = threading.Thread(target=s.run, daemon=True)
        t.start()

    monitor(supervisors)

if __name__ == "__main__":
    main()

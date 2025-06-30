import subprocess
import os
import sys
from pathlib import Path
import time
def start_process(script_name, log_filename):
    """Start a Python script in a separate process and log its output."""
    log_file_path = Path(log_filename).resolve()
    log_file = open(log_file_path, "w")

    process = subprocess.Popen(
        [sys.executable, script_name],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True
    )
    print(f"Started {script_name} (PID: {process.pid}), logging to {log_file_path}")
    return process, log_file

def main():
    base_dir = Path(__file__).parent.resolve()

    processes = []
    try:
        # Start TickProducer
        producer_script = base_dir / "tick_producer.py"
        producer_log = base_dir / "tick_producer.log"
        p1, f1 = start_process(str(producer_script), str(producer_log))
        processes.append((p1, f1))
        
        # Start StrategyConsumer
        strategy_script = base_dir / "strategy_consumer.py"
        strategy_log = base_dir / "strategy_consumer.log"
        p2, f2 = start_process(str(strategy_script), str(strategy_log))
        processes.append((p2, f2))

        # Wait for both processes to complete (until interrupted)
        for proc, _ in processes:
            proc.wait()

    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Terminating processes...")
        for proc, _ in processes:
            proc.terminate()
    finally:
        for proc, f in processes:
            proc.kill()
            f.close()
        print("All processes terminated and log files closed.")

if __name__ == "__main__":
    main()

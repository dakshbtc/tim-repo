import os
import signal
import subprocess

def stop_process(process_name):
    """Stops all processes with the given script name."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", process_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        pids = result.stdout.strip().split("\n")

        if not pids or pids == ['']:
            print(f"No running processes found for {process_name}.")
            return

        for pid in pids:
            try:
                print(f"Terminating {process_name} (PID: {pid})")
                os.kill(int(pid), signal.SIGTERM)
            except Exception as e:
                print(f"Failed to terminate PID {pid}: {e}")
    except Exception as e:
        print(f"Error while searching for {process_name}: {e}")

def main():
    stop_process("tick_producer.py")
    stop_process("strategy_consumer.py")
    print("All matching processes terminated.")

if __name__ == "__main__":
    main()

import subprocess
import sys

def main():
    result = subprocess.run([sys.executable, "start.py"])
    if result.returncode != 0:
        sys.exit(result.returncode)
    
    result = subprocess.run([sys.executable, "main.py"])
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
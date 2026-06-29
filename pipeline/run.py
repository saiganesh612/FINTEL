import os
import sys
from datetime import datetime, timezone

def main():
    session = os.environ.get("SESSION", "UNKNOWN")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print("=" * 50)
    print(f"FINTEL Pipeline — M1 Skeleton")
    print(f"Session  : {session}")
    print(f"Triggered: {now}")
    print(f"Status   : OK — scheduler is working")
    print("=" * 50)

    if session == "UNKNOWN":
        print("WARNING: Session could not be determined from trigger time.")
        sys.exit(1)

    print(f"[{session}] Pipeline complete. No logic yet — M1 verification only.")

if __name__ == "__main__":
    main()

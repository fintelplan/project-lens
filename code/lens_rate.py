"""
lens_rate.py
S4 RLHF: Bro Alpha rates the latest macro report

Usage:
  python code/lens_rate.py 4
  python code/lens_rate.py 5 "excellent signal on dollar corridor"
  python code/lens_rate.py 2 "S3 context missing this cycle"

Rating scale:
  1 = Poor     (missed key signals, wrong direction)
  2 = Weak     (partially useful, significant gaps)
  3 = Adequate (correct but surface-level)
  4 = Good     (correct, useful, actionable)
  5 = Excellent (sharp, surfaced hidden signal, directly useful)

This is the RLHF subjective reward signal (LR-M-004).
Human judgment is co-ground truth alongside Brier score.
"""

import os, sys, json
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client

def main():
    if len(sys.argv) < 2:
        print("Usage: python code/lens_rate.py <rating 1-5> [optional note]")
        print("Example: python code/lens_rate.py 4 'good signal on financial flows'")
        sys.exit(1)

    try:
        rating = float(sys.argv[1])
        if not (1.0 <= rating <= 5.0):
            raise ValueError("Rating must be 1-5")
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    note = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else ""

    sb = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_KEY"]
    )

    # Get latest macro report
    r = sb.table("lens_macro_reports") \
        .select("id, run_id, created_at, threat_level, executive_summary") \
        .order("created_at", desc=True) \
        .limit(1).execute()

    if not r.data:
        print("ERROR: No macro reports found in lens_macro_reports.")
        sys.exit(1)

    report = r.data[0]
    report_id   = report["id"]
    run_id      = report.get("run_id", "?")
    created_at  = report.get("created_at", "?")[:16]
    threat      = report.get("threat_level", "?")
    summary     = (report.get("executive_summary", "") or "")[:120]

    print(f"\nRating report:")
    print(f"  ID:       {report_id}")
    print(f"  Run:      {run_id}")
    print(f"  Created:  {created_at}")
    print(f"  Threat:   {threat}")
    print(f"  Summary:  {summary}...")
    print(f"\nYour rating: {rating}/5.0")
    if note:
        print(f"Note: {note}")

    # Update the record
    update = {
        "bro_alpha_rating": rating,
        "rated_at": datetime.now(timezone.utc).isoformat(),
    }
    if note:
        update["rating_note"] = note

    sb.table("lens_macro_reports") \
        .update(update) \
        .eq("id", report_id) \
        .execute()

    print(f"\n✅ Rating {rating}/5.0 saved to lens_macro_reports.")
    print("This is the S4 RLHF subjective reward signal.")
    print("S4-C will use this alongside Brier scores for calibration.")

if __name__ == "__main__":
    main()

"""
lens_s2f_helpers.py — Shared utilities for S2-F aggregators
LENS-021 | Project Lens

Provides cached entity_id lookup for state_actor_lens values.
"""
import logging

log = logging.getLogger("s2f_helpers")
_STATE_OFFICE_CACHE = {}


def get_state_office_entity_id(client, lens: str):
    """Look up entity_id for a state_actor_lens value.

    Cached at module level (3 lookups max per Python process).
    Returns None on failure — caller decides how to handle.
    """
    if lens in _STATE_OFFICE_CACHE:
        return _STATE_OFFICE_CACHE[lens]
    try:
        r = (
            client.table("lens_entities")
                  .select("id")
                  .eq("entity_type", "state_office")
                  .eq("canonical_name", lens)
                  .limit(1)
                  .execute()
        )
        if r.data:
            eid = r.data[0]["id"]
            _STATE_OFFICE_CACHE[lens] = eid
            return eid
        log.warning(f"State office not found in registry: {lens}")
    except Exception as e:
        log.warning(f"State office lookup failed for {lens}: {str(e)[:200]}")
    return None


def lens_filter_from_argv(argv):
    """CC-89: the first non-flag argument is the lens name.

    CC-88 fixed this in the Clarity aggregator only. The same expression had
    been written out by hand three times, so the Watch and Verification
    copies went on reading "--dry" as a lens name and querying
    state_actor_lens=--dry. The copies were the bug. One implementation,
    three call sites.
    """
    return next((a for a in argv[1:] if not a.startswith("--")), None)

"""Tests fuer DF-MYZEL-EVENT-BUS Durable-Replay + Idempotency-Keys [CRUX-MK].

Welle-46-E Patch-4: SQLite-Backend statt in-memory + Idempotency-Decorator-Anwendung.

Adressiert Cross-LLM-3OF3-MODIFY-Verdict (in-memory-mocks).
"""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from engine import (
    AdapterOrchestrator,
    DurableEventStore,
    Event,
    EventBus,
    MyzelLayer,
    event_idempotency_key,
)


# ============================================================
# Test 1: Event-Persistence-To-SQLite (Durable-Replay)
# ============================================================

def test_event_persisted_to_sqlite(tmp_path: Path) -> None:
    """W46-E: DurableEventStore persistiert Events nach SQLite.

    Pre: Empty SQLite-DB, 3 Events publiziert via EventBus(store=store)
    Post: store.count() == 3, alle Events in DB-Table 'events'
    """
    db_path = tmp_path / "myzel-events.db"
    store = DurableEventStore(db_path=db_path)
    bus = EventBus(store=store)

    e1 = Event.make(MyzelLayer.L1_BUS, "event.published", {"k": 1})
    e2 = Event.make(MyzelLayer.L2_DISPATCH, "event.dispatch", {"k": 2})
    e3 = Event.make(MyzelLayer.L3_STATE, "state.update", {"k": 3})

    bus.publish(e1)
    bus.publish(e2)
    bus.publish(e3)

    # Persistence-Check
    assert store.count() == 3, "SQLite muss alle 3 Events enthalten"
    assert db_path.exists(), "SQLite-DB-File muss existieren"
    assert bus.published_count == 3
    assert bus.duplicate_count == 0


# ============================================================
# Test 2: Event-Replay-After-Restart (Durable-Replay)
# ============================================================

def test_event_replay_after_restart(tmp_path: Path) -> None:
    """W46-E: Events sind nach simulated-restart via Replay zugreifbar.

    Pre: 5 Events publiziert, EventBus + Store werden 'verworfen'
    Post: Neue Store-Instance kann via replay() alle 5 Events laden, replay_count=1
    """
    db_path = tmp_path / "myzel-events.db"

    # Phase 1: Original-Session
    store1 = DurableEventStore(db_path=db_path)
    bus1 = EventBus(store=store1)
    events_orig = [
        Event.make(MyzelLayer.L1_BUS, "event.published", {"i": i})
        for i in range(5)
    ]
    for e in events_orig:
        bus1.publish(e)

    # "Restart" -- Discard bus1 + store1
    del bus1
    del store1

    # Phase 2: New Session, gleiche DB
    store2 = DurableEventStore(db_path=db_path)
    replayed = store2.replay()

    assert len(replayed) == 5, f"Replay muss 5 Events liefern, bekam {len(replayed)}"
    # Reihenfolge muss erhalten bleiben (created_at ASC)
    replayed_ids = [e.event_id for e in replayed]
    orig_ids = [e.event_id for e in events_orig]
    assert replayed_ids == orig_ids, "Replay-Reihenfolge muss insertion-order entsprechen"

    # Replay-Count muss inkrementiert sein
    assert store2.get_replay_count(events_orig[0].event_id) == 1

    # 2. Replay -> count=2
    store2.replay()
    assert store2.get_replay_count(events_orig[0].event_id) == 2


# ============================================================
# Test 3: Idempotency-Duplicate-Event-ID-Rejected
# ============================================================

def test_idempotency_duplicate_event_id_rejected(tmp_path: Path) -> None:
    """W46-E: Idempotency-Decorator lehnt Duplicate-Event-ID innerhalb TTL ab.

    Pre: Event mit event_id="evt-123" publiziert
    Post: 2. Publish mit selber event_id -> duplicate_count=1, published_count=1
          (Handler nicht 2x invoked, 'duplicate' status zurueck)
    """
    db_path = tmp_path / "myzel-events.db"
    store = DurableEventStore(db_path=db_path)
    bus = EventBus(store=store, idempotency_ttl_seconds=86400)

    received: list[Event] = []
    bus.subscribe("event.published", lambda e: received.append(e))

    # Manueller Event mit fixer ID (statt Event.make -> randomer UUID)
    fixed_id = "evt-fixed-123"
    e_first = Event(
        event_id=fixed_id,
        layer=MyzelLayer.L1_BUS,
        topic="event.published",
        payload={"version": "first"},
        timestamp="2026-05-12T10:00:00+00:00",
    )
    e_dup = Event(
        event_id=fixed_id,  # SAME ID
        layer=MyzelLayer.L1_BUS,
        topic="event.published",
        payload={"version": "duplicate"},  # other payload, same idem-key
        timestamp="2026-05-12T10:05:00+00:00",
    )

    # First publish -> fresh
    handlers_invoked_1 = bus.publish(e_first)
    assert handlers_invoked_1 == 1, "Handler muss bei first publish invoked werden"
    assert bus.published_count == 1
    assert bus.duplicate_count == 0
    assert len(received) == 1

    # Duplicate publish -> rejected
    handlers_invoked_2 = bus.publish(e_dup)
    assert handlers_invoked_2 == 0, "Handler darf bei duplicate NICHT invoked werden"
    assert bus.published_count == 1, "published_count bleibt 1 (kein Increment)"
    assert bus.duplicate_count == 1, "duplicate_count muss 1 sein"
    assert len(received) == 1, "received-list bleibt bei 1 (no handler-invoke)"

    # Store enthaelt nur 1 Event (das first)
    assert store.count() == 1


# ============================================================
# Test 4: Idempotency-TTL-Expiry
# ============================================================

def test_idempotency_ttl_expiry(tmp_path: Path) -> None:
    """W46-E: Nach TTL-Ablauf wird gleiche event_id wieder akzeptiert.

    Pre: Event publiziert mit ttl=1s, 1.5s gewartet
    Post: 2. Publish mit selber event_id -> 'fresh' (TTL abgelaufen), kein duplicate-Reject
    """
    db_path = tmp_path / "myzel-events.db"
    store = DurableEventStore(db_path=db_path)
    bus = EventBus(store=store, idempotency_ttl_seconds=1)  # 1s TTL

    fixed_id = "evt-ttl-test-456"
    e1 = Event(
        event_id=fixed_id,
        layer=MyzelLayer.L1_BUS,
        topic="event.published",
        payload={"phase": 1},
        timestamp="2026-05-12T10:00:00+00:00",
    )
    bus.publish(e1)
    assert bus.published_count == 1
    assert bus.duplicate_count == 0
    assert store.count() == 1

    # Sofortiger Re-Publish -> duplicate
    bus.publish(e1)
    assert bus.duplicate_count == 1
    assert store.count() == 1

    # TTL warten + Re-Publish -> fresh (idempotency-key abgelaufen, alter Eintrag wird ueberschrieben)
    time.sleep(1.2)
    e2 = Event(
        event_id=fixed_id,
        layer=MyzelLayer.L1_BUS,
        topic="event.published",
        payload={"phase": 2},
        timestamp="2026-05-12T10:00:01+00:00",
    )
    bus.publish(e2)

    assert bus.published_count == 2, "Nach TTL-Expiry muss 2. publish wieder zaehlen"
    assert bus.duplicate_count == 1, "Duplicate-Count bleibt 1 (nur 1 echter Duplicate)"
    # Store enthaelt 1 Event (alter wurde ueberschrieben durch DELETE+INSERT in persist())
    assert store.count() == 1


# ============================================================
# Sanity-Check: Idempotency-Key-Determinismus
# ============================================================

def test_event_idempotency_key_deterministic() -> None:
    """Idempotency-Key ist deterministische SHA256(event_id)."""
    k1 = event_idempotency_key("evt-123")
    k2 = event_idempotency_key("evt-123")
    k3 = event_idempotency_key("evt-456")
    assert k1 == k2, "Gleiche event_id -> gleicher Key"
    assert k1 != k3, "Unterschiedliche event_id -> unterschiedlicher Key"
    assert len(k1) == 64, "SHA256-Hex = 64 chars"


# ============================================================
# Sanity-Check: Backward-Compatibility (kein Store = in-memory only)
# ============================================================

def test_eventbus_without_store_still_works() -> None:
    """Backward-compat: EventBus ohne store agiert in-memory wie zuvor."""
    bus = EventBus(store=None)  # default
    e = Event.make(MyzelLayer.L1_BUS, "topic", {})
    bus.publish(e)
    bus.publish(e)  # selbe ID, aber ohne store -> kein idempotency-check
    assert bus.published_count == 2  # beide gezaehlt
    assert bus.duplicate_count == 0


# ============================================================
# Sanity-Check: Replay topic-filter
# ============================================================

def test_replay_with_topic_filter(tmp_path: Path) -> None:
    """Replay mit topic-filter liefert nur matching Events."""
    db_path = tmp_path / "myzel-events.db"
    store = DurableEventStore(db_path=db_path)
    bus = EventBus(store=store)

    bus.publish(Event.make(MyzelLayer.L1_BUS, "topic.a", {}))
    bus.publish(Event.make(MyzelLayer.L1_BUS, "topic.b", {}))
    bus.publish(Event.make(MyzelLayer.L1_BUS, "topic.a", {}))

    a_events = store.replay(topic="topic.a")
    b_events = store.replay(topic="topic.b")
    assert len(a_events) == 2
    assert len(b_events) == 1

"""Tests fuer DF-MYZEL-EVENT-BUS Engine [CRUX-MK]."""
from pathlib import Path

import pytest

from engine import (
    AdapterOrchestrator,
    AuditLogger,
    Event,
    EventBus,
    EventStateTracker,
    K16Mutex,
    MetaPromptingRouter,
    MyzelAuditResult,
    MyzelLayer,
    RoutingDecision,
    run_myzel_audit,
)


# ============================================================
# Event
# ============================================================

def test_event_make_creates_unique_ids() -> None:
    """Event.make erzeugt eindeutige IDs."""
    e1 = Event.make(MyzelLayer.L1_BUS, "x", {})
    e2 = Event.make(MyzelLayer.L1_BUS, "x", {})
    assert e1.event_id != e2.event_id


def test_event_immutable() -> None:
    """Event ist frozen (dataclass)."""
    e = Event.make(MyzelLayer.L1_BUS, "x", {})
    with pytest.raises(Exception):
        e.topic = "changed"  # type: ignore[misc]


# ============================================================
# EventBus
# ============================================================

def test_eventbus_publish_increments_counter() -> None:
    """publish() erhoeht published_count."""
    bus = EventBus()
    e = Event.make(MyzelLayer.L1_BUS, "topic", {})
    bus.publish(e)
    bus.publish(e)
    assert bus.published_count == 2


def test_eventbus_subscribe_invokes_handler() -> None:
    """Subscriber bekommt Event."""
    bus = EventBus()
    received: list[Event] = []
    bus.subscribe("topic-A", lambda e: received.append(e))
    e = Event.make(MyzelLayer.L1_BUS, "topic-A", {})
    bus.publish(e)
    assert len(received) == 1
    assert received[0].event_id == e.event_id


def test_eventbus_drain() -> None:
    """drain() leert die Queue."""
    bus = EventBus()
    bus.publish(Event.make(MyzelLayer.L1_BUS, "x", {}))
    bus.publish(Event.make(MyzelLayer.L1_BUS, "y", {}))
    drained = bus.drain()
    assert len(drained) == 2
    assert bus.queue_size == 0


def test_eventbus_handler_exception_isolated() -> None:
    """Failing handler bricht publish nicht (LC4)."""
    bus = EventBus()
    def bad_handler(e: Event) -> None:
        raise RuntimeError("boom")
    bus.subscribe("topic", bad_handler)
    e = Event.make(MyzelLayer.L1_BUS, "topic", {})
    # publish darf nicht raisen
    bus.publish(e)
    assert bus.published_count == 1


# ============================================================
# MetaPromptingRouter
# ============================================================

def test_router_known_topic() -> None:
    """Bekanntes Topic -> high score."""
    router = MetaPromptingRouter()
    e = Event.make(MyzelLayer.L1_BUS, "event.published", {})
    decision = router.route(e)
    assert decision.routing_score == 1.0
    assert decision.target_layer == MyzelLayer.L1_BUS


def test_router_unknown_topic_fallback() -> None:
    """Unbekanntes Topic -> Fallback-Score 0.5."""
    router = MetaPromptingRouter()
    e = Event.make(MyzelLayer.L1_BUS, "unknown.topic", {})
    decision = router.route(e)
    assert decision.routing_score == 0.5


# ============================================================
# EventStateTracker
# ============================================================

def test_state_tracker_appends() -> None:
    """track() appendet Events."""
    tracker = EventStateTracker()
    tracker.track(Event.make(MyzelLayer.L1_BUS, "x", {}))
    tracker.track(Event.make(MyzelLayer.L2_DISPATCH, "y", {}))
    snap = tracker.snapshot()
    assert snap.total_events == 2
    assert snap.events_per_layer["L1_BUS"] == 1
    assert snap.events_per_layer["L2_DISPATCH"] == 1
    assert tracker.log_size == 2


# ============================================================
# AdapterOrchestrator
# ============================================================

def test_orchestrator_process_full() -> None:
    """Full process: publish + route + track."""
    orch = AdapterOrchestrator()
    e = Event.make(MyzelLayer.L1_BUS, "event.published", {})
    decision = orch.process(e)
    assert decision.routing_score == 1.0
    audit = orch.audit()
    assert audit.events_published == 1
    assert audit.events_routed == 1


def test_orchestrator_audit_empty() -> None:
    """Empty Audit."""
    orch = AdapterOrchestrator()
    audit = orch.audit()
    assert audit.events_published == 0
    assert audit.average_routing_score == 0.0


def test_orchestrator_veto_count() -> None:
    """Unbekannte Topics (score=0.5) zaehlen nicht als Veto (< 0.4 noetig)."""
    orch = AdapterOrchestrator()
    orch.process(Event.make(MyzelLayer.L1_BUS, "unknown.topic", {}))
    audit = orch.audit()
    # 0.5 >= 0.4 -> kein Veto
    assert audit.veto_count == 0


# ============================================================
# K16-Mutex
# ============================================================

def test_k16_mutex(tmp_path: Path) -> None:
    """K16-Mutex blockt zweite Instanz."""
    lock = tmp_path / ".lock"
    m1 = K16Mutex(lock)
    assert m1.acquire() is True
    m2 = K16Mutex(lock)
    assert m2.acquire() is False
    m1.release()


# ============================================================
# AuditLogger
# ============================================================

def test_audit_logger_appends(tmp_path: Path) -> None:
    logger = AuditLogger(tmp_path / "audit.jsonl")
    logger.log({"r": 1})
    logger.log({"r": 2})
    lines = (tmp_path / "audit.jsonl").read_text().splitlines()
    assert len(lines) == 2


# ============================================================
# Integration: run_myzel_audit
# ============================================================

def test_run_myzel_audit_full(tmp_path: Path) -> None:
    """Full Audit-Run mit 3 Events."""
    config = {
        "paths": {"audit_log": "audit.jsonl"},
        "k16_concurrent_spawn_mutex": {"lock_dir": str(tmp_path / ".lock")},
    }
    events = [
        Event.make(MyzelLayer.L1_BUS, "event.published", {}),
        Event.make(MyzelLayer.L2_DISPATCH, "event.dispatch", {}),
        Event.make(MyzelLayer.L3_STATE, "state.update", {}),
    ]
    result = run_myzel_audit(tmp_path, config, events_input=events)
    assert result.events_published == 3
    assert result.events_routed == 3
    assert result.layers_active == 3
    assert (tmp_path / "audit.jsonl").exists()


def test_run_myzel_audit_stop_flag(tmp_path: Path) -> None:
    """STOP.flag blockt."""
    config = {
        "paths": {"audit_log": "audit.jsonl"},
        "k16_concurrent_spawn_mutex": {"lock_dir": str(tmp_path / ".lock")},
    }
    stop = tmp_path / "STOP.flag"
    stop.write_text("stop")
    result = run_myzel_audit(tmp_path, config, stop_flag=stop)
    assert result.skipped_due_to_stop_flag is True


# ============================================================
# W49-D K12+K13 Migration Tests
# ============================================================

def test_w49d_k12_envelope_and_k13_anchor(tmp_path: Path) -> None:
    """K12 envelope + K13 RFC3161-anchor are produced by run_myzel_audit."""
    config = {
        "paths": {"audit_log": "audit.jsonl"},
        "k16_concurrent_spawn_mutex": {"lock_dir": str(tmp_path / ".lock-w49d")},
    }
    events = [Event.make(MyzelLayer.L1_BUS, "event.published", {})]
    result = run_myzel_audit(tmp_path, config, events_input=events)
    assert not result.skipped_due_to_stop_flag
    from engine import W49D_FOUNDATION
    if W49D_FOUNDATION:
        prov_dir = tmp_path / "provenance-full"
        assert prov_dir.exists()
        envs = list(prov_dir.glob("*.envelope.json"))
        assert len(envs) >= 1
        anchors = tmp_path / "anchors" / "rfc3161-anchors.jsonl"
        assert anchors.exists()
        assert anchors.read_text().strip()

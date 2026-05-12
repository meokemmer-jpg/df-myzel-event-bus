# DF-MYZEL-EVENT-BUS Engine [CRUX-MK]
"""
Event-Bus fuer 7 Myzel-Schichten (MYZ-01..28).

Architektur:
- EventBus: publish/subscribe, In-Memory-Queue (append-only)
- MetaPromptingRouter: Mock-Router fuer 7 Layer-Targets
- EventStateTracker: Append-only-Event-Log mit State-Tracking
- AdapterOrchestrator: Orchestrator zwischen Bus + Router
- AuditLogger: JSONL append-only

Per SAE-v8 MYZ-30 + MYZ-32 + MYZ-36 Pattern.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable


# 7 Myzel-Schichten (per SAE-v8 Myzel-Doktrin)
class MyzelLayer(str, Enum):
    L1_BUS = "L1_BUS"             # MYZ-30 Event-Router
    L2_DISPATCH = "L2_DISPATCH"   # MYZ-32 Dispatcher
    L3_STATE = "L3_STATE"         # State-Tracker
    L4_ROUTING = "L4_ROUTING"     # MYZ-36 Meta-Prompting
    L5_GOVERNANCE = "L5_GOVERNANCE"  # Governance-Layer
    L6_OBSERVE = "L6_OBSERVE"     # Observation/Audit
    L7_META = "L7_META"           # Meta-Layer


class Severity(str, Enum):
    OK = "OK"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    VETO = "VETO"


# ============================================================
# K16-Mutex (Pattern-Reuse)
# ============================================================

class K16Mutex:
    def __init__(self, lock_dir: Path) -> None:
        self.lock_dir = Path(lock_dir)

    def acquire(self) -> bool:
        try:
            self.lock_dir.mkdir(parents=False, exist_ok=False)
            (self.lock_dir / "pid").write_text(str(os.getpid()))
            return True
        except FileExistsError:
            return False

    def release(self) -> None:
        try:
            for child in self.lock_dir.iterdir():
                child.unlink()
            self.lock_dir.rmdir()
        except FileNotFoundError:
            pass


class AuditLogger:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, record: dict[str, Any]) -> None:
        record_with_ts = {"ts": datetime.now(timezone.utc).isoformat(), **record}
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record_with_ts, ensure_ascii=False) + "\n")


# ============================================================
# Event
# ============================================================

@dataclass(frozen=True)
class Event:
    """Immutable Event mit MYZ-30 konformer Struktur."""

    event_id: str
    layer: MyzelLayer
    topic: str
    payload: dict[str, Any]
    timestamp: str
    correlation_id: str | None = None

    @staticmethod
    def make(layer: MyzelLayer, topic: str, payload: dict[str, Any],
             correlation_id: str | None = None) -> "Event":
        return Event(
            event_id=str(uuid.uuid4()),
            layer=layer,
            topic=topic,
            payload=payload,
            timestamp=datetime.now(timezone.utc).isoformat(),
            correlation_id=correlation_id,
        )


# ============================================================
# EventBus
# ============================================================

class EventBus:
    """In-Memory publish/subscribe (MYZ-30 Pattern)."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable[[Event], None]]] = defaultdict(list)
        self._queue: deque[Event] = deque()
        self._published_count = 0

    def subscribe(self, topic: str, handler: Callable[[Event], None]) -> None:
        self._subscribers[topic].append(handler)

    def publish(self, event: Event) -> int:
        """Publish event. Returns count of handlers invoked.

        Pre: event ist Event-Instance
        Post: queue groesser um 1, handlers fuer topic invoked
        """
        self._queue.append(event)
        self._published_count += 1
        handlers = self._subscribers.get(event.topic, [])
        for h in handlers:
            try:
                h(event)
            except Exception:
                pass  # Failure-Isolation (LC4)
        return len(handlers)

    def drain(self) -> list[Event]:
        """Drain queue (returns + clears)."""
        events = list(self._queue)
        self._queue.clear()
        return events

    @property
    def published_count(self) -> int:
        return self._published_count

    @property
    def queue_size(self) -> int:
        return len(self._queue)


# ============================================================
# MetaPromptingRouter (Mock)
# ============================================================

@dataclass(frozen=True)
class RoutingDecision:
    event_id: str
    target_layer: MyzelLayer
    routing_score: float


class MetaPromptingRouter:
    """Mock-Router (MYZ-36 Pattern). Routes events to layers based on topic."""

    def __init__(self) -> None:
        # Mock-Routing-Tabelle: topic -> target Layer
        self.topic_routes: dict[str, MyzelLayer] = {
            "event.published": MyzelLayer.L1_BUS,
            "event.dispatch": MyzelLayer.L2_DISPATCH,
            "state.update": MyzelLayer.L3_STATE,
            "prompt.route": MyzelLayer.L4_ROUTING,
            "governance.check": MyzelLayer.L5_GOVERNANCE,
            "observe.audit": MyzelLayer.L6_OBSERVE,
            "meta.audit": MyzelLayer.L7_META,
        }

    def route(self, event: Event) -> RoutingDecision:
        target = self.topic_routes.get(event.topic, MyzelLayer.L1_BUS)
        # Mock-Score: 1.0 wenn match, 0.5 wenn fallback
        score = 1.0 if event.topic in self.topic_routes else 0.5
        return RoutingDecision(event_id=event.event_id, target_layer=target, routing_score=score)


# ============================================================
# EventStateTracker
# ============================================================

@dataclass
class StateSnapshot:
    total_events: int
    events_per_layer: dict[str, int]
    last_event_ts: str | None


class EventStateTracker:
    """Append-only State-Tracker (MYZ-32 Pattern)."""

    def __init__(self) -> None:
        self._events_per_layer: dict[str, int] = defaultdict(int)
        self._total: int = 0
        self._last_ts: str | None = None
        self._event_log: list[Event] = []  # append-only

    def track(self, event: Event) -> None:
        self._events_per_layer[event.layer.value] += 1
        self._total += 1
        self._last_ts = event.timestamp
        self._event_log.append(event)

    def snapshot(self) -> StateSnapshot:
        return StateSnapshot(
            total_events=self._total,
            events_per_layer=dict(self._events_per_layer),
            last_event_ts=self._last_ts,
        )

    @property
    def log_size(self) -> int:
        return len(self._event_log)


# ============================================================
# AdapterOrchestrator
# ============================================================

@dataclass(frozen=True)
class MyzelAuditResult:
    events_published: int
    events_routed: int
    layers_active: int
    average_routing_score: float
    veto_count: int
    skipped_due_to_stop_flag: bool = False


class AdapterOrchestrator:
    """Orchestriert Bus + Router + Tracker."""

    def __init__(self) -> None:
        self.bus = EventBus()
        self.router = MetaPromptingRouter()
        self.tracker = EventStateTracker()
        self.routing_scores: list[float] = []

    def process(self, event: Event) -> RoutingDecision:
        self.bus.publish(event)
        decision = self.router.route(event)
        self.tracker.track(event)
        self.routing_scores.append(decision.routing_score)
        return decision

    def audit(self) -> MyzelAuditResult:
        snapshot = self.tracker.snapshot()
        avg_score = (
            sum(self.routing_scores) / len(self.routing_scores)
            if self.routing_scores else 0.0
        )
        vetos = sum(1 for s in self.routing_scores if s < 0.4)
        return MyzelAuditResult(
            events_published=self.bus.published_count,
            events_routed=len(self.routing_scores),
            layers_active=len(snapshot.events_per_layer),
            average_routing_score=avg_score,
            veto_count=vetos,
        )


# ============================================================
# run_myzel_audit
# ============================================================

def run_myzel_audit(
    repo_root: Path,
    config: dict[str, Any],
    stop_flag: Path | None = None,
    events_input: list[Event] | None = None,
) -> MyzelAuditResult:
    if stop_flag is not None and stop_flag.exists():
        return MyzelAuditResult(0, 0, 0, 0.0, 0, skipped_due_to_stop_flag=True)

    lock_dir = Path(
        config.get("k16_concurrent_spawn_mutex", {}).get("lock_dir", "/tmp/df-myzel-event-bus.lock")
    )
    mutex = K16Mutex(lock_dir)
    if not mutex.acquire():
        return MyzelAuditResult(0, 0, 0, 0.0, 0)

    try:
        audit_log_path = Path(repo_root) / config.get("paths", {}).get("audit_log", "audit.jsonl")
        logger = AuditLogger(audit_log_path)
        orchestrator = AdapterOrchestrator()
        for event in events_input or []:
            orchestrator.process(event)
        result = orchestrator.audit()
        logger.log({"event": "myzel-audit-complete", "result": asdict(result)})
        return result
    finally:
        mutex.release()

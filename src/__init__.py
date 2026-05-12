# DF-MYZEL-EVENT-BUS [CRUX-MK]
"""Event-Bus fuer 7 Myzel-Schichten (MYZ-01..28)."""

# LAZY-IMPORT-PATTERN (Dual-Import-Bug-Vermeidung per coding.md §1)
__all__ = [
    "EventBus",
    "Event",
    "MetaPromptingRouter",
    "EventStateTracker",
    "AdapterOrchestrator",
    "AuditLogger",
    "K16Mutex",
    "MyzelLayer",
    "run_myzel_audit",
]

def __getattr__(name: str):
    if name in __all__:
        from . import engine
        return getattr(engine, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

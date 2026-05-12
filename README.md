# DF-MYZEL-EVENT-BUS [CRUX-MK]

Event-Bus fuer 7 Myzel-Schichten (MYZ-01..28).

## Architektur

- `src/engine.py` — EventBus + MetaPromptingRouter + EventStateTracker + AdapterOrchestrator + AuditLogger
- `tests/test_engine.py` — 14 Tests (Event + Bus + Router + Tracker + Orchestrator + Integration)
- `scripts/run-df-myzel-event-bus.sh` — K16-Mutex Wrapper

## 7 Myzel-Schichten

- L1_BUS (MYZ-30 Event-Router)
- L2_DISPATCH (MYZ-32 Dispatcher)
- L3_STATE (Append-only State-Tracker)
- L4_ROUTING (MYZ-36 Meta-Prompting-Router)
- L5_GOVERNANCE
- L6_OBSERVE
- L7_META

## SAE-v8 Integration

LAZY-IMPORT-PATTERN: Kein `from sae_v8.xxx`.

## Test

```bash
python3 -m pytest tests/ -q
```

[CRUX-MK]

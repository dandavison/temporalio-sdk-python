# API vs Python SDK Discrepancies

Comparison of gRPC API (commit `1a1e74e` in `temporalio/api`) with the Python SDK implementation.

---

## 1. API Features NOT Exposed in Python SDK

### `DescribeActivityExecution` options not exposed

**gRPC API:**
```protobuf
message DescribeActivityExecutionRequest {
    string namespace = 1;
    string activity_id = 2;
    string run_id = 3;
    bool include_input = 4;        // ← NOT EXPOSED
    bool include_outcome = 5;      // ← NOT EXPOSED
    bytes long_poll_token = 6;     // ← NOT EXPOSED
}
```

**Python SDK `DescribeActivityInput`:**
```python
@dataclass
class DescribeActivityInput:
    activity_id: str
    activity_run_id: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    # MISSING: include_input, include_outcome, long_poll_token
```

**Implementation hardcodes:**
```python
temporalio.api.workflowservice.v1.DescribeActivityExecutionRequest(
    namespace=self._client.namespace,
    activity_id=input.activity_id,
    run_id=input.activity_run_id or "",
    include_input=True,  # Always true
    # include_outcome not set
    # long_poll_token not supported
)
```

**Impact:**
- Users cannot opt-out of fetching input (bandwidth optimization)
- Users cannot fetch outcome in describe (must use separate result() call)
- No long-poll support for efficient UI refresh

---

## 2. Entire APIs NOT Implemented in Python SDK

These are expected per design docs - pause/reset/update-options are "MLP GA" scope, not pre-release.

---

## 3. Summary Table

| Feature | In API | In Python SDK | Notes |
|---------|--------|---------------|-------|
| **StartActivityExecution** | ✅ | ✅ | Fully implemented |
| **DescribeActivityExecution** | ✅ | ⚠️ Partial | Missing `include_input`, `include_outcome`, `long_poll_token` options |
| **PollActivityExecution** | ✅ | ✅ | Used internally for `result()` |
| **ListActivityExecutions** | ✅ | ✅ | Fully implemented |
| **CountActivityExecutions** | ✅ | ✅ | Fully implemented |
| **RequestCancelActivityExecution** | ✅ | ✅ | Fully implemented |
| **TerminateActivityExecution** | ✅ | ✅ | Fully implemented |
| **UpdateActivityOptions** | ✅ | ❌ | Planned for MLP GA |
| **PauseActivity** | ✅ | ❌ | Planned for MLP GA |
| **UnpauseActivity** | ✅ | ❌ | Planned for MLP GA |
| **ResetActivity** | ✅ | ❌ | Planned for MLP GA |
| **DeleteActivityExecution** | ✅ | ❌ | Not implemented |

---

## 4. Recommendations

### Future (MLP GA)

1. **Add describe options**: `include_input`, `include_outcome` parameters
2. **Add long-poll support** for describe (useful for UI)
3. **Implement pause/unpause/reset/update-options** when ready
4. **Implement delete** for cleanup operations

# Standalone Activity Implementation TODO

Comparison of spec (cross-sdk-design.md, Python section) against current PR implementation.

---

## 1. Missing Client Methods

**Spec Section:** 2.A
**Location:** `temporalio/client.py` in `Client` class
**Reference:** The spec says: "Same set of methods as for workflow activities: `start_activity`, `start_activity_class`, `start_activity_method`, `execute_activity`, `execute_activity_class`, `execute_activity_method`"

### 1.1 Add `start_activity_class` method
- [ ] Add method with same signature pattern as `start_activity` but accepting activity class
- [ ] Add appropriate overloads for type inference

### 1.2 Add `start_activity_method` method
- [ ] Add method with same signature pattern as `start_activity` but accepting activity method
- [ ] Add appropriate overloads for type inference

### 1.3 Add `execute_activity_class` method
- [ ] Add method with same signature pattern as `execute_activity` but accepting activity class
- [ ] Add appropriate overloads for type inference

### 1.4 Add `execute_activity_method` method
- [ ] Add method with same signature pattern as `execute_activity` but accepting activity method
- [ ] Add appropriate overloads for type inference

---

## 2. Missing Interceptor Method & Input Class

**Spec Section:** 3
**Location:** `temporalio/client.py`

### 2.1 Add `get_activity_result` to `OutboundInterceptor`
- [ ] Add method to `OutboundInterceptor` class (around line 7165+)

```python
async def get_activity_result(
    self, input: GetActivityResultInput[ReturnType]
) -> ReturnType:
    """Called for every :py:meth:`ActivityHandle.result` call.

    .. warning::
       This API is experimental.
    """
    return await self.next.get_activity_result(input)
```

### 2.2 Add `GetActivityResultInput` dataclass
- [ ] Add dataclass near other activity input classes (around line 6800+)

```python
@dataclass
class GetActivityResultInput(Generic[ReturnType]):
    """Input for :py:meth:`OutboundInterceptor.get_activity_result`.

    .. warning::
       This API is experimental.
    """

    activity_id: str
    activity_run_id: str | None
    result_type: Type[ReturnType]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
```

- [ ] Update `ActivityHandle.result()` to use the interceptor
- [ ] Add implementation in `_ClientImpl`

---

## 3. `ActivityExecutionDescription` Inheritance

**Spec Section:** 1
**Location:** `temporalio/client.py`, class `ActivityExecutionDescription` (line ~3558)

**Spec says:**
```python
@dataclass(frozen=True)
class ActivityExecutionDescription(ActivityExecution):
    ...
```

**Current:** `ActivityExecutionDescription` is standalone with duplicated fields.

### Tasks:
- [ ] Make `ActivityExecutionDescription` inherit from `ActivityExecution`
- [ ] Remove duplicated fields that exist in parent class:
  - `activity_id`, `activity_run_id`, `activity_type`, `close_time`, `execution_duration`, `namespace`, `scheduled_time`, `search_attributes`, `status`, `task_queue`
- [ ] Keep only the additional fields unique to Description:
  - `attempt`, `canceled_reason`, `current_retry_interval`, `eager_execution_requested`, `expiration_time`, `heartbeat_details`, `input`, `last_attempt_complete_time`, `last_failure`, `last_heartbeat_time`, `last_started_time`, `last_worker_identity`, `next_attempt_schedule_time`, `paused`, `raw_info`, `retry_policy`, `run_state`
- [ ] Update `raw_info` type in `ActivityExecution` to be `Union[ActivityExecutionListInfo, ActivityExecutionInfo]` per spec
- [ ] Update `_from_raw_info` factory methods to handle inheritance properly
- [ ] Note: `state_transition_count` field exists in `ActivityExecution` but not in `ActivityExecutionDescription` per spec

---

## 4. Evaluate `input` Field in `ActivityExecutionDescription`

**Spec Section:** 1
**Location:** `temporalio/client.py`, line ~3598

**Current:** Has `input: Sequence[Any]` field
**Spec:** Does not include this field

### Tasks:
- [ ] Confirm with spec owner whether `input` should be included
- [ ] If not needed, remove the field and update `_from_raw_info`

---

## Implementation Notes

### Already Correctly Implemented ✅
- `ActivityIDReusePolicy`, `ActivityIDConflictPolicy`, `ActivityExecutionStatus` enums (mapped to proto constants)
- `ActivityHandle` class with all methods
- `ActivityExecution`, `ActivityExecutionAsyncIterator`, `ActivityExecutionCount` classes
- `list_activities`, `count_activities`, `get_activity_handle` in `Client`
- `start_activity`, `execute_activity` in `Client`
- Interceptor methods: `start_activity`, `cancel_activity`, `terminate_activity`, `describe_activity`, `list_activities`, `count_activities`
- Input classes: `StartActivityInput`, `CancelActivityInput`, `TerminateActivityInput`, `DescribeActivityInput`, `ListActivitiesInput`, `CountActivitiesInput`
- `activity.Info` changes: `namespace`, `activity_run_id`, `in_workflow` property
- `AsyncActivityIDReference` workflow_id optionality
- `PendingActivityState` enum
- `ActivityFailedError` exception
- `ActivityExecutionDescription.retry_policy` is Optional

### Test Coverage
- Tests exist in `tests/test_activity.py` covering:
  - describe, get_result, get_activity_handle
  - list_activities, count_activities
  - manual completion/cancellation/failure/heartbeat
  - id_conflict_policy, id_reuse_policy
  - search_attributes, retry_policy
  - terminate

### Related Files to Update
- `temporalio/client.py` - Main implementation
- `temporalio/common.py` - Enum mapping
- `temporalio/activity.py` - Docstring fix
- `tests/test_activity_type_errors.py` - Type checking tests for new methods

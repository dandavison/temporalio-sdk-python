# Standalone Activity Implementation TODO

Comparison of spec (cross-sdk-design.md, Python section) against current PR implementation.

---

## 1. Evaluate `input` Field in `ActivityExecutionDescription`

**Spec Section:** 1
**Location:** `temporalio/client.py`, line ~3598

**Current:** Has `input: Sequence[Any]` field
**Spec:** Does not include this field

### Tasks:
- [ ] Confirm with spec owner whether `input` should be included
- [ ] If not needed, remove the field and update `_from_execution_info`

---

## Implementation Notes

### Already Correctly Implemented ✅
- `ActivityIDReusePolicy`, `ActivityIDConflictPolicy`, `ActivityExecutionStatus` enums (mapped to proto constants)
- `ActivityHandle` class with all methods
- `ActivityExecution`, `ActivityExecutionAsyncIterator`, `ActivityExecutionCount` classes
- `ActivityExecutionDescription` inherits from `ActivityExecution`
- `list_activities`, `count_activities`, `get_activity_handle` in `Client`
- `start_activity`, `execute_activity` in `Client`
- `start_activity_class`, `execute_activity_class` in `Client`
- `start_activity_method`, `execute_activity_method` in `Client`
- Interceptor methods: `start_activity`, `cancel_activity`, `terminate_activity`, `describe_activity`, `get_activity_result`, `list_activities`, `count_activities`
- Input classes: `StartActivityInput`, `CancelActivityInput`, `TerminateActivityInput`, `DescribeActivityInput`, `GetActivityResultInput`, `ListActivitiesInput`, `CountActivitiesInput`
- `activity.Info` changes: `namespace`, `activity_run_id`, `in_workflow` property
- `AsyncActivityIDReference` workflow_id optionality
- `PendingActivityState` enum
- `ActivityFailedError` exception
- `ActivityExecutionDescription.retry_policy` is Optional

### Test Coverage ✅
- Tests exist in `tests/test_activity.py` covering:
  - describe, get_result, get_activity_handle
  - list_activities, count_activities
  - manual completion/cancellation/failure/heartbeat
  - id_conflict_policy, id_reuse_policy
  - search_attributes, retry_policy
  - terminate
  - ActivityExecutionDescription inherits from ActivityExecution
  - All 7 interceptor methods have invocation tests:
    - `test_start_activity_calls_interceptor`
    - `test_get_activity_result_calls_interceptor`
    - `test_describe_activity_calls_interceptor`
    - `test_cancel_activity_calls_interceptor`
    - `test_terminate_activity_calls_interceptor`
    - `test_list_activities_calls_interceptor`
    - `test_count_activities_calls_interceptor`

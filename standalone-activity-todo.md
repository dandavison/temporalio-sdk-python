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

## 2. Evaluate `input` Field in `ActivityExecutionDescription`

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
- Interceptor methods: `start_activity`, `cancel_activity`, `terminate_activity`, `describe_activity`, `get_activity_result`, `list_activities`, `count_activities`
- Input classes: `StartActivityInput`, `CancelActivityInput`, `TerminateActivityInput`, `DescribeActivityInput`, `GetActivityResultInput`, `ListActivitiesInput`, `CountActivitiesInput`
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
  - ActivityExecutionDescription inherits from ActivityExecution
  - get_activity_result interceptor

### Related Files to Update
- `temporalio/client.py` - Main implementation
- `temporalio/common.py` - Enum mapping
- `temporalio/activity.py` - Docstring fix
- `tests/test_activity_type_errors.py` - Type checking tests for new methods

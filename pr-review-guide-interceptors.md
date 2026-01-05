# PR Review Guide: Activity Interceptors

This document compares the new client-side activity interceptors with the existing workflow-side activity interceptors.

---

## Overview

The new client-side activity API introduces 7 interceptor methods for activities not started by workflows:

| Client Interceptor Method | Purpose |
|--------------------------|---------|
| `start_activity` | Starting an activity |
| `get_activity_result` | Awaiting activity result |
| `describe_activity` | Getting activity description |
| `cancel_activity` | Cancelling an activity |
| `terminate_activity` | Terminating an activity |
| `list_activities` | Listing activity executions |
| `count_activities` | Counting activity executions |

---

## 1. `StartActivityInput` Comparison

### Workflow Implementation (Existing)

https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_interceptor.py#L247-L267

`temporalio/worker/_interceptor.py` (`StartActivityInput`)
```python
@dataclass
class StartActivityInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.start_activity`."""

    activity: str
    args: Sequence[Any]
    activity_id: str | None
    task_queue: str | None
    schedule_to_close_timeout: timedelta | None
    schedule_to_start_timeout: timedelta | None
    start_to_close_timeout: timedelta | None
    heartbeat_timeout: timedelta | None
    retry_policy: temporalio.common.RetryPolicy | None
    cancellation_type: temporalio.workflow.ActivityCancellationType
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    disable_eager_execution: bool
    versioning_intent: VersioningIntent | None
    summary: str | None
    priority: temporalio.common.Priority
    # The types may be absent
    arg_types: list[type] | None
    ret_type: type | None
```

### Client Implementation (New)

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7395-L7419

`temporalio/client.py` (`StartActivityInput`)
```python
@dataclass
class StartActivityInput:
    """Input for :py:meth:`OutboundInterceptor.start_activity`.

    .. warning::
       This API is experimental.
    """

    activity_type: str
    args: Sequence[Any]
    id: str
    task_queue: str
    result_type: Type | None
    schedule_to_close_timeout: timedelta | None
    start_to_close_timeout: timedelta | None
    schedule_to_start_timeout: timedelta | None
    heartbeat_timeout: timedelta | None
    id_reuse_policy: temporalio.common.ActivityIDReusePolicy
    id_conflict_policy: temporalio.common.ActivityIDConflictPolicy
    retry_policy: temporalio.common.RetryPolicy | None
    priority: temporalio.common.Priority
    search_attributes: temporalio.common.TypedSearchAttributes | None
    summary: str | None
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
```

### ⚠️ Discrepancies to Review

| Field | Workflow | Client | Issue? |
|-------|----------|--------|--------|
| Activity name | `activity: str` | `activity_type: str` | **Different field name** - intentional for clarity? |
| Activity ID | `activity_id: str \| None` | `id: str` | **Different name AND required** |
| Task queue | `task_queue: str \| None` | `task_queue: str` | Required in client |
| Type info | `arg_types`, `ret_type` | `result_type` | **Different approach** - workflow has both, client only has result |
| Cancellation | `cancellation_type` | ❌ Not present | OK - client uses separate `cancel()` |
| Versioning | `versioning_intent` | ❌ Not present | **Should this be supported?** |
| Eager exec | `disable_eager_execution` | ❌ Not present | OK - only relevant in workflow context |
| ID policies | ❌ Not present | `id_reuse_policy`, `id_conflict_policy` | OK - client-specific |
| Search attrs | ❌ Not present | `search_attributes` | OK - client-specific |
| RPC options | ❌ Not present | `rpc_metadata`, `rpc_timeout` | OK - client-specific |

---

## 2. Interceptor Method Comparison

### Workflow: `WorkflowOutboundInterceptor.start_activity`

https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_interceptor.py#L453-L459

`temporalio/worker/_interceptor.py` (`WorkflowOutboundInterceptor.start_activity`)
```python
def start_activity(
    self, input: StartActivityInput
) -> temporalio.workflow.ActivityHandle[Any]:
    """Called for every :py:func:`temporalio.workflow.start_activity` and
    :py:func:`temporalio.workflow.execute_activity` call.
    """
    return self.next.start_activity(input)
```

### Client: `OutboundInterceptor.start_activity`

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7847-L7853

`temporalio/client.py` (`OutboundInterceptor.start_activity`)
```python
async def start_activity(self, input: StartActivityInput) -> ActivityHandle[Any]:
    """Called for every :py:meth:`Client.start_activity` call.

    .. warning::
       This API is experimental.
    """
    return await self.next.start_activity(input)
```

### Key Differences

| Aspect | Workflow | Client |
|--------|----------|--------|
| Sync/Async | `def` (sync - deterministic) | `async def` |
| Return type | `temporalio.workflow.ActivityHandle` | `ActivityHandle` (from client module) |

---

## 3. Client-Only Interceptor Methods

These interceptor methods exist only in the client (no workflow equivalent):

### `get_activity_result`

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7468-L7479

`temporalio/client.py` (`GetActivityResultInput`)
```python
@dataclass
class GetActivityResultInput(Generic[ReturnType]):
    """Input for :py:meth:`OutboundInterceptor.get_activity_result`."""

    activity_id: str
    activity_run_id: str | None
    result_type: Type[ReturnType] | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
```

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7881-L7889

`temporalio/client.py` (`OutboundInterceptor.get_activity_result`)
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

**Note:** Workflow activities don't need this - the workflow runtime handles result fetching via the `ActivityHandle` returned from `start_activity`.

---

### `describe_activity`

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7454-L7464

`temporalio/client.py` (`DescribeActivityInput`)
```python
@dataclass
class DescribeActivityInput:
    """Input for :py:meth:`OutboundInterceptor.describe_activity`."""

    activity_id: str
    activity_run_id: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
```

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7871-L7879

`temporalio/client.py` (`OutboundInterceptor.describe_activity`)
```python
async def describe_activity(
    self, input: DescribeActivityInput
) -> ActivityExecutionDescription:
    """Called for every :py:meth:`ActivityHandle.describe` call."""
    return await self.next.describe_activity(input)
```

---

### `cancel_activity`

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7423-L7435

`temporalio/client.py` (`CancelActivityInput`)
```python
@dataclass
class CancelActivityInput:
    """Input for :py:meth:`OutboundInterceptor.cancel_activity`."""

    activity_id: str
    activity_run_id: str | None
    reason: str | None
    wait_for_cancel_completed: bool
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
```

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7855-L7861

`temporalio/client.py` (`OutboundInterceptor.cancel_activity`)
```python
async def cancel_activity(self, input: CancelActivityInput) -> None:
    """Called for every :py:meth:`ActivityHandle.cancel` call."""
    await self.next.cancel_activity(input)
```

**Contrast with workflow:** Workflow cancels activities via `ActivityHandle.cancel()` which sets `cancellation_type` at start time.

---

### `terminate_activity`

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7439-L7450

`temporalio/client.py` (`TerminateActivityInput`)
```python
@dataclass
class TerminateActivityInput:
    """Input for :py:meth:`OutboundInterceptor.terminate_activity`."""

    activity_id: str
    activity_run_id: str | None
    reason: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
```

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7863-L7869

`temporalio/client.py` (`OutboundInterceptor.terminate_activity`)
```python
async def terminate_activity(self, input: TerminateActivityInput) -> None:
    """Called for every :py:meth:`ActivityHandle.terminate` call."""
    await self.next.terminate_activity(input)
```

**Note:** Workflow has no equivalent - workflows cannot terminate individual activities.

---

### `list_activities`

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7483-L7495

`temporalio/client.py` (`ListActivitiesInput`)
```python
@dataclass
class ListActivitiesInput:
    """Input for :py:meth:`OutboundInterceptor.list_activities`."""

    query: str | None
    page_size: int
    next_page_token: bytes | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    limit: int | None
```

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7891-L7899

`temporalio/client.py` (`OutboundInterceptor.list_activities`)
```python
def list_activities(
    self, input: ListActivitiesInput
) -> ActivityExecutionAsyncIterator:
    """Called for every :py:meth:`Client.list_activities` call."""
    return self.next.list_activities(input)
```

**Note:** This is `def` not `async def` because it returns an async iterator.

---

### `count_activities`

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7499-L7508

`temporalio/client.py` (`CountActivitiesInput`)
```python
@dataclass
class CountActivitiesInput:
    """Input for :py:meth:`OutboundInterceptor.count_activities`."""

    query: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
```

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7901-L7909

`temporalio/client.py` (`OutboundInterceptor.count_activities`)
```python
async def count_activities(
    self, input: CountActivitiesInput
) -> ActivityExecutionCount:
    """Called for every :py:meth:`Client.count_activities` call."""
    return await self.next.count_activities(input)
```

---

## 4. Test Coverage

### Workflow Interceptor Tests

https://github.com/temporalio/sdk-python/blob/main/tests/worker/test_workflow.py#L8121-L8127

`tests/worker/test_workflow.py` (`HeaderWorkflowOutboundInterceptor.start_activity`)
```python
class HeaderWorkflowOutboundInterceptor(temporalio.worker.WorkflowOutboundInterceptor):
    def start_activity(
        self, input: temporalio.worker.StartActivityInput
    ) -> workflow.ActivityHandle:
        # Add a header to the outbound activity call
        input.headers = {"foo": Payload(data=b"bar")}
        return super().start_activity(input)
```

### Client Interceptor Tests

https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L58-L117

`tests/test_activity.py` (`ActivityTracingInterceptor`)
```python
class ActivityTracingInterceptor(Interceptor):
    """Test interceptor that tracks all activity interceptor calls."""

    def __init__(self):
        self.start_activity_calls: list[StartActivityInput] = []
        self.get_activity_result_calls: list[GetActivityResultInput] = []
        self.describe_activity_calls: list[DescribeActivityInput] = []
        self.cancel_activity_calls: list[CancelActivityInput] = []
        self.terminate_activity_calls: list[TerminateActivityInput] = []
        self.list_activities_calls: list[ListActivitiesInput] = []
        self.count_activities_calls: list[CountActivitiesInput] = []

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        return ActivityTracingOutboundInterceptor(self, next)
```

Individual test coverage:

| Interceptor Method | Test |
|-------------------|------|
| `start_activity` | `test_start_activity_calls_interceptor` (line 161) |
| `get_activity_result` | `test_get_activity_result_calls_interceptor` (line 188) |
| `describe_activity` | `test_describe_activity_calls_interceptor` (line 221) |
| `cancel_activity` | `test_cancel_activity_calls_interceptor` (line 249) |
| `terminate_activity` | `test_terminate_activity_calls_interceptor` (line 277) |
| `list_activities` | `test_list_activities_calls_interceptor` (line 305) |
| `count_activities` | `test_count_activities_calls_interceptor` (line 334) |

---

## 5. Review Checklist

### Naming Consistency
- [ ] **`activity` vs `activity_type`**: Workflow uses `activity`, client uses `activity_type`. Is this intentional?
- [ ] **`activity_id` vs `id`**: Different naming conventions between workflow and client

### Missing Features
- [ ] **`versioning_intent`**: Not present in client `StartActivityInput`. Should it be?
- [ ] **`arg_types`**: Workflow has `arg_types` for type hints, client only has `result_type`

### Experimental Warnings
- [x] All client interceptor methods have `.. warning:: This API is experimental.`
- [x] All client input dataclasses have `.. warning:: This API is experimental.`

### Test Coverage
- [x] All 7 client interceptor methods have individual tests
- [x] Tests verify interceptor is called and receives correct input

### Potential Issues
- [ ] **Field ordering in dataclasses**: Are fields in logical order matching the method signature?
- [ ] **Generic type on `GetActivityResultInput`**: Uses `Generic[ReturnType]` - verify this works correctly with interceptors


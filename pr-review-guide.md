# PR Review Guide: Client-Side Activity API

This document provides links comparing the new client-side activity API with the existing workflow-side activity API.

**Repository:** https://github.com/temporalio/sdk-python
**Branch:** `standalone-activity`

---

## 1. Start/Execute Activity (Functions)

Start an activity by passing an activity function reference. Returns a handle (`start_activity`) or awaits result (`execute_activity`).

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/workflow.py (`start_activity`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2267) | [temporalio/client.py (`Client.start_activity`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L1435) |
| [temporalio/workflow.py (`execute_activity`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2483) | [temporalio/client.py (`Client.execute_activity`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L1677) |
| [tests/worker/test_workflow.py (`SimpleActivityWorkflow`)](https://github.com/temporalio/sdk-python/blob/main/tests/worker/test_workflow.py#L815) | [tests/test_activity.py (`test_get_result`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L363) |

### Difference Analysis

| Parameter | Workflow | Client | Intentional? |
|-----------|----------|--------|--------------|
| `id` / `activity_id` | Optional (`activity_id: str \| None = None`) | **Required** (`id: str`) | ✅ **Yes**: Workflow activities can auto-generate IDs from workflow history; standalone activities need explicit IDs since there's no workflow context. |
| `task_queue` | Optional (defaults to workflow's task queue) | **Required** | ✅ **Yes**: Workflows have an inherent task queue; standalone activities have no default. |
| `id_reuse_policy` | Not present | Present with default `ALLOW_DUPLICATE` | ✅ **Yes**: Standalone activities support reusing IDs from closed activities; workflow activities are scoped to the workflow. |
| `id_conflict_policy` | Not present | Present with default `FAIL` | ✅ **Yes**: Standalone activities can conflict with running activities; workflow activities have unique IDs per workflow. |
| `search_attributes` | Not present | Present | ✅ **Yes**: Standalone activities are top-level entities visible in Temporal UI/CLI and need search attributes. |
| `cancellation_type` | Present | Not present | ✅ **Yes**: Workflow activities need to specify how cancellation is handled in the deterministic workflow context. |
| `versioning_intent` | Present (deprecated) | Not present | ✅ **Yes**: Only relevant for Worker Versioning in workflow contexts. |
| `rpc_metadata` / `rpc_timeout` | Not present | Present | ✅ **Yes**: Client calls are direct RPC; workflow scheduling is through commands. |

---

## 2. Start/Execute Activity (Callable Classes)

Start an activity defined as a callable class (with `__call__` method). Pass the class type; the worker registers an instance.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/workflow.py (`start_activity_class`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2643) | [temporalio/client.py (`Client.start_activity_class`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L1880) |
| [temporalio/workflow.py (`execute_activity_class`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2800) | [temporalio/client.py (`Client.execute_activity_class`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2072) |
| [tests/worker/test_workflow.py (`test_workflow_activity_callable_class`)](https://github.com/temporalio/sdk-python/blob/main/tests/worker/test_workflow.py#L3017) | [tests/test_activity.py (`test_start_activity_class_async`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L935) |

### Difference Analysis

Same differences as Section 1 (Start/Execute Activity Functions). The `_class` variants mirror the function variants with identical parameter differences.

---

## 3. Start/Execute Activity (Methods)

Start an activity defined as a method on a class. Pass an unbound method reference; the worker registers bound methods from an instance.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/workflow.py (`start_activity_method`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2957) | [temporalio/client.py (`Client.start_activity_method`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2219) |
| [temporalio/workflow.py (`execute_activity_method`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L3114) | [temporalio/client.py (`Client.execute_activity_method`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2366) |
| [tests/worker/test_workflow.py (`test_workflow_activity_method`)](https://github.com/temporalio/sdk-python/blob/main/tests/worker/test_workflow.py#L3067) | [tests/test_activity.py (`test_start_activity_method_async`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L1027) |

### Difference Analysis

Same differences as Section 1 (Start/Execute Activity Functions). The `_method` variants mirror the function variants with identical parameter differences.

---

## 4. Activity Handle

Handle to an activity execution for awaiting result, cancelling, describing, etc.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/workflow.py (`ActivityHandle`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2085) | [temporalio/client.py (`ActivityHandle`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4555) |
| — | [temporalio/client.py (`ActivityHandle.result`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4618) |
| — | [temporalio/client.py (`ActivityHandle.cancel`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4702) |
| — | [temporalio/client.py (`ActivityHandle.terminate`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4738) |
| — | [temporalio/client.py (`ActivityHandle.describe`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4769) |
| — | [temporalio/client.py (`Client.get_activity_handle`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2506) |

### Difference Analysis

| Aspect | Workflow | Client | Intentional? |
|--------|----------|--------|--------------|
| Base class | Extends `asyncio.Task` (awaitable directly) | Generic class with explicit `result()` method | ✅ **Yes**: Workflow activities integrate with the deterministic event loop and can be awaited directly. Client activities need explicit polling via RPC. |
| Result retrieval | `await handle` | `await handle.result()` | ✅ **Yes**: Different execution models require different APIs. |
| `activity_run_id` property | Not present | Present | ✅ **Yes**: Standalone activities have run IDs; workflow activities don't track this. |
| `cancel()` method | Simple `cancel()` | Rich `cancel(reason, wait_for_cancel_completed, ...)` | ✅ **Yes**: Client can wait for cancellation since it's non-deterministic; workflow cancellation must be immediate for replay. |
| `terminate()` method | Not present | Present | ✅ **Yes**: Only standalone activities support explicit termination. |
| `describe()` method | Not present | Present | ✅ **Yes**: Standalone activities are first-class entities that can be described. |
| `with_context()` method | Not present | Present | ✅ **Yes**: Allows setting serialization context for data converter customization. |
| Result caching | N/A (event loop managed) | Explicit `_cached_result` / `_result_fetched` | ✅ **Yes**: Client needs to cache to avoid repeated RPC calls. |

---

## 5. List/Count Activities

Query activity executions by visibility query.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| N/A (client-only feature) | [temporalio/client.py (`Client.list_activities`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2416) |
| — | [temporalio/client.py (`Client.count_activities`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2462) |
| — | [temporalio/client.py (`ActivityExecutionAsyncIterator`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L3991) |
| — | [temporalio/client.py (`ActivityExecution`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4099) |
| — | [temporalio/client.py (`ActivityExecutionCount`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4207) |
| — | [tests/test_activity.py (`test_list_activities`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L430) |
| — | [tests/test_activity.py (`test_count_activities`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L455) |

### Difference Analysis

✅ **Intentionally client-only**: Listing and counting are visibility/query operations that don't make sense in a workflow context. Workflows know about their own activities through their event history, not via queries.

These APIs mirror the existing `Client.list_workflows()` and `Client.count_workflows()` patterns.

---

## 6. Describe Activity

Get detailed information about an activity execution.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| N/A (client-only feature) | [temporalio/client.py (`ActivityHandle.describe`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4769) |
| — | [temporalio/client.py (`ActivityExecutionDescription`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4241) |
| — | [tests/test_activity.py (`test_describe`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L79) |

### Difference Analysis

✅ **Intentionally client-only**: Describe is a visibility operation. Workflows access activity status through event history, not via describe calls.

This API mirrors `WorkflowHandle.describe()`.

---

## 7. Cancel Activity

Request cancellation of an activity execution.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/workflow.py (`ActivityHandle.cancel`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2107) | [temporalio/client.py (`ActivityHandle.cancel`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4702) |
| — | [tests/test_activity.py (`test_manual_cancellation`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L561) |

### Difference Analysis

| Aspect | Workflow | Client | Intentional? |
|--------|----------|--------|--------------|
| Method signature | `cancel()` (no parameters) | `cancel(reason, wait_for_cancel_completed, rpc_metadata, rpc_timeout)` | ✅ **Yes** |
| `reason` parameter | Not present | Present | ✅ **Yes**: Standalone cancellation can record a reason for visibility. |
| `wait_for_cancel_completed` | Not present | Present | ✅ **Yes**: Client can block until cancellation completes; workflows cannot block for determinism. |
| RPC options | Not present | `rpc_metadata`, `rpc_timeout` | ✅ **Yes**: Client calls are direct RPC. |

---

## 8. Terminate Activity

Forcefully terminate an activity execution (client-only, no workflow equivalent).

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| N/A (client-only feature) | [temporalio/client.py (`ActivityHandle.terminate`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4738) |
| — | [tests/test_activity.py (`test_terminate`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L898) |

### Difference Analysis

✅ **Intentionally client-only**: Termination is an immediate, forceful stop that doesn't give the activity code a chance to clean up. Workflows schedule activities through commands and use cancellation semantics; termination is an external administrative action.

This mirrors `WorkflowHandle.terminate()`.

---

## 9. Async Activity Completion (Manual Completion)

Complete/fail/heartbeat an activity asynchronously from outside the activity execution context.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/client.py (`AsyncActivityHandle`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/client.py#L5355) | Same (existing API) |
| [temporalio/client.py (`Client.get_async_activity_handle`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/client.py#L2552) | Same (existing API) |
| — | [tests/test_activity.py (`test_manual_completion`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L524) |
| — | [tests/test_activity.py (`test_manual_failure`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L612) |
| — | [tests/test_activity.py (`test_manual_heartbeat`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L680) |

### Difference Analysis

✅ **Same API**: The existing `AsyncActivityHandle` for async completion works for both workflow-started and standalone activities. No changes needed because async completion is already a client-side operation that works with task tokens.

---

## 10. Interceptors

Intercept client-side activity operations.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/worker/_interceptor.py (`StartActivityInput`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_interceptor.py#L247) | [temporalio/client.py (`StartActivityInput`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7395) |
| [temporalio/worker/_interceptor.py (`WorkflowOutboundInterceptor.start_activity`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_interceptor.py#L453) | [temporalio/client.py (`OutboundInterceptor.start_activity`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7847) |
| — | [temporalio/client.py (`GetActivityResultInput`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7468) |
| — | [temporalio/client.py (`OutboundInterceptor.get_activity_result`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7881) |
| — | [temporalio/client.py (`CancelActivityInput`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7423) |
| — | [temporalio/client.py (`OutboundInterceptor.cancel_activity`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7855) |
| — | [temporalio/client.py (`TerminateActivityInput`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7439) |
| — | [temporalio/client.py (`OutboundInterceptor.terminate_activity`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7863) |
| — | [temporalio/client.py (`DescribeActivityInput`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7454) |
| — | [temporalio/client.py (`OutboundInterceptor.describe_activity`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7871) |
| — | [temporalio/client.py (`ListActivitiesInput`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7483) |
| — | [temporalio/client.py (`OutboundInterceptor.list_activities`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7891) |
| — | [temporalio/client.py (`CountActivitiesInput`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7499) |
| — | [temporalio/client.py (`OutboundInterceptor.count_activities`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L7901) |
| — | [tests/test_activity.py (`ActivityTracingInterceptor`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L100) |
| — | [tests/test_activity.py (`test_start_activity_calls_interceptor`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L161) |

### Difference Analysis

**`StartActivityInput` differences:**

| Field | Workflow Interceptor | Client Interceptor | Intentional? |
|-------|---------------------|-------------------|--------------|
| `activity` vs `activity_type` | `activity: str` | `activity_type: str` | ⚠️ **Naming inconsistency**: Consider aligning field names. |
| `activity_id` vs `id` | `activity_id: str \| None` | `id: str` (required) | ✅ **Yes**: Different requirements (see Section 1). |
| `task_queue` | `str \| None` | `str` (required) | ✅ **Yes**: Different defaults. |
| `cancellation_type` | Present | Not present | ✅ **Yes**: Workflow-specific. |
| `disable_eager_execution` | Present | Not present | ✅ **Yes**: Workflow-specific optimization. |
| `versioning_intent` | Present | Not present | ✅ **Yes**: Workflow versioning specific. |
| `id_reuse_policy` | Not present | Present | ✅ **Yes**: Standalone-specific. |
| `id_conflict_policy` | Not present | Present | ✅ **Yes**: Standalone-specific. |
| `search_attributes` | Not present | Present | ✅ **Yes**: Standalone activities are visible entities. |
| `rpc_metadata` / `rpc_timeout` | Not present | Present | ✅ **Yes**: Client calls are direct RPC. |
| `arg_types` / `ret_type` | Present | Not present | ⚠️ **Review**: Consider adding for type consistency. |

**Additional client-side interceptors** (all intentionally new):
- `GetActivityResultInput` / `get_activity_result` - For polling results
- `CancelActivityInput` / `cancel_activity` - For cancellation
- `TerminateActivityInput` / `terminate_activity` - For termination
- `DescribeActivityInput` / `describe_activity` - For describe
- `ListActivitiesInput` / `list_activities` - For listing
- `CountActivitiesInput` / `count_activities` - For counting

---

## 11. Activity Info

Information available within a running activity. Updated to support activities not started by a workflow.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/activity.py (`Info`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/activity.py#L74) | Same class, updated docstrings |
| [temporalio/activity.py (`Info.workflow_id`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/activity.py#L108) | Now `None` if activity not started by workflow |
| [temporalio/activity.py (`Info.workflow_run_id`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/activity.py#L114) | Now `None` if activity not started by workflow |
| [temporalio/activity.py (`Info.workflow_type`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/activity.py#L117) | Now `None` if activity not started by workflow |
| [temporalio/activity.py (`Info.activity_run_id`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/activity.py#L98) | Set for activities not started by workflow |
| [temporalio/activity.py (`Info.in_workflow`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/activity.py#L145) | Property to check if started by workflow |

### Difference Analysis

| Field | Before | After | Intentional? |
|-------|--------|-------|--------------|
| `workflow_id` | `str` (always set) | `str \| None` | ✅ **Yes**: Standalone activities don't have a parent workflow. |
| `workflow_run_id` | `str` (always set) | `str \| None` | ✅ **Yes**: Same reason. |
| `workflow_type` | `str` (always set) | `str \| None` | ✅ **Yes**: Same reason. |
| `activity_run_id` | Not present | `str \| None = None` | ✅ **Yes**: Standalone activities have their own run ID. None for workflow activities. |
| `in_workflow` property | Not present | Added | ✅ **Yes**: Convenience property to check `workflow_id is not None`. |

⚠️ **Breaking change note**: Existing code that assumes `workflow_id` is always set will need to handle `None`. The `in_workflow` property provides a clean way to check.

---

## 12. Common Enums

Enums for activity ID policies and execution status.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| N/A (client-only) | [temporalio/common.py (`ActivityIDReusePolicy`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/common.py) |
| — | [temporalio/common.py (`ActivityIDConflictPolicy`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/common.py) |
| — | [temporalio/common.py (`ActivityExecutionStatus`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/common.py) |
| — | [tests/test_activity.py (`test_id_conflict_policy_fail`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L721) |
| — | [tests/test_activity.py (`test_id_reuse_policy_reject_duplicate`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L774) |

### Difference Analysis

✅ **Intentionally new enums for standalone activities**:

- `ActivityIDReusePolicy` - Mirrors `WorkflowIDReusePolicy` for workflow IDs
- `ActivityIDConflictPolicy` - Mirrors `WorkflowIDConflictPolicy` for workflow IDs
- `ActivityExecutionStatus` - Mirrors `WorkflowExecutionStatus` for workflow status

These follow the same patterns as the existing workflow equivalents.

---

## 13. Type Tests

Static type checking tests for overload type inference.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| — | [tests/test_activity_type_errors.py](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity_type_errors.py) |

### Difference Analysis

✅ **New test file**: Tests pyright type inference for the new client-side activity API overloads. Ensures type safety for function, class, and method activity patterns.

---

## Summary of All Differences

### Intentional Differences (by design)

1. **Required vs optional parameters**: Client requires explicit `id` and `task_queue` since there's no workflow context to provide defaults.
2. **ID policies**: Client adds `id_reuse_policy` and `id_conflict_policy` since standalone activities are independent entities.
3. **Search attributes**: Client supports search attributes since standalone activities appear in visibility.
4. **RPC options**: Client exposes `rpc_metadata` and `rpc_timeout` since calls are direct RPC.
5. **No cancellation_type/versioning_intent**: These are workflow-specific concepts.
6. **Rich handle operations**: Client handle has `result()`, `cancel()`, `terminate()`, `describe()` since it's not integrated with an event loop.
7. **List/count/describe operations**: Client-only visibility operations.
8. **Activity Info changes**: Workflow fields are now optional; `activity_run_id` and `in_workflow` added.

### Items to Review

1. ⚠️ **Naming**: `activity` vs `activity_type` in interceptor inputs - consider alignment.
2. ⚠️ **Type info**: Workflow interceptor has `arg_types`/`ret_type` but client doesn't - consider adding for consistency.
3. ⚠️ **Breaking change**: `activity.Info.workflow_id` can now be `None` - document migration path.

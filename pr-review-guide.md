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

---

## 2. Start/Execute Activity (Callable Classes)

Start an activity defined as a callable class (with `__call__` method). Pass the class type; the worker registers an instance.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/workflow.py (`start_activity_class`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2643) | [temporalio/client.py (`Client.start_activity_class`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L1880) |
| [temporalio/workflow.py (`execute_activity_class`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2800) | [temporalio/client.py (`Client.execute_activity_class`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2072) |
| [tests/worker/test_workflow.py (`test_workflow_activity_callable_class`)](https://github.com/temporalio/sdk-python/blob/main/tests/worker/test_workflow.py#L3017) | [tests/test_activity.py (`test_start_activity_class_async`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L935) |

---

## 3. Start/Execute Activity (Methods)

Start an activity defined as a method on a class. Pass an unbound method reference; the worker registers bound methods from an instance.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/workflow.py (`start_activity_method`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2957) | [temporalio/client.py (`Client.start_activity_method`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2219) |
| [temporalio/workflow.py (`execute_activity_method`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L3114) | [temporalio/client.py (`Client.execute_activity_method`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2366) |
| [tests/worker/test_workflow.py (`test_workflow_activity_method`)](https://github.com/temporalio/sdk-python/blob/main/tests/worker/test_workflow.py#L3067) | [tests/test_activity.py (`test_start_activity_method_async`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L1027) |

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

---

## 6. Describe Activity

Get detailed information about an activity execution.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| N/A (client-only feature) | [temporalio/client.py (`ActivityHandle.describe`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4769) |
| — | [temporalio/client.py (`ActivityExecutionDescription`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4241) |
| — | [tests/test_activity.py (`test_describe`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L79) |

---

## 7. Cancel Activity

Request cancellation of an activity execution.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| [temporalio/workflow.py (`ActivityHandle.cancel`)](https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2107) | [temporalio/client.py (`ActivityHandle.cancel`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4702) |
| — | [tests/test_activity.py (`test_manual_cancellation`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L561) |

---

## 8. Terminate Activity

Forcefully terminate an activity execution (client-only, no workflow equivalent).

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| N/A (client-only feature) | [temporalio/client.py (`ActivityHandle.terminate`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L4738) |
| — | [tests/test_activity.py (`test_terminate`)](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L898) |

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

---

## 13. Type Tests

Static type checking tests for overload type inference.

| Workflow Implementation | Client Implementation |
|------------------------|----------------------|
| — | [tests/test_activity_type_errors.py](https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity_type_errors.py) |


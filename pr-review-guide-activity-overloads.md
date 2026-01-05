# PR Review Guide: Activity Start/Execute Overloads

This document compares the new client-side activity APIs with the existing workflow-side activity APIs to facilitate PR review.

---

## Overview

The new client-side activity API mirrors the existing workflow-side activity API, allowing activities to be started directly from a client without going through a workflow. This enables use cases like:

- One-off background tasks that don't need workflow orchestration
- Simpler programming model for single-activity workloads
- Direct activity invocation from external services

---

## 1. `start_activity` / `execute_activity` (Functions)

### Requirements

- Start an activity by passing an activity function reference
- Support async and sync activity functions
- Support single-param, no-param, and multi-param (via `args=`) variants
- Return type inference from the activity function signature
- `start_activity` returns a handle; `execute_activity` awaits the result

### Workflow Implementation (Existing)

https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2267-L2320

`temporalio/workflow.py` (`start_activity`)
```python
def start_activity(
    activity: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[Any]:
    """Start an activity and return its handle."""
```

https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2483-L2523

`temporalio/workflow.py` (`execute_activity`)
```python
async def execute_activity(
    activity: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    activity_id: str | None = None,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any:
    """Start an activity and wait for completion."""
```

### Client Implementation (New)

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L1435-L1514

`temporalio/client.py` (`Client.start_activity`)
```python
async def start_activity(
    self,
    activity: (
        str | Callable[..., Awaitable[ReturnType]] | Callable[..., ReturnType]
    ),
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: str,
    task_queue: str,
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.ActivityIDReusePolicy = ...,
    id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = ...,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    search_attributes: temporalio.common.TypedSearchAttributes | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = ...,
    rpc_metadata: Mapping[str, str] | None = None,
    rpc_timeout: timedelta | None = None,
) -> ActivityHandle[ReturnType]:
    """Start an activity by its function and return a handle.

    The activity must not have been started by a workflow.
    """
```

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L1677-L1736

`temporalio/client.py` (`Client.execute_activity`)
```python
async def execute_activity(
    self,
    activity: (
        str | Callable[..., Awaitable[ReturnType]] | Callable[..., ReturnType]
    ),
    arg: Any = temporalio.common._arg_unset,
    *,
    # ... same parameters as start_activity ...
) -> ReturnType:
    """Start an activity and wait for completion.

    This is a shortcut for ``await (await self.start_activity(...)).result()``.
    """
```

### Key Differences

| Aspect | Workflow | Client |
|--------|----------|--------|
| `id` parameter | Optional (`activity_id`) | **Required** (`id`) |
| `task_queue` | Optional (defaults to workflow's) | **Required** |
| ID policies | N/A | `id_reuse_policy`, `id_conflict_policy` |
| Search attributes | N/A | Supported |
| RPC options | N/A | `rpc_metadata`, `rpc_timeout` |
| Cancellation type | Supported | N/A (use `cancel()` method) |

### Workflow Tests

https://github.com/temporalio/sdk-python/blob/main/tests/worker/test_workflow.py#L815-L823

`tests/worker/test_workflow.py` (`SimpleActivityWorkflow`)
```python
@workflow.defn
class SimpleActivityWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            say_hello,
            name,
            schedule_to_close_timeout=timedelta(seconds=5),
        )
```

### Client Tests

https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L321-L347

`tests/test_activity.py` (`test_get_result`)
```python
async def test_get_result(client: Client):
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    activity_handle = await client.start_activity(
        increment,
        args=(1,),
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    async with Worker(client, task_queue=task_queue, activities=[increment]):
        assert await activity_handle.result() == 2
```

---

## 2. `start_activity_class` / `execute_activity_class` (Callable Classes)

### Requirements

- Start an activity defined as a callable class (with `__call__` method)
- Pass the **class type** (not an instance) to reference the activity
- The worker registers an **instance** of the class
- Enables stateful activities where instance holds configuration

### Workflow Implementation (Existing)

https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2643-L2679

`temporalio/workflow.py` (`start_activity_class`)
```python
def start_activity_class(
    activity: Type[Callable],
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    # ... same parameters as start_activity ...
) -> ActivityHandle[Any]:
    """Start an activity from a callable class.

    See :py:meth:`start_activity` for parameter and return details.
    """
```

https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2800-L2837

`temporalio/workflow.py` (`execute_activity_class`)
```python
async def execute_activity_class(
    activity: Type[Callable],
    arg: Any = temporalio.common._arg_unset,
    *,
    # ... same parameters ...
) -> Any:
    """Start an activity from a callable class and wait for completion.

    This is a shortcut for ``await`` :py:meth:`start_activity_class`.
    """
```

### Client Implementation (New)

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L1880-L1928

`temporalio/client.py` (`Client.start_activity_class`)
```python
async def start_activity_class(
    self,
    activity: Type[Callable],
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: str,
    task_queue: str,
    # ... same parameters as start_activity ...
) -> ActivityHandle[Any]:
    """Start an activity from a callable class.

    .. warning::
       This API is experimental.

    See :py:meth:`start_activity` for parameter and return details.
    """
```

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2072-L2120

`temporalio/client.py` (`Client.execute_activity_class`)
```python
async def execute_activity_class(
    self,
    activity: Type[Callable],
    arg: Any = temporalio.common._arg_unset,
    *,
    # ... same parameters ...
) -> Any:
    """Start an activity from a callable class and wait for completion.

    .. warning::
       This API is experimental.

    This is a shortcut for ``await`` :py:meth:`start_activity_class`.
    """
```

### Workflow Tests

https://github.com/temporalio/sdk-python/blob/main/tests/worker/test_workflow.py#L2994-L3028

`tests/worker/test_workflow.py` (`CallableClassActivity`, `test_workflow_activity_callable_class`)
```python
@activity.defn
class CallableClassActivity:
    def __init__(self, orig_field1: str) -> None:
        self.orig_field1 = orig_field1

    async def __call__(self, to_add: MyDataClass) -> MyDataClass:
        return MyDataClass(field1=self.orig_field1 + to_add.field1)


@workflow.defn
class ActivityCallableClassWorkflow:
    @workflow.run
    async def run(self, to_add: MyDataClass) -> MyDataClass:
        result = await workflow.execute_activity_class(
            CallableClassActivity, to_add, start_to_close_timeout=timedelta(seconds=30)
        )
        return result


async def test_workflow_activity_callable_class(client: Client):
    activity_instance = CallableClassActivity("in worker")
    async with new_worker(
        client, ActivityCallableClassWorkflow, activities=[activity_instance]
    ) as worker:
        result = await client.execute_workflow(...)
        assert result == MyDataClass(field1="in worker, workflow param")
```

### Client Tests

https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L935-L955

`tests/test_activity.py` (`test_start_activity_class_async`)
```python
async def test_start_activity_class_async(client: Client):
    """Test start_activity_class with an async callable class."""
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    handle = await client.start_activity_class(
        IncrementClass,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    async with Worker(client, task_queue=task_queue, activities=[IncrementClass()]):
        result = await handle.result()
        assert result == 2
```

https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L998-L1024

`tests/test_activity.py` (`test_start_activity_class_sync`)
```python
async def test_start_activity_class_sync(client: Client):
    """Test start_activity_class with a sync callable class."""
    import concurrent.futures

    # ... setup ...
    handle = await client.start_activity_class(
        SyncIncrementClass,
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    with concurrent.futures.ThreadPoolExecutor() as executor:
        async with Worker(
            client,
            task_queue=task_queue,
            activities=[SyncIncrementClass()],
            activity_executor=executor,
        ):
            result = await handle.result()
            assert result == 2
```

### Type Tests

https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity_type_errors.py#L355-L398

`tests/test_activity_type_errors.py` (type inference tests)
```python
async def _test_start_activity_class_single_param() -> None:
    client = Client(service_client=Mock(spec=ServiceClient))

    _handle: ActivityHandle[int] = await client.start_activity_class(
        IncrementClass,
        1,
        id="activity-id",
        task_queue="tq",
        start_to_close_timeout=timedelta(seconds=5),
    )
```

---

## 3. `start_activity_method` / `execute_activity_method` (Methods)

### Requirements

- Start an activity defined as a method on a class
- Pass an **unbound method reference** (e.g., `MyClass.my_method`)
- The worker registers the **bound method** from an instance
- Enables grouping related activities in a class with shared state

### Workflow Implementation (Existing)

https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L2957-L2993

`temporalio/workflow.py` (`start_activity_method`)
```python
def start_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    # ... same parameters as start_activity ...
) -> ActivityHandle[Any]:
    """Start an activity from a method.

    See :py:meth:`start_activity` for parameter and return details.
    """
```

https://github.com/temporalio/sdk-python/blob/main/temporalio/workflow.py#L3114-L3150

`temporalio/workflow.py` (`execute_activity_method`)
```python
async def execute_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    # ... same parameters ...
) -> Any:
    """Start an activity from a method and wait for completion.

    This is a shortcut for ``await`` :py:meth:`start_activity_method`.
    """
```

### Client Implementation (New)

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2219-L2267

`temporalio/client.py` (`Client.start_activity_method`)
```python
async def start_activity_method(
    self,
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: str,
    task_queue: str,
    # ... same parameters as start_activity ...
) -> ActivityHandle[Any]:
    """Start an activity from a method.

    .. warning::
       This API is experimental.

    See :py:meth:`start_activity` for parameter and return details.
    """
```

https://github.com/temporalio/sdk-python/blob/standalone-activity/temporalio/client.py#L2366-L2414

`temporalio/client.py` (`Client.execute_activity_method`)
```python
async def execute_activity_method(
    self,
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    # ... same parameters ...
) -> Any:
    """Start an activity from a method and wait for completion.

    .. warning::
       This API is experimental.

    This is a shortcut for ``await`` :py:meth:`start_activity_method`.
    """
```

### Workflow Tests

https://github.com/temporalio/sdk-python/blob/main/tests/worker/test_workflow.py#L3040-L3080

`tests/worker/test_workflow.py` (`MethodActivity`, `test_workflow_activity_method`)
```python
class MethodActivity:
    def __init__(self, orig_field1: str) -> None:
        self.orig_field1 = orig_field1

    @activity.defn(name="custom-name")
    async def add(self, to_add: MyDataClass) -> MyDataClass:
        return MyDataClass(field1=self.orig_field1 + to_add.field1)


@workflow.defn
class ActivityMethodWorkflow:
    @workflow.run
    async def run(self, to_add: MyDataClass) -> MyDataClass:
        ret = await workflow.execute_activity_method(
            MethodActivity.add, to_add, start_to_close_timeout=timedelta(seconds=30)
        )
        return ret


async def test_workflow_activity_method(client: Client):
    activity_instance = MethodActivity("in worker")
    async with new_worker(
        client,
        ActivityMethodWorkflow,
        activities=[activity_instance.add, activity_instance.add_multi],
    ) as worker:
        result = await client.execute_workflow(...)
```

### Client Tests

https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity.py#L1027-L1049

`tests/test_activity.py` (`test_start_activity_method_async`)
```python
async def test_start_activity_method_async(client: Client):
    """Test start_activity_method with an async method."""
    activity_id = str(uuid.uuid4())
    task_queue = str(uuid.uuid4())

    holder = ActivityHolder()
    handle = await client.start_activity_method(
        ActivityHolder.async_increment,  # Unbound method reference
        1,
        id=activity_id,
        task_queue=task_queue,
        start_to_close_timeout=timedelta(seconds=5),
    )

    async with Worker(
        client,
        task_queue=task_queue,
        activities=[holder.async_increment],  # Bound method from instance
    ):
        result = await handle.result()
        assert result == 2
```

### Type Tests

https://github.com/temporalio/sdk-python/blob/standalone-activity/tests/test_activity_type_errors.py#L444-L492

`tests/test_activity_type_errors.py` (type inference tests)
```python
async def _test_start_activity_method_unbound() -> None:
    client = Client(service_client=Mock(spec=ServiceClient))

    # Using unbound method reference
    _handle: ActivityHandle[int] = await client.start_activity_method(
        ActivityHolder.increment_method,
        args=[1],
        id="activity-id",
        task_queue="tq",
        start_to_close_timeout=timedelta(seconds=5),
    )
```

---

## Summary: Overload Structure

Each method has multiple `@overload` declarations to support type inference:

| Overload Pattern | Description |
|-----------------|-------------|
| `CallableAsyncNoParam[ReturnType]` | Async function with no parameters |
| `CallableSyncNoParam[ReturnType]` | Sync function with no parameters |
| `CallableAsyncSingleParam[ParamType, ReturnType]` | Async function with one parameter |
| `CallableSyncSingleParam[ParamType, ReturnType]` | Sync function with one parameter |
| `Callable[..., Awaitable[ReturnType]]` with `args=` | Async function with multiple parameters |
| `Callable[..., ReturnType]` with `args=` | Sync function with multiple parameters |
| `str` (activity name) | Dynamic activity invocation by name |

For `_class` variants, these are wrapped in `Type[...]`.
For `_method` variants, these use `MethodAsync*` / `MethodSync*` protocols.

---

## Review Checklist

- [ ] Overload signatures match between workflow and client implementations
- [ ] Docstrings are consistent and accurate
- [ ] `id` and `task_queue` are required for client methods (unlike workflow)
- [ ] The `.. warning:: This API is experimental` note is present on `_class` and `_method` variants
- [ ] Type inference works correctly (verified by type tests)
- [ ] Integration tests cover async/sync variants and single-param/no-param cases


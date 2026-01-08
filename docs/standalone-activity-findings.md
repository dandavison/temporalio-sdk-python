# Standalone Activity Verification Findings

## API Operations (1a1e74e)

| gRPC Operation | Python SDK | Go SDK (WIP) | Notes |
|----------------|------------|--------------|-------|
| StartActivityExecution | ✓ start_activity | ✓ ExecuteActivity | |
| DescribeActivityExecution | ✓ describe | ✓ Describe | |
| PollActivityExecution | ✓ result | ✓ Get | Used internally for result |
| ListActivityExecutions | ✓ list_activities | ✓ ListActivities | |
| CountActivityExecutions | ✓ count_activities | ✓ CountActivities | |
| RequestCancelActivityExecution | ✓ cancel | ✓ Cancel | |
| TerminateActivityExecution | ✓ terminate | ✓ Terminate | |
| DeleteActivityExecution | ✗ | ✗ | Not in spec |
| UpdateActivityOptions | ✗ | ✗ | Workflow-only (requires workflow execution) |
| PauseActivity | ✗ | ✗ | Workflow-only (requires workflow execution) |
| UnpauseActivity | ✗ | ✗ | Workflow-only (requires workflow execution) |
| ResetActivity | ✗ | ✗ | Workflow-only (requires workflow execution) |

## Spec vs Python Implementation

### StartActivityInput Fields

| Spec Field | Python | Status |
|------------|--------|--------|
| activity_type | ✓ activity_type | OK |
| args | ✓ args | OK |
| id | ✓ id | OK |
| task_queue | ✓ task_queue | OK |
| result_type | ✓ result_type | OK |
| schedule_to_close_timeout | ✓ | OK |
| schedule_to_start_timeout | ✓ | OK |
| start_to_close_timeout | ✓ | OK |
| heartbeat_timeout | ✓ | OK |
| id_reuse_policy | ✓ | OK |
| id_conflict_policy | ✓ | OK |
| retry_policy | ✓ | OK |
| search_attributes | ✓ | OK |
| summary | ✓ | OK |
| priority | ✓ | OK |
| headers | ✓ | OK |
| rpc_metadata | ✓ | OK |
| rpc_timeout | ✓ | OK |

### CancelActivityInput Fields

| Spec Field | Python | Status |
|------------|--------|--------|
| activity_id | ✓ | OK |
| activity_run_id | ✓ | OK |
| reason | ✓ | OK |
| wait_for_cancel_completed | ✗ | Not in gRPC API; spec outdated |
| rpc_metadata | ✓ | OK |
| rpc_timeout | ✓ | OK |

**Note**: `wait_for_cancel_completed` is in Python spec but NOT in gRPC API. Correctly omitted from implementation.

### ActivityInfo Fields

| Spec Field | Python | Status |
|------------|--------|--------|
| namespace | ✓ | OK |
| activity_run_id | ✓ | OK |
| workflow_id (nullable) | ✓ | OK |
| workflow_namespace (deprecated) | ✓ | OK |
| workflow_run_id (nullable) | ✓ | OK |
| workflow_type (nullable) | ✓ | OK |
| in_workflow property | ✓ | OK |

## Python vs Go SDK Discrepancies

### 1. Method naming
- Go: `ExecuteActivity` (single method, returns handle)
- Python: `start_activity` (returns handle) + `execute_activity` (waits for result)
- **Assessment**: Both valid per spec; Python has explicit separation

### 2. ActivityHandle.Get vs result
- Go: `Get(ctx, valuePtr)` - writes to pointer
- Python: `result()` - returns value
- **Assessment**: Language-idiomatic difference

### 3. Describe options
- Go: `DescribeActivityOptions` struct (empty, for future compat)
- Python: `include_input`, `include_outcome`, `long_poll_token` params
- **Assessment**: Python exposes more API options

### 4. ActivityExecutionDescription
- Go: Has `DataConverter`, `FailureConverter` for lazy decoding
- Python: Decodes eagerly, stores decoded values
- **Assessment**: Design difference; Python simpler for users

### 5. List return type
- Go: `iter.Seq2[*ActivityExecutionMetadata, error]` (Go 1.23 iterator)
- Python: `ActivityExecutionAsyncIterator` (async iterator)
- **Assessment**: Language-idiomatic

## Issues Found

### 1. Missing DeleteActivityExecution
- API has `DeleteActivityExecution` RPC
- Not in spec, not implemented
- **Assessment**: Intentionally omitted (spec decision)

### 2. ActivityExecutionOutcome exposure
- Added in recent change with `include_outcome` describe option
- Go SDK doesn't expose this yet
- **Assessment**: Python ahead of Go here

## Workflow vs Client API Comparison

| Parameter | workflow.start_activity | client.start_activity | Notes |
|-----------|------------------------|----------------------|-------|
| activity | ✓ | ✓ | |
| args | ✓ | ✓ | |
| task_queue | Optional (defaults to workflow's) | **Required** | Different semantics |
| result_type | ✓ | ✓ | |
| schedule_to_close_timeout | ✓ | ✓ | |
| schedule_to_start_timeout | ✓ | ✓ | |
| start_to_close_timeout | ✓ | ✓ | |
| heartbeat_timeout | ✓ | ✓ | |
| retry_policy | ✓ | ✓ | |
| priority | ✓ | ✓ | |
| summary | ✓ | ✓ | |
| activity_id | Optional (auto-generated) | **Required** (`id`) | Named differently |
| cancellation_type | ✓ | ✗ | Workflow-only concept |
| versioning_intent | ✓ | ✗ | Workflow-only concept |
| id_reuse_policy | ✗ | ✓ | Standalone-only |
| id_conflict_policy | ✗ | ✓ | Standalone-only |
| search_attributes | ✗ | ✓ | Standalone has visibility |
| rpc_metadata | ✗ | ✓ | Client-level concept |
| rpc_timeout | ✗ | ✓ | Client-level concept |

### Key Differences

1. **task_queue**: Optional in workflow (defaults to workflow's queue); required in client
2. **activity_id vs id**: Named `activity_id` in workflow (optional), `id` in client (required)
3. **cancellation_type**: Workflow-only; controls how workflow reacts to cancellation
4. **versioning_intent**: Workflow-only; for worker deployment versioning
5. **id_reuse_policy/id_conflict_policy**: Standalone-only; controls ID collision behavior
6. **search_attributes**: Standalone-only; standalone activities have visibility

## Test Coverage

| Feature | Test File | Status |
|---------|-----------|--------|
| start_activity | test_activity.py | ✓ |
| execute_activity | test_activity.py | ✓ |
| describe | test_activity.py | ✓ |
| describe options | test_activity.py::TestDescribeOptions | ✓ |
| cancel | test_activity.py | ✓ |
| terminate | test_activity.py::test_terminate | ✓ |
| list_activities | test_activity.py::test_list_activities | ✓ |
| count_activities | test_activity.py::test_count_activities | ✓ |
| interceptors | test_activity.py | ✓ |
| ActivityInfo fields | Needs review | ? |


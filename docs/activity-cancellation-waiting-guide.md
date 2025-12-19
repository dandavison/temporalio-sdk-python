# Activity Cancellation Waiting: Cross-SDK Implementation Guide

This guide explains how different Temporal SDKs implement the concept of "waiting for activity cancellation confirmation" before proceeding with workflow execution.

## The Core Problem

When a workflow requests cancellation of an activity, there are three possible behaviors:

1. **Try Cancel**: Send the cancel request and immediately continue workflow execution
2. **Wait for Cancellation Completed**: Send the cancel request and wait until the activity actually confirms it's cancelled before continuing
3. **Abandon**: Don't even send a cancel request, just immediately continue workflow execution

The key consideration: **This is purely a workflow-side (SDK) concern about deterministic replay behavior.** No server-side RPC command changes—it's about when the workflow code should unblock after requesting cancellation.

---

## Go SDK Implementation: Boolean `WaitForCancellation`

### API Design

Go uses a **simple boolean** field in `ActivityOptions`:

```go
// internal/internal_activity.go
type ExecuteActivityOptions struct {
    ActivityID             string
    TaskQueueName          string
    ScheduleToCloseTimeout time.Duration
    ScheduleToStartTimeout time.Duration
    StartToCloseTimeout    time.Duration
    HeartbeatTimeout       time.Duration
    WaitForCancellation    bool        // <-- THE FIELD
    RetryPolicy            *commonpb.RetryPolicy
    DisableEagerExecution  bool
    // ...
}
```

### Usage

```go
ao := workflow.ActivityOptions{
    StartToCloseTimeout: 1 * time.Minute,
    HeartbeatTimeout:    5 * time.Second,
    WaitForCancellation: true,   // Wait for activity to confirm cancellation
}
ctx = workflow.WithActivityOptions(ctx, ao)
```

There's also a helper function:

```go
// workflow/activity_options.go
func WithWaitForCancellation(ctx Context, wait bool) Context {
    return internal.WithWaitForCancellation(ctx, wait)
}
```

### Behavior Mapping

| Go `WaitForCancellation` | Equivalent Behavior |
|--------------------------|---------------------|
| `false` (default)        | Try Cancel - immediate return |
| `true`                   | Wait for cancellation completed |
| (no equivalent)          | Abandon - no cancel request sent |

### Implementation Details

In Go's workflow event handler (`internal/internal_event_handlers.go`):

```go
type scheduledActivity struct {
    callback             ResultHandler
    waitForCancelRequest bool    // <-- stored here
    handled              bool
    activityType         ActivityType
}

func (wc *workflowEnvironmentImpl) RequestCancelActivity(activityID ActivityID) {
    command := wc.commandsHelper.requestCancelActivityTask(activityID.id)
    activity := command.getData().(*scheduledActivity)

    // Key logic: if command is done OR we don't wait, immediately notify callback
    if command.isDone() || !activity.waitForCancelRequest {
        activity.handle(nil, ErrCanceled)
    }
    // Otherwise, we wait for the cancellation event from the server
}
```

### Child Workflow Support

Go also uses `WaitForCancellation` for child workflows:

```go
cwo := workflow.ChildWorkflowOptions{
    WorkflowRunTimeout:  time.Minute,
    WaitForCancellation: true,
}
ctx = workflow.WithChildOptions(ctx, cwo)
```

---

## Java SDK Implementation: Enum `ActivityCancellationType`

### API Design

Java uses a **three-value enum** in `ActivityOptions`:

```java
// io/temporal/activity/ActivityCancellationType.java
public enum ActivityCancellationType {
    /**
     * Wait for the Activity Execution to confirm any requested cancellation.
     * An Activity Execution must Heartbeat to receive a cancellation notification.
     * This can block the cancellation of a Workflow Execution for a long time if the
     * Activity Execution doesn't Heartbeat or chooses to ignore the cancellation request.
     */
    WAIT_CANCELLATION_COMPLETED,

    /**
     * In case of activity's scope cancellation send an Activity cancellation request
     * to the server, and report cancellation to the Workflow Execution by causing
     * the activity stub call to fail with CanceledFailure
     */
    TRY_CANCEL,

    /**
     * Do not request cancellation of the Activity Execution at all (no request is
     * sent to the server) and immediately report cancellation to the Workflow
     * Execution by causing the activity stub call to fail with CanceledFailure immediately.
     */
    ABANDON,
}
```

### Usage

```java
ActivityOptions options = ActivityOptions.newBuilder()
    .setStartToCloseTimeout(Duration.ofMinutes(1))
    .setHeartbeatTimeout(Duration.ofSeconds(5))
    .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
    .build();

MyActivities activity = Workflow.newActivityStub(MyActivities.class, options);
```

### Implementation Details

In Java's `ActivityStateMachine.java`:

```java
public void cancel() {
    if (cancellationType == ActivityCancellationType.ABANDON) {
        // Immediately notify without sending cancel command
        notifyCanceled(false);
    } else if (!isFinalState()) {
        // Send cancel command (handles TRY_CANCEL and WAIT differently later)
        explicitEvent(ExplicitEvent.CANCEL);
    }
}

// Called when cancel command is created
private void notifyCanceledIfTryCancelImmediately() {
    if (cancellationType == ActivityCancellationType.TRY_CANCEL) {
        // Immediately notify workflow code
        notifyCanceled(false);
    }
    // WAIT_CANCELLATION_COMPLETED does NOT notify here - waits for event
}

// Called when cancel event is observed
private void notifyCancellationFromEvent() {
    if (cancellationType == ActivityCancellationType.WAIT_CANCELLATION_COMPLETED) {
        // Only now do we notify the workflow
        notifyCanceled(true);
    }
}
```

---

## Core SDK (Rust) & Python SDK Implementation

### Proto Definition

The Core SDK defines the enum in protobuf:

```protobuf
// workflow_commands.proto
enum ActivityCancellationType {
    // Initiate a cancellation request and immediately report cancellation to the workflow.
    TRY_CANCEL = 0;

    // Wait for activity cancellation completion. Note that activity must heartbeat to receive a
    // cancellation notification. This can block the cancellation for a long time if activity
    // doesn't heartbeat or chooses to ignore the cancellation request.
    WAIT_CANCELLATION_COMPLETED = 1;

    // Do not request cancellation of the activity and immediately report cancellation to the
    // workflow
    ABANDON = 2;
}
```

### Python Usage

```python
# temporalio/workflow.py
class ActivityCancellationType(IntEnum):
    """How an activity cancellation should be handled."""
    TRY_CANCEL = 0
    WAIT_CANCELLATION_COMPLETED = 1
    ABANDON = 2

# Usage
handle = workflow.start_activity(
    my_activity,
    schedule_to_close_timeout=timedelta(minutes=5),
    heartbeat_timeout=timedelta(seconds=10),
    cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
)
```

---

## Comparison Table

| SDK | API Design | Default | Values |
|-----|------------|---------|--------|
| **Go** | Boolean `WaitForCancellation` | `false` (Try Cancel) | `true`/`false` |
| **Java** | Enum `ActivityCancellationType` | `TRY_CANCEL` | `TRY_CANCEL`, `WAIT_CANCELLATION_COMPLETED`, `ABANDON` |
| **Python** | Enum `ActivityCancellationType` | `TRY_CANCEL` | `TRY_CANCEL`, `WAIT_CANCELLATION_COMPLETED`, `ABANDON` |
| **TypeScript** | Enum `ActivityCancellationType` | `TRY_CANCEL` | Same as Java |
| **.NET** | Enum `ActivityCancellationType` | `TryCancel` | Same as Java |

### Mapping Go ↔ Java/Core

| Go `WaitForCancellation` | Java/Core `ActivityCancellationType` |
|--------------------------|--------------------------------------|
| `false` | `TRY_CANCEL` |
| `true` | `WAIT_CANCELLATION_COMPLETED` |
| (not available) | `ABANDON` |

**Note**: Go doesn't have a direct boolean equivalent to `ABANDON`. To achieve abandon-like behavior in Go, you would need to handle it differently (e.g., using a disconnected context).

---

## Why This Matters

### Workflow Determinism

This setting affects **workflow determinism during replay**. When replaying:

- **TRY_CANCEL**: The workflow continues immediately after the cancel request command is recorded
- **WAIT_CANCELLATION_COMPLETED**: The workflow waits until it sees the `ActivityTaskCanceled` event in history
- **ABANDON**: The workflow continues immediately without any cancel command

### Practical Considerations

1. **Activity must heartbeat** to receive cancellation notification
2. **WAIT_CANCELLATION_COMPLETED can block indefinitely** if:
   - Activity doesn't heartbeat
   - Activity ignores the cancellation
   - Activity hangs before heartbeating
3. **TRY_CANCEL** is the safest default for most use cases
4. **ABANDON** is useful when you don't care about the activity's fate at all

---

## Standalone Activities: Not Applicable

**Important**: This `ActivityCancellationType` concept is **workflow-specific**. It does NOT apply to standalone activities (activities started directly by a client).

For standalone activities:
- The client calls `cancel()` → RPC sent to server → Activity receives cancel on next heartbeat
- There is no "cancellation type" because there's no workflow determinism to maintain
- The client can separately call `result()` to wait for the activity to complete/fail/cancel

---

## Historical Note

The difference in API design (Go's boolean vs Java/Core's enum) reflects:
1. **Different design philosophies**: Go tends toward simpler APIs; Java/Core prefer explicit enum variants
2. **Evolution over time**: The enum approach allows for future extension without breaking changes
3. **ABANDON was added later**: Go's boolean design doesn't cleanly accommodate the third option

The Core SDK chose the enum approach, and all Core-based SDKs (Python, TypeScript, .NET) inherit this design.


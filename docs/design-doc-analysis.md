# Design Document Analysis: Corrections and Clarifications

This document analyzes the official Temporal design documents for Standalone Activities and identifies where they confirm, clarify, or contradict statements made in the PR review guide.

---

## 1. Activity ID Space: CORRECTION NEEDED ⚠️

### What I Said (pr-review-guide.md)
> **Open question**: Do standalone and workflow activity IDs share a namespace?

### What the Design Docs Say
> "Standalone activities will live in their **own ID space** to avoid colliding with workflows. E.g. it will be possible for a running workflow and a running activity to share the same ID."
>
> "The reason for not sharing the ID space is to avoid activities showing up in the workflow list and vice versa and avoid confusing 'workflow execution already started' error messages to users starting an activity."

### Correction
**Standalone activities have a SEPARATE ID space from workflows.** A workflow and a standalone activity CAN have the same ID simultaneously. This is intentional to:
1. Avoid cross-contamination in list views
2. Avoid confusing error messages

**However**, workflow activities (started from workflows) and standalone activities may still have potential for confusion - the docs don't explicitly address whether `activity_id="1"` from a workflow and `activity_id="1"` from a standalone start would conflict.

---

## 2. Visibility for Workflow Activities: CONFIRMED ✅

### What I Said
> **Incrementally different**: Currently client-only because standalone activities are being added to visibility first... Visibility queries for workflow-started activities are a natural evolution.

### What the Design Docs Say
> "[Post MLP] Visibility for workflow activities."
>
> "[Post MLP] Power workflow activities with the CHASM component - to avoid maintaining two separate activity implementations, the current activity implementation will be deprecated and will not receive new features."

### Confirmation
This is explicitly on the roadmap. Workflow activities WILL eventually:
1. Be powered by CHASM (same as standalone)
2. Have visibility (list/count/describe)
3. The current workflow activity implementation will be **deprecated**

---

## 3. CHASM Unification: CONFIRMED ✅

### What I Said
> As CHASM unifies... the underlying execution model will unify.

### What the Design Docs Say
> "We will **copy** the current activity code to fit into a CHASM standalone component. The new code should be reusable for standalone activities, in-workflow activity, and potentially future state machines."
>
> "the current activity implementation will be deprecated and will not receive new features"

### Confirmation
CHASM will eventually power ALL activities. The current workflow activity code is being preserved for backwards compatibility but will be deprecated.

---

## 4. Activity Info Fields: CONFIRMED ✅

### What I Said
> `workflow_id`, `workflow_run_id`, `workflow_type` are now `str | None`

### What the Design Docs Say
> "Workflow execution, workflow namespace and workflow type in activity info will be empty in case of a standalone activity and thus the activity logger tags may need to adapt accordingly."

### Confirmation
Exactly as implemented. These fields are empty/None for standalone activities.

---

## 5. Run ID for Activities: NEW INFORMATION 📝

### What I Said
> `activity_run_id` property... Standalone activities have run IDs; workflow activities don't track this.

### What the Design Docs Say
> "There is also a system-generated run ID, similar to workflow run IDs, where the activity ID can be reused after completion."
>
> From the proto: `ActivityExecution { activity_id, run_id }`

### Clarification
Standalone activities have a run_id that works like workflow run_id:
- System-generated UUID
- Allows activity_id reuse after completion
- Can target specific run or "latest run" (empty run_id)

---

## 6. Cancellation Types: CONFIRMED ✅

### What I Said (cancellation-types-guide.md)
> `ActivityCancellationType` is about workflow replay determinism... Standalone activities don't need it.

### What the Design Docs Say
The design docs show cancel/terminate as separate RPC operations without `cancellation_type`:
- `RequestCancelActivityExecution`
- `TerminateActivityExecution`

No mention of `cancellation_type` for standalone activities. The concepts map to:
- TRY_CANCEL → `cancel()` (returns immediately)
- WAIT_CANCELLATION_COMPLETED → Not directly available; caller must poll for completion after cancel
- ABANDON → Just don't await, or use `terminate()`

### Confirmation
Cancellation type is not part of the standalone activity API. The `cancel()` method returns immediately after requesting cancellation.

---

## 7. Pause/Reset/UpdateOptions: NEW INFORMATION 📝

### What I Said
Not explicitly addressed.

### What the Design Docs Say
> "[MLP GA] Pause, reset, update options."

And from the proto:
```protobuf
bool paused = 19;
message PauseInfo { ... }
```

### New Information
Standalone activities will support:
- **Pause/Unpause** - Stop scheduling new attempts
- **Reset** - Reset to initial state
- **UpdateOptions** - Change retry policy, timeouts, etc. at runtime

These are planned for MLP GA (not pre-release). The SDK Python PR may not include these yet.

---

## 8. Memo: CORRECTION NEEDED ⚠️

### What I Said
Not explicitly addressed (assumed similar to workflows).

### What the Design Docs Say
In the proto definition, `memo` is struck through:
```protobuf
~~temporal.api.common.v1.Memo memo = 10;~~
```

### Correction
**Standalone activities do NOT support memos** (at least not in MLP). This is different from workflows which do support memos.

---

## 9. CLI Unification: NEW INFORMATION 📝

### What I Said
Not addressed.

### What the Design Docs Say
> "`temporal activity` should operate on both standalone and in-workflow activities."
>
> "We will differentiate between a standalone and in-workflow activity by checking the ID provided. In-workflow activities must be addressed with a `--workflow-id`, and standalone activities with an `--activity-id`."

### New Information
The CLI will be **unified** for both activity types:
- `--workflow-id` → targets workflow activity
- `--activity-id` (without `--workflow-id`) → targets standalone activity

Commands like `complete`, `fail`, `pause`, `reset` will work for both.

---

## 10. Starting Standalone Activities from Workflows: NEW INFORMATION 📝

### What I Said
Not addressed.

### What the Design Docs Say (TS example)
```typescript
// workflow.ts
const handle = await workflow.startStandaloneActivity(activity, {
    id: 'my-business-id',
    taskQueue: 'foo',
    startToCloseTimeout: '10 minutes',
});
const handle = await workflow.getStandaloneActivityHandle(myActivityId);
await handle.result();
```

### New Information
Workflows will eventually be able to:
1. **Start** standalone activities (not just regular workflow activities)
2. **Get handles** to existing standalone activities
3. **Await results** of standalone activities

This is marked as "Post MLP" and depends on Nexus external callers.

---

## 11. New Terminal Statuses: NEW INFORMATION 📝

### What I Said
`ActivityExecutionStatus` mirrors `WorkflowExecutionStatus`.

### What the Design Docs Say
```protobuf
enum ActivityExecutionStatus {
    ACTIVITY_EXECUTION_STATUS_RUNNING = 1;
    ACTIVITY_EXECUTION_STATUS_COMPLETED = 2;
    ACTIVITY_EXECUTION_STATUS_FAILED = 3;
    ACTIVITY_EXECUTION_STATUS_CANCELED = 4;
    ACTIVITY_EXECUTION_STATUS_TERMINATED = 5;
    ACTIVITY_EXECUTION_STATUS_TIMED_OUT = 6;
}
```

### Clarification
This DOES mirror workflow statuses but with activity-specific semantics:
- `TERMINATED` - Forcefully stopped, activity can't react
- `TIMED_OUT` - schedule-to-start or schedule-to-close timeout reached
- `CANCELED` - Only if activity allows CancelledError to bubble out

---

## 12. Eager Execution: NEW INFORMATION 📝

### What I Said
Not addressed.

### What the Design Docs Say
> "`request_eager_execution` - If set to `true` the caller is expected to have a worker available and capable of processing the task. The returned task will be marked as started..."
>
> "[Post MLP] Eager start - similar to eager workflow start for latency optimization"

### New Information
Standalone activities will support **eager execution** (returning the first task inline in the start response) for latency optimization. This is planned for post-MLP.

---

## 13. Completion Callbacks: NEW INFORMATION 📝

### What I Said
Not addressed.

### What the Design Docs Say
```protobuf
// Callbacks to be called by the server when this activity reaches a terminal status.
repeated temporal.api.common.v1.Callback completion_callbacks = 14;
```

### New Information
Standalone activities support **completion callbacks** - server-side webhooks called when the activity reaches a terminal status. This enables push-based integrations.

---

## 14. Search Attributes: CONFIRMED ✅

### What I Said
> Client supports search attributes since standalone activities appear in visibility.

### What the Design Docs Say
System search attributes for standalone activities:
```
ActivityId, RunId, ActivityType, TaskQueue, StartTime, ExecutionTime,
CloseTime, ExecutionStatus, ExecutionDuration, StateTransitionCount, PauseInfo
```

### Confirmation
Confirmed with specific attribute list. Note: `PauseInfo` is a search attribute, enabling queries like "find all paused activities."

---

## 15. On-Conflict Options: NEW INFORMATION 📝

### What I Said
`id_conflict_policy` with default `FAIL`.

### What the Design Docs Say
```protobuf
// Defines actions to be done to the existing running activity when
// ID_CONFLICT_POLICY_USE_EXISTING is used.
temporal.api.activity.v1.OnConflictOptions on_conflict_options = 17;
```

### New Information
When using `USE_EXISTING` conflict policy, you can specify **actions to take on the existing activity** (e.g., update its input, add links). This is more sophisticated than just "return the existing handle."

---

## Summary of Changes to Previous Guides

### Corrections Needed

| Topic | Previous Statement | Correction |
|-------|-------------------|------------|
| ID Space | Open question about shared namespace | **Separate ID spaces** - workflow and standalone activity can share same ID |
| Memo | Assumed similar to workflows | **Memos NOT supported** for standalone activities |

### Confirmed Statements

| Topic | Statement |
|-------|-----------|
| Visibility for workflow activities | Will come post-MLP |
| CHASM unification | Confirmed, current impl will be deprecated |
| Activity Info fields | `workflow_*` fields are None for standalone |
| Cancellation types | Not applicable to standalone (confirmed) |
| Search attributes | Supported (with specific list) |

### New Information to Add

| Topic | Information |
|-------|-------------|
| Pause/Reset/UpdateOptions | Coming in MLP GA |
| CLI unification | Both activity types via same commands |
| Workflow → Standalone activity | Future capability |
| Eager execution | Post-MLP latency optimization |
| Completion callbacks | Server-side webhooks |
| On-conflict options | Actions on existing activity |
| Run ID | System-generated, enables ID reuse |

---

## Open Questions Resolved

1. **Q: Do standalone and workflow activities share an ID space?**
   - **A: No.** They have separate ID spaces. A workflow and standalone activity can have the same ID.

2. **Q: Will workflow activities gain visibility?**
   - **A: Yes.** Explicitly planned for post-MLP.

3. **Q: Will cancellation_type apply to standalone?**
   - **A: No.** Different model with cancel/terminate as separate operations.

## New Open Questions from Design Docs

1. **Activity ID collision between workflow activities and standalone activities** - If a workflow starts an activity with ID "foo" and someone starts a standalone activity with ID "foo", do these collide? The docs say workflows and activities are separate, but what about workflow-started activities vs standalone?

2. **Memo removal rationale** - Why are memos not supported? Is this a permanent limitation or just MLP scope reduction?

3. **CLI unified commands timing** - Will the Python SDK ship with CLI parity, or will CLI updates come later?


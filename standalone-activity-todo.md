# Standalone Activity SDK Verification TODO

## Sources
- API: `~/src/temporal-all/repos/api` @ 1a1e74e
- Spec: `docs/standalone-activity-cross-sdk-design.md`
- Go SDK WIP: maciejdudko/non-workflow-activities (fetched locally)

## Tasks

### 1. API Inventory
- [x] List all gRPC operations in API for standalone activities
- [x] Map each to Python SDK implementation status

### 2. Spec Compliance (Python section)
- [x] `Client.start_activity` / `execute_activity` params
- [x] `ActivityHandle` methods: `result`, `describe`, `cancel`, `terminate`
- [x] `ActivityExecution` / `ActivityExecutionDescription` fields
- [x] `ActivityExecutionAsyncIterator` for list
- [x] `ActivityExecutionCount` for count
- [x] `OutboundInterceptor` methods
- [x] `StartActivityInput` fields
- [x] `DescribeActivityInput` fields
- [x] `GetActivityResultInput` fields
- [x] `CancelActivityInput` fields
- [x] `TerminateActivityInput` fields
- [x] `ListActivitiesInput` fields
- [x] `CountActivitiesInput` fields
- [x] `activity.Info` changes (nullable workflow fields, new `activity_run_id`, `namespace`)
- [x] `AsyncActivityIDReference` changes (workflow_id is Optional[str])

### 3. Go SDK Comparison
- [x] Compare `StartActivityOptions` fields
- [x] Compare `ActivityHandle` interface
- [x] Compare `ActivityExecutionDescription` fields
- [x] Compare interceptor inputs
- [x] Note any discrepancies

### 4. Workflow Activity API Comparison
- [x] Compare `workflow.start_activity` vs `client.start_activity` params
- [x] Document intentional vs unintentional differences

### 5. Test Coverage
- [x] Integration tests for each operation (existing)
- [ ] ActivityInfo standalone fields test
- [ ] Error case tests review

### 6. Findings Document
- [x] Create `docs/standalone-activity-findings.md`
- [x] Document omissions
- [x] Document errors
- [x] Document discrepancies with Go SDK

## Progress Log
- Initial pass complete: API inventory, spec compliance, Go comparison
- Findings documented in docs/standalone-activity-findings.md
- Remaining: AsyncActivityIDReference, workflow vs client API comparison, ActivityInfo test

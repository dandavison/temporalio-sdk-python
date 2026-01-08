# Standalone Activity SDK Verification TODO

## Sources
- API: `~/src/temporal-all/repos/api` @ 1a1e74e
- Spec: `docs/standalone-activity-cross-sdk-design.md`
- Go SDK WIP: https://github.com/maciejdudko/temporal-sdk-go/tree/non-workflow-activities

## Tasks

### 1. API Inventory
- [ ] List all gRPC operations in API for standalone activities
- [ ] Map each to Python SDK implementation status

### 2. Spec Compliance (Python section)
- [ ] `Client.start_activity` / `execute_activity` params
- [ ] `ActivityHandle` methods: `result`, `describe`, `cancel`, `terminate`
- [ ] `ActivityExecution` / `ActivityExecutionDescription` fields
- [ ] `ActivityExecutionAsyncIterator` for list
- [ ] `ActivityExecutionCount` for count
- [ ] `OutboundInterceptor` methods
- [ ] `StartActivityInput` fields
- [ ] `DescribeActivityInput` fields
- [ ] `GetActivityResultInput` fields
- [ ] `CancelActivityInput` fields
- [ ] `TerminateActivityInput` fields
- [ ] `ListActivitiesInput` fields
- [ ] `CountActivitiesInput` fields
- [ ] `activity.Info` changes (nullable workflow fields, new `activity_run_id`, `namespace`)
- [ ] `AsyncActivityIDReference` changes

### 3. Go SDK Comparison
- [ ] Compare `StartActivityOptions` fields
- [ ] Compare `ActivityHandle` interface
- [ ] Compare `ActivityExecutionDescription` fields
- [ ] Compare interceptor inputs
- [ ] Note any discrepancies

### 4. Workflow Activity API Comparison
- [ ] Compare `workflow.start_activity` vs `client.start_activity` params
- [ ] Document intentional vs unintentional differences

### 5. Test Coverage
- [ ] Type-level tests for new types
- [ ] Integration tests for each operation
- [ ] Error case tests

### 6. Findings Document
- [ ] Create `docs/standalone-activity-findings.md`
- [ ] Document omissions
- [ ] Document errors
- [ ] Document discrepancies with Go SDK

## Progress Log
- Started: (date)

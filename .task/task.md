# Task: Make it easier for people to get started with Temporal

Today, when a user is first getting started with Temporal, they are instructed to write Temporal
Python code and also either run a local server, or use Temporal Cloud (once they've got an account
etc). Envconfig provides an easy transition from the former to the latter: they connect their clients
using an envconfig API, and thereafter can switch by changing config without any code changes.

To make this even easier, in this task we're going to explore running a local server from Python.
The SDK already contains facilities to download and start/shutdown a local server, but they are
exposed as a test utility only. Here we will explore exposing this as non-test functionality. The
class name should be LocalServer, and we should expose an async context manager and an instance with
start/shutdown methods.

## Decisions

1. **Module location**: `temporalio.local_server`

2. **Lifecycle API**: Both `async with LocalServer() as server` and explicit `start()` / `shutdown()`
   at arbitrary code locations.

3. **Relationship to `WorkflowEnvironment.start_local()`**: Share code to the extent reasonable.


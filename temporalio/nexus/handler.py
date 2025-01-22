from __future__ import annotations

import json
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Generic, Optional, TypeVar

import nexusrpc.handler

from temporalio.client import (
    Client,
    WorkflowHandle,
)
from temporalio.common import CompletionCallback
from temporalio.types import (
    MethodAsyncSingleParam,
)

O = TypeVar("O")
I = TypeVar("I")


# TODO(dan): should this take [O] or [W, O]?
class AsyncWorkflowOperationResult(nexusrpc.handler.AsyncOperationResult, Generic[O]):
    @classmethod
    def from_workflow_handle(
        cls, workflow_handle: WorkflowHandle[Any, O]
    ) -> "AsyncWorkflowOperationResult[O]":
        return cls(token=cls._encode_token(workflow_handle))

    @staticmethod
    def _encode_token(workflow_handle: WorkflowHandle[Any, O]) -> str:
        return json.dumps([workflow_handle.id, workflow_handle.run_id])

    @staticmethod
    def _decode_token(token: str) -> tuple[str, str]:
        try:
            workflow_id, run_id = map(str, json.loads(token))
        except Exception as e:
            raise ValueError(f"Invalid token: {token}") from e
        return workflow_id, run_id

    @staticmethod
    def to_workflow_handle(token: str, client: Client) -> WorkflowHandle[Any, O]:
        workflow_id, run_id = AsyncWorkflowOperationResult._decode_token(token)
        return client.get_workflow_handle(workflow_id, run_id=run_id)


async def start_workflow(
    workflow_run_method: MethodAsyncSingleParam[Any, I, O],
    arg: I,
    id: str,
    options: nexusrpc.handler.StartOperationOptions,
) -> AsyncWorkflowOperationResult[O]:
    # TODO(dan): handle client and task queue provided by user?
    _client = client()
    _task_queue = task_queue()
    completion_callbacks = (
        [CompletionCallback(url=options.callback_url, header=options.callback_header)]
        if options.callback_url
        else []
    )
    workflow_handle = await _client.start_workflow(
        workflow_run_method,
        arg,
        id=id,
        task_queue=_task_queue,
        completion_callbacks=completion_callbacks,
    )
    return AsyncWorkflowOperationResult.from_workflow_handle(workflow_handle)


async def fetch_workflow_info(
    operation_token: str,
    options: nexusrpc.handler.FetchOperationInfoOptions,
) -> nexusrpc.handler.OperationInfo:
    # TODO(dan)
    return nexusrpc.handler.OperationInfo(
        token=operation_token,
        status=nexusrpc.handler.OperationState.RUNNING,
    )


async def fetch_workflow_result(
    operation_token: str,
    options: nexusrpc.handler.FetchOperationResultOptions,
) -> O:
    # TODO(dan): handle client provided by user?
    _client = client()
    workflow_handle = AsyncWorkflowOperationResult.to_workflow_handle(
        operation_token, _client
    )
    return await workflow_handle.result()


async def cancel_workflow(
    operation_token: str,
    options: nexusrpc.handler.CancelOperationOptions,
) -> None:
    # TODO(dan): handle client provided by user?
    _client = client()
    workflow_handle = AsyncWorkflowOperationResult.to_workflow_handle(
        operation_token, _client
    )
    await workflow_handle.cancel()


_current_context: ContextVar[_Context] = ContextVar("nexus-handler")


@dataclass
class _Context:
    client: Optional[Client]
    task_queue: Optional[str]


def client() -> Client:
    context = _current_context.get(None)
    if context is None:
        raise RuntimeError("Not in Nexus handler context")
    if context.client is None:
        raise RuntimeError("Nexus handler client not set")
    return context.client


def task_queue() -> str:
    context = _current_context.get(None)
    if context is None:
        raise RuntimeError("Not in Nexus handler context")
    if context.task_queue is None:
        raise RuntimeError("Nexus handler task queue not set")
    return context.task_queue

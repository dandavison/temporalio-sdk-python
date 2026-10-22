"""Stream errors."""

from __future__ import annotations


class StreamError(Exception):
    """Base class of stream errors."""


class StreamClosedError(StreamError):
    """The stream is closed."""


class StreamNotFoundError(StreamError):
    """The stream does not exist."""


class StreamExpiredError(StreamError):
    """The stream existed but has been deleted."""


class StreamNotOwnerError(StreamError):
    """Only the stream's owner may close it."""


class StreamCursorError(StreamError):
    """The cursor belongs to another stream."""


class SeqDivergentError(StreamError):
    """The seq is already stored with different content."""


class SeqStaleError(StreamError):
    """The seq is lower than the producer's latest: a stale sequence."""


class EpochFencedError(StreamError):
    """The epoch is lower than one already seen for this producer."""


class SpeculativeWorkflowTaskError(StreamError):
    """Appends from speculative workflow tasks are not supported.

    Only the native backend raises it. Neither the worker nor Core can tell that a task is
    speculative, so on Redis such appends are stored.
    """


class SeqGapError(StreamError):
    """The seq is not the next seq: an epoch's first append has seq 0, and each later one the next."""


class StreamSettingsMismatchError(StreamError):
    """A create of a stream that exists with other settings."""


class StreamTooLargeError(StreamError):
    """The append exceeds a size limit."""


class StreamUnavailableError(StreamError):
    """The store could not be reached: transient failures are retried for 30 seconds."""


class WorkflowAppendModeMismatchError(StreamError):
    """A sink-mode workflow append to a run whose first workflow append fixed commit mode: the
    run's workers have definitions of its workflow that disagree on commit mode.

    A commit-mode completion in the wrong mode does not raise it: the server fails the workflow
    task with an application failure of type ``BAD_STREAM_APPEND``, and the task is retried.
    """

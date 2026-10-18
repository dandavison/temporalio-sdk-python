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
    """Appends from speculative workflow tasks are not supported."""


class SeqGapError(StreamError):
    """The seq is not the next seq: an epoch's first append has seq 0, and each later one the next."""


class StreamTooLargeError(StreamError):
    """The append exceeds a size limit."""


class StreamUnavailableError(StreamError):
    """The store could not be reached, after retrying."""


class WorkflowAppendModeMismatchError(StreamError):
    """A workflow append is in the other mode from the one its run fixed: the run's workers
    disagree on the append mode.
    """

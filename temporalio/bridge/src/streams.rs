use prost::Message;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use std::sync::Arc;
use temporalio_streams::proto::api::{NativeOptions, RedisOptions};
use temporalio_streams::{Error, Streams};

use crate::client;
use crate::runtime;

pyo3::create_exception!(temporal_sdk_bridge, StreamError, PyException);

/// A stream backend from temporalio-streams. Requests and responses are that crate's encoded protos.
#[pyclass]
pub struct StreamsRef {
    streams: Arc<Streams>,
    runtime: runtime::Runtime,
}

pub fn new_native_streams(client: &client::ClientRef, options: &[u8]) -> PyResult<StreamsRef> {
    let options =
        NativeOptions::decode(options).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(StreamsRef {
        streams: Arc::new(Streams::native(client.connection.clone(), options)),
        runtime: client.runtime.clone(),
    })
}

/// A Redis backend, which calls the server through the client's connection for the owners'
/// notifiers.
pub fn new_redis_streams<'a>(
    py: Python<'a>,
    client: &client::ClientRef,
    options: &[u8],
) -> PyResult<Bound<'a, PyAny>> {
    let options =
        RedisOptions::decode(options).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let runtime = client.runtime.clone();
    let connection = client.connection.clone();
    client.runtime.future_into_py(py, async move {
        Ok(StreamsRef {
            streams: Arc::new(
                Streams::redis(options, Some(connection))
                    .await
                    .map_err(stream_error)?,
            ),
            runtime,
        })
    })
}

#[pymethods]
impl StreamsRef {
    /// Runs the named call on an encoded request, returning the encoded response.
    fn call<'p>(
        &self,
        py: Python<'p>,
        name: String,
        request: Vec<u8>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let streams = self.streams.clone();
        self.runtime.future_into_py(py, async move {
            streams.call(&name, &request).await.map_err(stream_error)
        })
    }
}

/// Raises StreamError with the failure's type name and message.
fn stream_error(e: Error) -> PyErr {
    StreamError::new_err((e.type_name().to_string(), e.to_string()))
}

use bytes::Bytes;
use http::Response;
use monoio::io::{
    sink::{Sink, SinkExt},
    stream::Stream,
    AsyncReadRent, AsyncWriteRent,
};
use monoio_http::{
    common::{
        body::{Body, HttpBody},
        error::HttpError,
        request::{Request, RequestHead},
        IntoParts,
    },
    h1::{
        codec::{
            decoder::{DecodeError, PayloadDecoder},
            ClientCodec,
        },
        payload::{fixed_payload_pair, stream_payload_pair, Payload},
    },
    h2::client::SendRequest,
};


use crate::pool::{Key, Poolable, Pooled};
use std::cell::Cell;

// Thread-local variable to track if the current request is a HEAD request
thread_local! {
    static IS_HEAD_REQUEST: Cell<bool> = const { Cell::new(false) };
}

/// Set whether the current request is a HEAD request
fn set_head_request(is_head: bool) {
    IS_HEAD_REQUEST.with(|cell| cell.set(is_head));
}

/// Check if the current request is a HEAD request
fn is_head_request() -> bool {
    IS_HEAD_REQUEST.with(|cell| cell.get())
}


/// A HTTP/1.1 connection.
pub struct Http1Connection<IO: AsyncWriteRent> {
    framed: ClientCodec<IO>,
    using: bool,
    open: bool,
}

impl<IO: AsyncWriteRent> Http1Connection<IO> {
    pub fn new(framed: ClientCodec<IO>) -> Self {
        Self {
            framed,
            using: false,
            open: true,
        }
    }


}

impl<IO: AsyncWriteRent> Poolable for Http1Connection<IO> {
    #[inline]
    fn is_open(&self) -> bool {
        match self {
            Self { using, open, .. } => *open && !*using,
        }
    }
}

impl<IO: AsyncReadRent + AsyncWriteRent> Http1Connection<IO> {
    pub async fn send_request<R, E>(
        &mut self,
        request: R,
    ) -> (Result<Response<HttpBody>, HttpError>, bool)
    where
        ClientCodec<IO>: Sink<R, Error = E>,
        E: std::fmt::Debug + Into<HttpError>,
    {
        let handle = &mut self.framed;

        if let Err(e) = handle.send_and_flush(request).await {
            #[cfg(feature = "logging")]
            tracing::error!("send upstream request error {:?}", e);
            self.open = false;
            return (Err(e.into()), false);
        }

        match handle.next().await {
            Some(Ok(resp)) => {
                let (parts, payload_decoder) = resp.into_parts();
                
                match payload_decoder {
                    PayloadDecoder::None => {
                        let payload = Payload::None;
                        let response = Response::from_parts(parts, payload.into());
                        (Ok(response), false)
                    }
                    PayloadDecoder::Fixed(_) => {
                        if is_head_request() {
                            // For HEAD requests, servers may return Content-Length but no actual body
                            // RFC 7231: A HEAD response has the same headers as a GET response, 
                            // but MUST NOT contain a message-body
                            let payload = Payload::None;
                            let response = Response::from_parts(parts, payload.into());
                            (Ok(response), false)
                        } else {
                            let mut framed_payload = payload_decoder.with_io(handle);
                            let (payload, payload_sender) = fixed_payload_pair();
                            if let Some(data) = framed_payload.next_data().await {
                                payload_sender.feed(data)
                            }
                            let payload = Payload::Fixed(payload);
                            let response = Response::from_parts(parts, payload.into());
                            (Ok(response), false)
                        }
                    }
                    PayloadDecoder::Streamed(_) => {
                        if is_head_request() {
                            // For HEAD requests, servers may return Transfer-Encoding: chunked but no actual body
                            // RFC 7231: A HEAD response has the same headers as a GET response, 
                            // but MUST NOT contain a message-body
                            let payload = Payload::None;
                            let response = Response::from_parts(parts, payload.into());
                            (Ok(response), false)
                        } else {
                            let mut framed_payload = payload_decoder.with_io(handle);
                            let (payload, mut payload_sender) = stream_payload_pair();
                            loop {
                                match framed_payload.next_data().await {
                                    Some(Ok(data)) => payload_sender.feed_data(Some(data)),
                                    Some(Err(e)) => {
                                        #[cfg(feature = "logging")]
                                        tracing::error!("decode upstream response error {:?}", e);
                                        self.open = false;
                                        return (Err(e), false);
                                    }
                                    None => {
                                        payload_sender.feed_data(None);
                                        break;
                                    }
                                }
                            }
                            let payload = Payload::Stream(payload);
                            let response = Response::from_parts(parts, payload.into());
                            (Ok(response), false)
                        }
                    }
                }
            }
            Some(Err(e)) => {
                #[cfg(feature = "logging")]
                tracing::error!("decode upstream response error {:?}", e);
                self.open = false;
                (Err(e), false)
            }
            None => {
                #[cfg(feature = "logging")]
                tracing::error!("upstream return eof");
                self.open = false;
                (Err(DecodeError::UnexpectedEof.into()), false)
            }
        }
    }

    /// Send an already reconstructed request (for internal use by HttpConnection)
    pub async fn send_reconstructed_request<E>(
        &mut self,
        request: monoio_http::common::request::Request<monoio_http::h1::payload::Payload>,
    ) -> (Result<Response<HttpBody>, HttpError>, bool)
    where
        ClientCodec<IO>: Sink<monoio_http::common::request::Request<monoio_http::h1::payload::Payload>, Error = E>,
        E: std::fmt::Debug + Into<HttpError>,
    {
        let handle = &mut self.framed;

        if let Err(e) = handle.send_and_flush(request).await {
            #[cfg(feature = "logging")]
            tracing::error!("send upstream request error {:?}", e);
            self.open = false;
            return (Err(e.into()), false);
        }

        match handle.next().await {
            Some(Ok(resp)) => {
                let (parts, payload_decoder) = resp.into_parts();
                
                match payload_decoder {
                    PayloadDecoder::None => {
                        let payload = Payload::None;
                        let response = Response::from_parts(parts, payload.into());
                        (Ok(response), false)
                    }
                    PayloadDecoder::Fixed(_) => {
                        if is_head_request() {
                            // For HEAD requests, servers may return Content-Length but no actual body
                            // RFC 7231: A HEAD response has the same headers as a GET response, 
                            // but MUST NOT contain a message-body
                            let payload = Payload::None;
                            let response = Response::from_parts(parts, payload.into());
                            (Ok(response), false)
                        } else {
                            let mut framed_payload = payload_decoder.with_io(handle);
                            let (payload, payload_sender) = fixed_payload_pair();
                            if let Some(data) = framed_payload.next_data().await {
                                payload_sender.feed(data)
                            }
                            let payload = Payload::Fixed(payload);
                            let response = Response::from_parts(parts, payload.into());
                            (Ok(response), false)
                        }
                    }
                    PayloadDecoder::Streamed(_) => {
                        if is_head_request() {
                            // For HEAD requests, servers may return Transfer-Encoding: chunked but no actual body
                            // RFC 7231: A HEAD response has the same headers as a GET response, 
                            // but MUST NOT contain a message-body
                            let payload = Payload::None;
                            let response = Response::from_parts(parts, payload.into());
                            (Ok(response), false)
                        } else {
                            let mut framed_payload = payload_decoder.with_io(handle);
                            let (payload, mut payload_sender) = stream_payload_pair();
                            loop {
                                match framed_payload.next_data().await {
                                    Some(Ok(data)) => payload_sender.feed_data(Some(data)),
                                    Some(Err(e)) => {
                                        #[cfg(feature = "logging")]
                                        tracing::error!("decode upstream response error {:?}", e);
                                        self.open = false;
                                        return (Err(e), false);
                                    }
                                    None => {
                                        payload_sender.feed_data(None);
                                        break;
                                    }
                                }
                            }
                            let payload = Payload::Stream(payload);
                            let response = Response::from_parts(parts, payload.into());
                            (Ok(response), false)
                        }
                    }
                }
            }
            Some(Err(e)) => {
                #[cfg(feature = "logging")]
                tracing::error!("decode upstream response error {:?}", e);
                self.open = false;
                (Err(e), false)
            }
            None => {
                #[cfg(feature = "logging")]
                tracing::error!("upstream return eof");
                self.open = false;
                (Err(DecodeError::UnexpectedEof.into()), false)
            }
        }
    }
}

/// A HTTP/2 connection.
#[derive(Clone, Debug)]
pub struct Http2Connection {
    tx: SendRequest<Bytes>,
}

impl Poolable for Http2Connection {
    #[inline]
    fn is_open(&self) -> bool {
        !self.tx.has_conn_error()
    }
}

impl Http2Connection {
    pub fn new(tx: SendRequest<Bytes>) -> Self {
        Self { tx }
    }

    #[allow(dead_code)]
    fn to_owned(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }

    pub fn conn_error(&self) -> Option<HttpError> {
        self.tx.conn_error()
    }
}

impl Http2Connection {
    pub async fn send_request<R>(
        &mut self,
        request: R,
    ) -> (Result<Response<HttpBody>, HttpError>, bool)
    where
        R: IntoParts<Parts = RequestHead>,
        R::Body: Body<Data = Bytes, Error = HttpError>,
    {
        let mut client = match self.tx.clone().ready().await {
            Ok(client) => client,
            Err(e) => {
                return (Err(e.into()), false);
            }
        };

        let (parts, mut body) = request.into_parts();
        let h2_request = Request::from_parts(parts, ());

        let (response, mut send_stream) = match client.send_request(h2_request, false) {
            Ok((response, send_stream)) => (response, send_stream),
            Err(e) => {
                return (Err(e.into()), false);
            }
        };

        while let Some(data) = body.next_data().await {
            match data {
                Ok(data) => {
                    if let Err(e) = send_stream.send_data(data, false) {
                        #[cfg(feature = "logging")]
                        tracing::error!("H2 client body send error {:?}", e);
                        return (Err(e.into()), false);
                    }
                }
                Err(e) => {
                    #[cfg(feature = "logging")]
                    tracing::error!("H2 request body stream error {:?}", e);
                    return (Err(e), false);
                }
            }
        }
        // Mark end of stream
        let _ = send_stream.send_data(Bytes::new(), true);

        let response = match response.await {
            Ok(response) => response,
            Err(e) => {
                #[cfg(feature = "logging")]
                tracing::error!("H2 client response error {:?}", e);
                return (Err(e.into()), false);
            }
        };

        let (parts, body) = response.into_parts();
        (Ok(Response::from_parts(parts, body.into())), true)
    }
}

/// A unified representation of an HTTP connection, supporting both HTTP/1.1 and HTTP/2 protocols.
///
/// This enum is designed to work with monoio's native IO traits, which are optimized for io_uring.
/// It allows for efficient handling of both HTTP/1.1 and HTTP/2 connections within the same
/// abstraction.
pub enum HttpConnection<K: Key, IO: AsyncReadRent + AsyncWriteRent> {
    Http1(Pooled<K, Http1Connection<IO>>),
    Http2(Http2Connection),
}

impl<K: Key, IO: AsyncWriteRent + AsyncReadRent> Poolable for HttpConnection<K, IO> {
    #[inline]
    fn is_open(&self) -> bool {
        match self {
            Self::Http1(conn) => conn.is_open(),
            Self::Http2(conn) => conn.is_open(),
        }
    }
}

impl<K: Key, IO: AsyncReadRent + AsyncWriteRent> From<Pooled<K, Http1Connection<IO>>>
    for HttpConnection<K, IO>
{
    fn from(pooled_conn: Pooled<K, Http1Connection<IO>>) -> Self {
        Self::Http1(pooled_conn)
    }
}

impl<K: Key, IO: AsyncReadRent + AsyncWriteRent> From<Http2Connection> for HttpConnection<K, IO> {
    fn from(conn: Http2Connection) -> Self {
        Self::Http2(conn)
    }
}

impl<K: Key, IO: AsyncReadRent + AsyncWriteRent> HttpConnection<K, IO> {
    /// Sends an HTTP request using the appropriate protocol (HTTP/1.1 or HTTP/2).
    ///
    /// This method automatically handles the differences between HTTP/1.1 and HTTP/2,
    /// providing a unified interface for sending requests.
    ///
    /// # Arguments
    ///
    /// * `request` - The HTTP request to send.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - `Result<Response<HttpBody>, HttpError>`: The HTTP response or an error.
    /// - `bool`: Indicates whether the connection can be reused (true) or should be closed (false).
    ///
    /// # Type Parameters
    ///
    /// * `R`: The request type, which must be convertible into parts with a `RequestHead`.
    /// * `E`: The error type for the `ClientCodec`, which must be convertible into `HttpError`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use crate::{HttpConnection, Request, Response, HttpBody, HttpError};
    /// # async fn example<K: Key, IO: AsyncReadRent + AsyncWriteRent>(
    /// #     mut conn: HttpConnection<K, IO>,
    /// #     request: Request<Vec<u8>>
    /// # ) -> Result<(), HttpError> {
    /// let (response, can_reuse) = conn.send_request(request).await;
    /// let response: Response<HttpBody> = response?;
    ///  Ok(())
    /// }
    /// ```
    pub async fn send_request<R, E>(
        &mut self,
        request: R,
    ) -> (Result<Response<HttpBody>, HttpError>, bool)
    where
        ClientCodec<IO>: Sink<R, Error = E>,
        E: std::fmt::Debug + Into<HttpError>,
        R: IntoParts<Parts = RequestHead>,
        R::Body: Body<Data = Bytes, Error = HttpError> + Into<HttpBody>,
    {
        // We'll extract the parts to check the method below
        
        // For now, let's extract the parts to check the method
        let (parts, body) = request.into_parts();
        let is_head = parts.method == http::Method::HEAD;
        
        // Set the thread-local flag for HTTP/1.1 to use
        set_head_request(is_head);
        

        
        // Reconstruct and send
        let request = monoio_http::common::request::Request::from_parts(parts, body);
        
        match self {
            Self::Http1(conn) => {
                // For HTTP/1, convert body to HttpBody then extract H1 Payload
                let (parts, body) = request.into_parts();
                let http_body: HttpBody = body.into();
                let h1_payload = match http_body {
                    monoio_http::common::body::HttpBody::H1(payload) => payload,
                    monoio_http::common::body::HttpBody::H2(_h2_body) => {
                        // Convert H2 body to H1 payload - use None for simplicity
                        monoio_http::h1::payload::Payload::None
                    },
                    monoio_http::common::body::HttpBody::Ready(_bytes_opt) => {
                        // Convert Ready bytes to Fixed payload - for simplicity, use None
                        // TODO: Properly convert bytes to fixed payload
                        monoio_http::h1::payload::Payload::None
                    },
                };
                let h1_request = monoio_http::common::request::Request::from_parts(parts, h1_payload);
                conn.send_reconstructed_request(h1_request).await
            },
            Self::Http2(conn) => conn.send_request(request).await,
        }
    }
}

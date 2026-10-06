//! WebSocket server for streaming RDP desktop and handling input.
//!
//! Provides a WebSocket interface matching the agent-browser protocol for
//! debugging and interactive viewing of the remote desktop.
//!
//! Also serves the embedded viewer HTML on regular HTTP requests.

use std::collections::HashSet;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info};

use crate::rdp_session::{PointerShape, RdpSession};
use crate::ws_input::{keyboard_to_fastpath, mouse_to_fastpath, ClipboardContent, WsInputMessage};

/// Embedded viewer HTML.
const VIEWER_HTML: &str = include_str!("../../../assets/viewer/viewer.html");

/// Frame message sent to clients.
#[derive(Debug, Serialize)]
struct FrameMessage {
    #[serde(rename = "type")]
    msg_type: &'static str,
    data: String,
    metadata: FrameMetadata,
}

/// Metadata included with frame messages.
#[derive(Debug, Serialize)]
struct FrameMetadata {
    #[serde(rename = "deviceWidth")]
    device_width: u16,
    #[serde(rename = "deviceHeight")]
    device_height: u16,
}

/// Status message sent to clients.
#[derive(Debug, Serialize)]
struct StatusMessage {
    #[serde(rename = "type")]
    msg_type: &'static str,
    connected: bool,
    streaming: bool,
    #[serde(rename = "viewportWidth")]
    viewport_width: u16,
    #[serde(rename = "viewportHeight")]
    viewport_height: u16,
}

/// Agent cursor position (server → client), drawn as an overlay in the viewer.
#[derive(Debug, Serialize)]
struct CursorMessage {
    #[serde(rename = "type")]
    msg_type: &'static str,
    x: u16,
    y: u16,
    action: &'static str,
}

/// Remote pointer shape (server → client), used as the viewer's mouse cursor.
#[derive(Debug, Serialize)]
struct PointerMessage {
    #[serde(rename = "type")]
    msg_type: &'static str,
    /// "default", "hidden" or "bitmap".
    shape: &'static str,
    /// Base64 PNG for "bitmap".
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    #[serde(rename = "hotspotX")]
    hotspot_x: u16,
    #[serde(rename = "hotspotY")]
    hotspot_y: u16,
}

impl PointerMessage {
    fn shape(shape: &'static str) -> Self {
        Self { msg_type: "pointer", shape, data: None, hotspot_x: 0, hotspot_y: 0 }
    }
}

/// Clipboard changed notification (server → client).
#[derive(Debug, Serialize)]
struct ClipboardChangedMessage {
    #[serde(rename = "type")]
    msg_type: &'static str,
}

/// Clipboard data message (server → client).
#[derive(Debug, Serialize)]
struct ClipboardDataMessage {
    #[serde(rename = "type")]
    msg_type: &'static str,
    content: ClipboardContent,
}

/// Client ID type.
type ClientId = u64;

/// WebSocket server for desktop streaming.
pub struct WsServer {
    bind: String,
    token: Option<String>,
    port: u16,
    jpeg_quality: u8,
    serve_viewer: bool,
    /// Active clients (by ID).
    clients: Arc<Mutex<HashSet<ClientId>>>,
    /// Next client ID.
    next_client_id: Arc<Mutex<ClientId>>,
}

/// Configuration for the WebSocket server.
pub struct WsServerConfig {
    /// Address to bind to. Defaults to loopback.
    pub bind: String,
    /// Access token; required on every request when set.
    pub token: Option<String>,
    pub port: u16,
    pub fps: u32,
    pub jpeg_quality: u8,
    /// Serve the embedded HTML viewer on HTTP requests.
    pub serve_viewer: bool,
}

impl Default for WsServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            token: None,
            port: 9224,
            fps: 10,
            jpeg_quality: 80,
            serve_viewer: false,
        }
    }
}

impl WsServer {
    /// Create a new WebSocket server.
    pub fn new(config: WsServerConfig) -> Self {
        Self {
            bind: config.bind,
            token: config.token,
            port: config.port,
            jpeg_quality: config.jpeg_quality,
            serve_viewer: config.serve_viewer,
            clients: Arc::new(Mutex::new(HashSet::new())),
            next_client_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Start the WebSocket server.
    ///
    /// Returns a handle that can be used to broadcast frames to clients.
    pub async fn start(
        &self,
        rdp_session: Arc<tokio::sync::Mutex<Option<RdpSession>>>,
    ) -> anyhow::Result<WsServerHandle> {
        // Loopback by default: the viewer accepts mouse and keyboard input for the session.
        let ip: std::net::IpAddr = self.bind.parse()?;
        if ip.is_unspecified() {
            anyhow::bail!("refusing to bind the streaming server to all interfaces ({})", ip);
        }
        if !ip.is_loopback() && self.token.is_none() {
            anyhow::bail!("a non-loopback streaming bind ({}) requires a stream token", ip);
        }
        let addr = std::net::SocketAddr::new(ip, self.port);
        let listener = TcpListener::bind(addr).await?;
        info!("WebSocket server listening on ws://{}", addr);

        // Create broadcast channel
        let (broadcast_tx, _) = tokio::sync::broadcast::channel::<String>(16);
        let broadcast_tx_clone = broadcast_tx.clone();

        // Spawn accept loop
        let clients = Arc::clone(&self.clients);
        let next_client_id = Arc::clone(&self.next_client_id);
        let jpeg_quality = self.jpeg_quality;
        let serve_viewer = self.serve_viewer;
        let token = self.token.clone();

        let port = self.port;
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!("Connection from {}", addr);

                        let client_id = {
                            let mut id = next_client_id.lock();
                            *id += 1;
                            *id
                        };

                        let clients = Arc::clone(&clients);
                        let rdp_session = Arc::clone(&rdp_session);
                        let broadcast_rx = broadcast_tx.subscribe();
                        let jpeg_quality = jpeg_quality;
                        let serve_viewer = serve_viewer;
                        let token = token.clone();

                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(
                                stream,
                                token.as_deref(),
                                client_id,
                                clients,
                                rdp_session,
                                broadcast_rx,
                                jpeg_quality,
                                port,
                                serve_viewer,
                            )
                            .await
                            {
                                debug!("Client {} disconnected: {}", client_id, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
                }
            }
        });

        Ok(WsServerHandle {
            broadcast_tx: broadcast_tx_clone,
            clients: Arc::clone(&self.clients),
            jpeg_quality: self.jpeg_quality,
            pointer_sent: Mutex::new(None),
        })
    }
}

/// Handle for broadcasting frames to WebSocket clients.
pub struct WsServerHandle {
    broadcast_tx: tokio::sync::broadcast::Sender<String>,
    clients: Arc<Mutex<HashSet<ClientId>>>,
    jpeg_quality: u8,
    /// Pointer version and newest client that last received the pointer shape.
    pointer_sent: Mutex<Option<(u64, ClientId)>>,
}

impl WsServerHandle {
    /// Send the remote pointer shape when it changed or a new client joined.
    pub fn broadcast_pointer(&self, version: u64, shape: &PointerShape) {
        let Some(newest_client) = self.clients.lock().iter().max().copied() else {
            return;
        };
        {
            let mut sent = self.pointer_sent.lock();
            if *sent == Some((version, newest_client)) {
                return;
            }
            *sent = Some((version, newest_client));
        }

        let msg = match shape {
            PointerShape::Default => PointerMessage::shape("default"),
            PointerShape::Hidden => PointerMessage::shape("hidden"),
            PointerShape::Bitmap(pointer) => match encode_png(pointer.width, pointer.height, &pointer.bitmap_data) {
                Ok(png) => PointerMessage {
                    msg_type: "pointer",
                    shape: "bitmap",
                    data: Some(base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &png)),
                    hotspot_x: pointer.hotspot_x,
                    hotspot_y: pointer.hotspot_y,
                },
                Err(e) => {
                    error!("Failed to encode pointer: {}", e);
                    return;
                }
            },
        };

        if let Ok(json) = serde_json::to_string(&msg) {
            let _ = self.broadcast_tx.send(json);
        }
    }

    /// Check if there are any connected clients.
    pub fn has_clients(&self) -> bool {
        !self.clients.lock().is_empty()
    }

    /// Broadcast a frame to all connected clients.
    ///
    /// Takes the raw RGBA image data and converts it to JPEG.
    pub fn broadcast_frame(&self, width: u16, height: u16, rgba_data: &[u8]) {
        if !self.has_clients() {
            return;
        }

        // Convert RGBA to JPEG
        let jpeg_data = match encode_jpeg(width, height, rgba_data, self.jpeg_quality) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to encode JPEG: {}", e);
                return;
            }
        };

        // Base64 encode
        let base64_data = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &jpeg_data,
        );

        // Create frame message
        let msg = FrameMessage {
            msg_type: "frame",
            data: base64_data,
            metadata: FrameMetadata {
                device_width: width,
                device_height: height,
            },
        };

        if let Ok(json) = serde_json::to_string(&msg) {
            let _ = self.broadcast_tx.send(json);
        }
    }

    /// Show the agent's cursor in viewers. Viewer-only: nothing is drawn on the remote desktop.
    pub fn broadcast_cursor(&self, x: u16, y: u16, action: &'static str) {
        if !self.has_clients() {
            return;
        }

        let msg = CursorMessage {
            msg_type: "cursor",
            x,
            y,
            action,
        };

        if let Ok(json) = serde_json::to_string(&msg) {
            let _ = self.broadcast_tx.send(json);
        }
    }

    /// Notify clients that the remote clipboard has changed.
    pub fn broadcast_clipboard_changed(&self) {
        if !self.has_clients() {
            return;
        }

        let msg = ClipboardChangedMessage {
            msg_type: "clipboard_changed",
        };

        if let Ok(json) = serde_json::to_string(&msg) {
            debug!("Broadcasting clipboard_changed to clients");
            let _ = self.broadcast_tx.send(json);
        }
    }
}

/// Handle an incoming connection - either HTTP or WebSocket.
async fn handle_connection(
    stream: TcpStream,
    token: Option<&str>,
    client_id: ClientId,
    clients: Arc<Mutex<HashSet<ClientId>>>,
    rdp_session: Arc<tokio::sync::Mutex<Option<RdpSession>>>,
    broadcast_rx: tokio::sync::broadcast::Receiver<String>,
    jpeg_quality: u8,
    ws_port: u16,
    serve_viewer: bool,
) -> anyhow::Result<()> {
    // Peek at the request headers without consuming them
    let mut peek_buf = [0u8; 2048];
    let n = stream.peek(&mut peek_buf).await?;
    let request_preview = String::from_utf8_lossy(&peek_buf[..n]);

    if !request_allowed(&request_preview, token) {
        return serve_forbidden(stream).await;
    }

    // Check if this is a WebSocket upgrade request
    let is_websocket = request_preview.to_lowercase().contains("upgrade: websocket");

    if is_websocket {
        // Handle as WebSocket
        let ws_stream = tokio_tungstenite::accept_async(stream).await?;
        handle_websocket_client(ws_stream, client_id, clients, rdp_session, broadcast_rx, jpeg_quality).await
    } else if serve_viewer {
        // Serve the viewer HTML (consume the request first)
        serve_viewer_html(stream, ws_port).await
    } else {
        // Return 404 - viewer not enabled
        serve_not_found(stream).await
    }
}

/// Check the request's Origin (blocks cross-site WebSocket hijacking from web pages)
/// and, when the server has a token, the `token` query parameter.
fn request_allowed(request: &str, token: Option<&str>) -> bool {
    let mut lines = request.lines();
    let target = lines.next().and_then(|l| l.split_whitespace().nth(1)).unwrap_or("");

    let mut host = None;
    let mut origin = None;
    for line in lines.take_while(|l| !l.is_empty()) {
        if let Some((name, value)) = line.split_once(':') {
            match name.trim().to_ascii_lowercase().as_str() {
                "host" => host = Some(value.trim()),
                "origin" => origin = Some(value.trim()),
                _ => {}
            }
        }
    }
    if let Some(origin) = origin {
        let origin_host = origin.split_once("://").map(|(_, rest)| rest).unwrap_or(origin);
        if host.is_none_or(|h| !h.eq_ignore_ascii_case(origin_host)) {
            return false;
        }
    }

    let Some(expected) = token else { return true };
    let given = target
        .split_once('?')
        .map(|(_, query)| query)
        .unwrap_or("")
        .split('&')
        .find_map(|pair| pair.strip_prefix("token="))
        .unwrap_or("");
    // Constant-time comparison.
    given.len() == expected.len()
        && given.bytes().zip(expected.bytes()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

/// Serve a 403 response.
async fn serve_forbidden(mut stream: TcpStream) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;

    let mut buf = [0u8; 4096];
    let _ = stream.read(&mut buf).await;
    stream
        .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await?;
    stream.flush().await?;
    Ok(())
}

/// Serve a 404 response.
async fn serve_not_found(mut stream: TcpStream) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;

    // Consume the HTTP request
    let mut buf = [0u8; 4096];
    let _ = stream.read(&mut buf).await;

    let response = "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 25\r\nConnection: close\r\n\r\nViewer is not enabled.\r\n";
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// Serve the embedded viewer HTML.
async fn serve_viewer_html(mut stream: TcpStream, ws_port: u16) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;

    // Consume the HTTP request (we already peeked at it)
    let mut buf = [0u8; 4096];
    let _ = stream.read(&mut buf).await;

    // Inject the WebSocket URL into the HTML
    let html = VIEWER_HTML.replace(
        "value=\"ws://localhost:9224\"",
        &format!("value=\"ws://localhost:{}\"", ws_port),
    );

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        html.len(),
        html
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// Handle a single WebSocket client connection.
async fn handle_websocket_client<S>(
    ws_stream: S,
    client_id: ClientId,
    clients: Arc<Mutex<HashSet<ClientId>>>,
    rdp_session: Arc<tokio::sync::Mutex<Option<RdpSession>>>,
    mut broadcast_rx: tokio::sync::broadcast::Receiver<String>,
    jpeg_quality: u8,
) -> anyhow::Result<()>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin,
{
    let (mut ws_sink, mut ws_stream) = ws_stream.split();

    // Register client
    {
        clients.lock().insert(client_id);
    }
    info!("Client {} connected (total: {})", client_id, clients.lock().len());

    // Send initial status
    {
        let session = rdp_session.lock().await;
        let (connected, width, height) = if let Some(ref rdp) = *session {
            (true, rdp.width(), rdp.height())
        } else {
            (false, 0, 0)
        };

        let status = StatusMessage {
            msg_type: "status",
            connected,
            streaming: true,
            viewport_width: width,
            viewport_height: height,
        };

        if let Ok(json) = serde_json::to_string(&status) {
            let _ = ws_sink.send(Message::Text(json.into())).await;
        }
    }

    // Send initial frame
    {
        let session = rdp_session.lock().await;
        if let Some(ref rdp) = *session {
            let (width, height, data) = rdp.get_image_data();
            if let Ok(jpeg_data) = encode_jpeg(width, height, &data, jpeg_quality) {
                let base64_data = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &jpeg_data,
                );
                let msg = FrameMessage {
                    msg_type: "frame",
                    data: base64_data,
                    metadata: FrameMetadata {
                        device_width: width,
                        device_height: height,
                    },
                };
                if let Ok(json) = serde_json::to_string(&msg) {
                    let _ = ws_sink.send(Message::Text(json.into())).await;
                }
            }
        }
    }

    loop {
        tokio::select! {
            // Receive broadcast frames
            result = broadcast_rx.recv() => {
                match result {
                    Ok(json) => {
                        if let Err(e) = ws_sink.send(Message::Text(json.into())).await {
                            debug!("Failed to send frame to client {}: {}", client_id, e);
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        debug!("Client {} lagged {} frames", client_id, n);
                    }
                    Err(_) => break,
                }
            }

            // Receive client messages
            result = ws_stream.next() => {
                match result {
                    Some(Ok(msg)) => {
                        if let Message::Text(text) = msg {
                            handle_client_message(&text, &rdp_session, &mut ws_sink).await;
                        } else if let Message::Close(_) = msg {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        debug!("WebSocket error for client {}: {}", client_id, e);
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    // Unregister client
    {
        clients.lock().remove(&client_id);
    }
    info!("Client {} disconnected (total: {})", client_id, clients.lock().len());

    Ok(())
}

/// Handle an incoming message from a WebSocket client.
async fn handle_client_message<S>(
    text: &str,
    rdp_session: &Arc<tokio::sync::Mutex<Option<RdpSession>>>,
    ws_sink: &mut S,
) where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Debug,
{
    // Parse the input message
    let input: WsInputMessage = match serde_json::from_str(text) {
        Ok(msg) => msg,
        Err(e) => {
            debug!("Failed to parse WebSocket message: {} - {}", e, text);
            return;
        }
    };

    match input {
        WsInputMessage::Mouse(payload) => {
            let events = mouse_to_fastpath(&payload);
            if !events.is_empty() {
                let session = rdp_session.lock().await;
                if let Some(ref rdp) = *session {
                    if let Err(e) = rdp.send_input(events).await {
                        error!("Failed to send input to RDP session: {}", e);
                    }
                }
            }
        }
        WsInputMessage::Keyboard(payload) => {
            let events = keyboard_to_fastpath(&payload);
            if !events.is_empty() {
                let session = rdp_session.lock().await;
                if let Some(ref rdp) = *session {
                    if let Err(e) = rdp.send_input(events).await {
                        error!("Failed to send input to RDP session: {}", e);
                    }
                }
            }
        }
        WsInputMessage::ClipboardGet(_payload) => {
            // Client is requesting remote clipboard content
            debug!("Received clipboard_get request from client");
            let session = rdp_session.lock().await;
            if let Some(ref rdp) = *session {
                match rdp.clipboard_get().await {
                    Ok(text) => {
                        let msg = ClipboardDataMessage {
                            msg_type: "clipboard_data",
                            content: ClipboardContent {
                                content_type: "text".to_string(),
                                text,
                            },
                        };
                        if let Ok(json) = serde_json::to_string(&msg) {
                            let _ = ws_sink.send(Message::Text(json.into())).await;
                        }
                    }
                    Err(e) => {
                        debug!("Failed to get clipboard: {}", e);
                    }
                }
            }
        }
        WsInputMessage::ClipboardSet(payload) => {
            // Client is setting clipboard (before paste)
            debug!("Received clipboard_set from client: {} chars", payload.text.len());
            let session = rdp_session.lock().await;
            if let Some(ref rdp) = *session {
                if let Err(e) = rdp.clipboard_set(payload.text).await {
                    debug!("Failed to set clipboard: {}", e);
                }
            }
        }
    }
}

/// Encode RGBA image data to JPEG.
fn encode_jpeg(width: u16, height: u16, rgba_data: &[u8], quality: u8) -> anyhow::Result<Vec<u8>> {
    use image::{ImageBuffer, Rgba};

    // Create image buffer from RGBA data
    let img: ImageBuffer<Rgba<u8>, _> = ImageBuffer::from_raw(
        width as u32,
        height as u32,
        rgba_data.to_vec(),
    )
    .ok_or_else(|| anyhow::anyhow!("Failed to create image buffer"))?;

    // Convert to RGB (JPEG doesn't support alpha)
    let rgb_img = image::DynamicImage::ImageRgba8(img).into_rgb8();

    // Encode to JPEG
    let mut jpeg_data = Vec::new();
    let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg_data, quality);
    rgb_img.write_with_encoder(encoder)?;

    Ok(jpeg_data)
}

/// Encode an RGBA bitmap as PNG.
fn encode_png(width: u16, height: u16, rgba_data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let img = image::RgbaImage::from_raw(width as u32, height as u32, rgba_data.to_vec())
        .ok_or_else(|| anyhow::anyhow!("invalid pointer bitmap size"))?;
    let mut png = Vec::new();
    img.write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)?;
    Ok(png)
}

/// Get the stream port from environment or default.
pub fn get_stream_port() -> u16 {
    std::env::var("AGENT_RDP_STREAM_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Get the stream FPS from environment or default.
pub fn get_stream_fps() -> u32 {
    std::env::var("AGENT_RDP_STREAM_FPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10)
}

/// Get the stream JPEG quality from environment or default.
pub fn get_stream_quality() -> u8 {
    std::env::var("AGENT_RDP_STREAM_QUALITY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(80)
}

#[cfg(test)]
mod tests {
    use super::request_allowed;

    const PLAIN: &str = "GET / HTTP/1.1\r\nHost: 100.1.2.3:9224\r\n\r\n";

    #[test]
    fn allows_requests_without_token_when_none_is_set() {
        assert!(request_allowed(PLAIN, None));
    }

    #[test]
    fn requires_matching_token_when_set() {
        assert!(!request_allowed(PLAIN, Some("secret")));
        assert!(!request_allowed("GET /?token=wrong HTTP/1.1\r\nHost: h\r\n\r\n", Some("secret")));
        assert!(request_allowed("GET /?token=secret HTTP/1.1\r\nHost: h\r\n\r\n", Some("secret")));
    }

    #[test]
    fn rejects_cross_origin_requests() {
        let cross = "GET / HTTP/1.1\r\nHost: localhost:9224\r\nOrigin: https://evil.example\r\n\r\n";
        let same = "GET / HTTP/1.1\r\nHost: localhost:9224\r\nOrigin: http://localhost:9224\r\n\r\n";
        assert!(!request_allowed(cross, None));
        assert!(request_allowed(same, None));
    }
}

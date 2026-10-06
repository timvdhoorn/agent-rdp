//! View command implementation - opens the web viewer served by the daemon.

use std::net::IpAddr;

use crate::cli::ViewArgs;
use crate::output::Output;

pub async fn run(args: ViewArgs, stream_bind: Option<&str>, output: &Output) -> anyhow::Result<()> {
    // The daemon serves the viewer HTML on the same port as the WebSocket server
    let bind = stream_bind.map(resolve_bind).transpose()?;
    let url = viewer_url(bind.as_deref(), args.port);

    if output.is_json() {
        println!(r#"{{"url":"{}"}}"#, url);
    } else {
        println!("Opening viewer at: {}", url);
        if bind.as_deref().is_some_and(|b| !is_loopback(b)) {
            println!("Non-loopback viewers need the URL with ?token= printed by connect.");
        }
    }

    // Open browser
    if let Err(e) = open::that(&url) {
        output.print_error("open_failed", &format!("Failed to open browser: {}", e));
        std::process::exit(1);
    }

    Ok(())
}

/// Resolve a stream bind address: an IP address, or "tailscale" for this machine's Tailscale IPv4.
pub fn resolve_bind(bind: &str) -> anyhow::Result<String> {
    if bind.eq_ignore_ascii_case("tailscale") {
        let out = std::process::Command::new("tailscale")
            .args(["ip", "-4"])
            .output()
            .map_err(|e| anyhow::anyhow!("failed to run `tailscale ip -4`: {}", e))?;
        let ip = String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        if !out.status.success() || ip.parse::<IpAddr>().is_err() {
            anyhow::bail!("no Tailscale IPv4 address found; is Tailscale running?");
        }
        return Ok(ip);
    }
    let ip = bind
        .parse::<IpAddr>()
        .map_err(|_| anyhow::anyhow!("invalid --stream-bind '{}': expected an IP address or 'tailscale'", bind))?;
    if ip.is_unspecified() {
        anyhow::bail!("--stream-bind {} would expose session control on all interfaces; use a specific address", ip);
    }
    Ok(bind.to_string())
}

/// Whether a bind address is loopback (no token needed).
pub fn is_loopback(bind: &str) -> bool {
    bind.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// Viewer URL for a bind address; loopback and wildcard binds open on localhost.
pub fn viewer_url(bind: Option<&str>, port: u16) -> String {
    let host = match bind.and_then(|b| b.parse::<IpAddr>().ok()) {
        Some(ip) if !ip.is_loopback() && !ip.is_unspecified() => match ip {
            IpAddr::V6(_) => format!("[{}]", ip),
            IpAddr::V4(_) => ip.to_string(),
        },
        _ => "localhost".to_string(),
    };
    format!("http://{}:{}", host, port)
}

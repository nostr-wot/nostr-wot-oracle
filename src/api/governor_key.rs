//! Rate-limit key extraction using this crate's `axum` dependency.
//!
//! `tower_governor::SmartIpKeyExtractor` resolves `ConnectInfo<SocketAddr>` via its own optional
//! `axum` dependency. If Cargo resolves two `axum` versions, extension `TypeId`s differ and every
//! request fails with `GovernorError::UnableToExtractKey` (HTTP 500, body "Unable To Extract Key!").

use axum::extract::ConnectInfo;
use axum::http::{header::FORWARDED, HeaderMap, Request};
use forwarded_header_value::{ForwardedHeaderValue, Identifier};
use std::net::{IpAddr, SocketAddr};
use tower_governor::key_extractor::KeyExtractor;
use tower_governor::GovernorError;

const X_REAL_IP: &str = "x-real-ip";
const X_FORWARDED_FOR: &str = "x-forwarded-for";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OracleSmartIpKeyExtractor;

impl KeyExtractor for OracleSmartIpKeyExtractor {
    type Key = IpAddr;

    fn extract<T>(&self, req: &Request<T>) -> Result<Self::Key, GovernorError> {
        let headers = req.headers();
        maybe_x_forwarded_for(headers)
            .or_else(|| maybe_x_real_ip(headers))
            .or_else(|| maybe_forwarded(headers))
            .or_else(|| {
                req.extensions()
                    .get::<ConnectInfo<SocketAddr>>()
                    .map(|ci| ci.ip())
            })
            .ok_or(GovernorError::UnableToExtractKey)
    }
}

fn maybe_x_forwarded_for(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get(X_FORWARDED_FOR)
        .and_then(|hv| hv.to_str().ok())
        .and_then(|s| s.split(',').find_map(|s| s.trim().parse::<IpAddr>().ok()))
}

fn maybe_x_real_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get(X_REAL_IP)
        .and_then(|hv| hv.to_str().ok())
        .and_then(|s| s.parse::<IpAddr>().ok())
}

fn maybe_forwarded(headers: &HeaderMap) -> Option<IpAddr> {
    headers.get_all(FORWARDED).iter().find_map(|hv| {
        hv.to_str()
            .ok()
            .and_then(|s| ForwardedHeaderValue::from_forwarded(s).ok())
            .and_then(|f| {
                f.iter()
                    .filter_map(|fs| fs.forwarded_for.as_ref())
                    .find_map(|ff| match ff {
                        Identifier::SocketAddr(a) => Some(a.ip()),
                        Identifier::IpAddr(ip) => Some(*ip),
                        _ => None,
                    })
            })
    })
}

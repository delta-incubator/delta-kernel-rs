use std::sync::Arc;
use std::time::Duration;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
use rstest::rstest;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::rustls::crypto::ring::default_provider;
use tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use super::{build_rest_client, RestClientOptions};

#[rstest]
#[case::default_accepts_matching_hostname(None, false, true, false, true)]
#[case::default_rejects_mismatched_hostname(None, true, true, false, false)]
#[case::disabled_bypass_rejects_mismatched_hostname(Some(false), true, true, false, false)]
#[case::enabled_bypass_accepts_mismatched_hostname(Some(true), true, true, false, true)]
#[case::enabled_bypass_rejects_untrusted_certificate(Some(true), true, false, false, false)]
#[case::enabled_bypass_rejects_expired_certificate(Some(true), true, true, true, false)]
#[tokio::test]
async fn hostname_bypass_preserves_certificate_validation(
    #[case] allow_invalid_hostnames: Option<bool>,
    #[case] mismatched_hostname: bool,
    #[case] trusted: bool,
    #[case] expired: bool,
    #[case] succeeds: bool,
) {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Test CA");
    let ca = ca_params.clone().self_signed(&ca_key).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    if expired {
        params.not_after = rcgen::date_time_ymd(2000, 1, 1);
    }
    let cert = params.signed_by(&server_key, &ca, &ca_key).unwrap();
    let config = ServerConfig::builder_with_provider(Arc::new(default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone(), ca.der().clone()],
            PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        // Rejected certificates terminate the handshake before an HTTP request is sent.
        if let Ok(mut stream) = TlsAcceptor::from(Arc::new(config)).accept(stream).await {
            let mut request = [0; 4096];
            if stream.read(&mut request).await.is_ok() {
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .await;
            }
        }
    });

    let dir = tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, server_key.serialize_pem()).unwrap();
    let ca_pem = if trusted {
        ca.pem()
    } else {
        ca_params
            .self_signed(&KeyPair::generate().unwrap())
            .unwrap()
            .pem()
    };
    std::fs::write(&ca_path, ca_pem).unwrap();
    let mut opts = RestClientOptions {
        cert_path: Some(cert_path.to_str().unwrap().into()),
        key_path: Some(key_path.to_str().unwrap().into()),
        ca_path: Some(ca_path.to_str().unwrap().into()),
        timeout_secs: Some(5),
        ..Default::default()
    };
    if let Some(value) = allow_invalid_hostnames {
        opts.allow_invalid_hostnames = value;
    }
    let client = build_rest_client(&opts).unwrap();
    let host = if mismatched_hostname {
        "127.0.0.1"
    } else {
        "localhost"
    };
    let response = client.get(format!("https://{host}:{port}/")).send().await;
    if succeeds {
        assert_eq!(response.unwrap().text().await.unwrap(), "ok");
    } else {
        assert!(response.is_err(), "invalid certificate was accepted");
    }
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap();
}

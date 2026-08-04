use super::{
    BasicConstraints, DistinguishedName, DnType, IsCa, SanType, certificate_material_is_valid,
    certificate_params_for_node, cert_contains_san, ensure_or_generate_tls_cert,
    should_refresh_leaf_cert,
};
use rcgen::{CertificateParams, KeyPair, KeyUsagePurpose};
use std::fs;

#[test]
fn certificate_params_include_dns_subject_alt_names() {
    let params = certificate_params_for_node(
        "server-node-01",
        "public.example:4001",
        &["foo.example".to_string()],
    )
    .expect("should build certificate params");

    let names = params
        .subject_alt_names
        .iter()
        .filter_map(|san| match san {
            SanType::DnsName(name) => Some(name.to_string()),
            SanType::IpAddress(ip) => Some(ip.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert!(names.iter().any(|name| name == "localhost"));
    assert!(names.iter().any(|name| name == "public.example"));
    assert!(names.iter().any(|name| name == "foo.example"));
}

#[test]
fn certificate_params_include_loopback_and_advertised_names() {
    let params = certificate_params_for_node(
        "server-node-01",
        "127.0.0.1:4001",
        &[],
    )
    .expect("should build certificate params");

    let names = params
        .subject_alt_names
        .iter()
        .filter_map(|san| match san {
            SanType::DnsName(name) => Some(name.to_string()),
            SanType::IpAddress(ip) => Some(ip.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert!(names.iter().any(|name| name == "localhost"));
    assert!(names.iter().any(|name| name == "127.0.0.1"));
}

#[test]
fn certificate_params_ignores_placeholder_hosts_in_subject_alt_names() {
    let params = certificate_params_for_node(
        "server-node-01",
        "--",
        &["*".to_string()],
    )
    .expect("should build certificate params");

    let names = params
        .subject_alt_names
        .iter()
        .filter_map(|san| match san {
            SanType::DnsName(name) => Some(name.to_string()),
            SanType::IpAddress(ip) => Some(ip.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert!(names.iter().any(|name| name == "localhost"));
    assert!(!names.iter().any(|name| name == "--"));
    assert!(!names.iter().any(|name| name == "*"));
}

#[test]
fn should_refresh_leaf_cert_when_expected_san_is_missing() {
    let dir = std::path::Path::new("../../target/test-data");
    let _ = fs::create_dir_all(dir);

    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.distinguished_name = DistinguishedName::new();
    params.distinguished_name.push(DnType::CommonName, "test-node");

    let key_pair = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    let cert_pem = cert.pem();
    let cert_path = dir.join("leaf-cert.pem");
    fs::write(&cert_path, &cert_pem).unwrap();

    assert!(cert_contains_san(&cert_pem, "localhost"));
    assert!(!cert_contains_san(&cert_pem, "public.example.com"));
    assert!(should_refresh_leaf_cert(&cert_path, "public.example.com:4001", &[]));
}

#[test]
fn certificate_material_is_valid_rejects_unrelated_leaf_certificate() {
    let dir = std::path::Path::new("../../target/test-data-invalid");
    let _ = fs::create_dir_all(dir);

    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params.distinguished_name.push(DnType::CommonName, "test-ca");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let mut leaf_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    leaf_params.distinguished_name = DistinguishedName::new();
    leaf_params.distinguished_name.push(DnType::CommonName, "test-leaf");
    let leaf_cert = leaf_params.self_signed(&leaf_key).unwrap();

    let cert_path = dir.join("leaf-cert.pem");
    let key_path = dir.join("leaf-key.pem");
    let ca_cert_path = dir.join("ca-cert.pem");
    let ca_key_path = dir.join("ca-key.pem");

    fs::write(&cert_path, leaf_cert.pem()).unwrap();
    fs::write(&key_path, leaf_key.serialize_pem()).unwrap();
    fs::write(&ca_cert_path, ca_cert.pem()).unwrap();
    fs::write(&ca_key_path, ca_key.serialize_pem()).unwrap();

    assert!(!certificate_material_is_valid(
        &cert_path,
        &key_path,
        &ca_cert_path,
        &ca_key_path,
    ));
}

#[test]
fn generated_tls_material_includes_rustls_compatible_key_usage_extensions() {
    let dir = std::path::Path::new("../../target/test-data-key-usage");
    let _ = fs::remove_dir_all(dir);
    let _ = fs::create_dir_all(dir);

    let result = ensure_or_generate_tls_cert(
        dir,
        "server-node-01",
        "public.example:4001",
        &[],
    )
    .expect("should generate tls material");

    let leaf_pem = fs::read_to_string(&result.cert_path).unwrap();
    let ca_pem = fs::read_to_string(&result.ca_path).unwrap();

    assert!(leaf_pem.contains("BEGIN CERTIFICATE"));
    assert!(ca_pem.contains("BEGIN CERTIFICATE"));
    assert!(certificate_material_is_valid(
        &result.cert_path,
        &result.key_path,
        &result.ca_path,
        &result.key_path.with_file_name("ca-key.pem"),
    ));
}

#[test]
fn ensure_or_generate_tls_cert_rewrites_ca_material_when_existing_ca_is_inconsistent() {
    let dir = std::path::Path::new("../../target/test-data-ca-rebuild");
    let _ = fs::remove_dir_all(dir);
    let _ = fs::create_dir_all(dir);

    let bad_ca_key = rcgen::KeyPair::generate().unwrap();
    let good_ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params.distinguished_name.push(DnType::CommonName, "test-ca");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    let bad_ca_cert = ca_params.self_signed(&bad_ca_key).unwrap();
    let stale_ca_pem = bad_ca_cert.pem();
    let cert_path = dir.join("server-node-01-cert.pem");
    let key_path = dir.join("server-node-01-key.pem");
    let ca_cert_path = dir.join("ca-cert.pem");
    let ca_key_path = dir.join("ca-key.pem");

    fs::write(&ca_cert_path, &stale_ca_pem).unwrap();
    fs::write(&ca_key_path, good_ca_key.serialize_pem()).unwrap();
    fs::write(&cert_path, "placeholder").unwrap();
    fs::write(&key_path, "placeholder").unwrap();

    let result = ensure_or_generate_tls_cert(
        dir,
        "server-node-01",
        "public.example.com:4001",
        &[],
    )
    .expect("should rebuild tls material");

    let ca_pem = fs::read_to_string(&ca_cert_path).unwrap();
    let leaf_pem = fs::read_to_string(&result.cert_path).unwrap();
    assert!(certificate_material_is_valid(
        &result.cert_path,
        &result.key_path,
        &result.ca_path,
        &ca_key_path,
    ));
    assert!(ca_pem.contains("BEGIN CERTIFICATE"));
    assert!(leaf_pem.contains("BEGIN CERTIFICATE"));
    assert!(certificate_material_is_valid(
        &result.cert_path,
        &result.key_path,
        &result.ca_path,
        &ca_key_path,
    ));
}

#[test]
fn ensure_or_generate_tls_cert_persists_ca_fingerprint_file() {
    let cluster_dir = std::path::Path::new("../../target/test-data-ca-fingerprint");
    let node_dir = cluster_dir.join("node-1");
    let _ = fs::remove_dir_all(cluster_dir);
    let _ = fs::create_dir_all(&node_dir);

    let _result = ensure_or_generate_tls_cert(
        &node_dir,
        "server-node-01",
        "public.example.com:4001",
        &[],
    )
    .expect("should generate tls material");

    let fingerprint_path = cluster_dir.join("p2p-tls").join("ca-fingerprint.sha256");
    let raw = fs::read_to_string(&fingerprint_path)
        .expect("ca fingerprint file should exist");
    let fingerprint = raw.trim();

    assert_eq!(fingerprint.len(), 64);
    assert!(fingerprint.chars().all(|ch| ch.is_ascii_hexdigit()));
}
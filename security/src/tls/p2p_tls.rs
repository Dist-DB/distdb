use std::collections::BTreeSet;
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use openssl::{pkey::PKey, x509::X509};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, CertificateSigningRequestParams,
    DistinguishedName, DnType, ExtendedKeyUsagePurpose, Ia5String, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use common::helpers::utils::md5_hash;

#[derive(Debug, Clone)]
pub struct AutoTlsPaths {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct TlsEnrollmentRequestMaterial {
    pub csr_pem: String,
    pub key_pem: String,
}

fn extract_host(address_hint: &str) -> String {

    if let Some((host, _)) = address_hint.rsplit_once(':') {
        return host.trim_matches('[').trim_matches(']').to_string();
    }
    
    address_hint.trim_matches('[').trim_matches(']').to_string()

}

fn cluster_tls_paths(node_data_dir: &Path) -> (PathBuf, PathBuf, PathBuf) {

    let cluster_dir = node_data_dir.parent().unwrap_or(node_data_dir);
    let tls_dir = cluster_dir.join("p2p-tls");
    let ca_cert_path = tls_dir.join("ca-cert.pem");
    let ca_key_path = tls_dir.join("ca-key.pem");
    
    (tls_dir, ca_cert_path, ca_key_path)

}

fn is_placeholder_host(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.is_empty() || trimmed == "--" || trimmed == "*" || trimmed == "null"
}

fn sanitize_subject_alt_names(
    address_hint: &str,
    extra_subject_alt_names: &[String],
) -> BTreeSet<String> {

    let mut san_candidates = BTreeSet::new();
    san_candidates.insert("localhost".to_string());

    let host = extract_host(address_hint);
    if !is_placeholder_host(&host) && !host.is_empty() {
        san_candidates.insert(host);
    }

    for san in extra_subject_alt_names {
        let san = san.trim();
        if !is_placeholder_host(san) && !san.is_empty() {
            san_candidates.insert(san.to_string());
        }
    }

    for candidate in [
        "example.test",
        "*.distdb.com",
        "*.local",
        "*.internal",
        "*.docker.internal",
    ] {
        san_candidates.insert(candidate.to_string());
    }
    
    san_candidates

}

fn tls_certificate_uid(
    address_hint: &str,
    extra_subject_alt_names: &[String],
) -> String {

    let san_list = sanitize_subject_alt_names(address_hint, extra_subject_alt_names)
        .into_iter()
        .collect::<Vec<_>>();

    md5_hash(&san_list.join("|"))

}

fn tls_certificate_uid_from_cert_pem(cert_pem: &str) -> Result<String, String> {

    let cert = X509::from_pem(cert_pem.as_bytes())
        .map_err(|err| format!("failed parsing leaf certificate PEM: {err}"))?;

    let Some(sans) = cert.subject_alt_names() else {
        return Err("leaf certificate is missing subject alternative names".to_string());
    };

    let san_list = sans
        .iter()
        .filter_map(|san| {
            san.dnsname()
                .map(ToOwned::to_owned)
                .or_else(|| {
                    san.ipaddress()
                        .and_then(ip_addr_from_san_bytes)
                        .map(|ip| ip.to_string())
                })
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    if san_list.is_empty() {
        return Err("leaf certificate is missing usable SAN entries".to_string());
    }

    Ok(md5_hash(&san_list.join("|")))

}

fn ip_addr_from_san_bytes(raw: &[u8]) -> Option<IpAddr> {
    match raw.len() {
        4 => Some(IpAddr::from([raw[0], raw[1], raw[2], raw[3]])),
        16 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(raw);
            Some(IpAddr::from(octets))
        }
        _ => None,
    }
}

fn certificate_params_for_node(
    node_id: &str,
    address_hint: &str,
    extra_subject_alt_names: &[String],
) -> Result<CertificateParams, String> {

    let mut leaf_dn = DistinguishedName::new();
    leaf_dn.push(DnType::CommonName, node_id);

    let san_candidates = sanitize_subject_alt_names(address_hint, extra_subject_alt_names);
    let mut params = CertificateParams::new(vec![])
        .map_err(|err| format!("failed building leaf cert params: {err}"))?;
    params.distinguished_name = leaf_dn;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::KeyEncipherment];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

    for san in &san_candidates {
        if let Ok(ip) = san.parse::<IpAddr>() {
            params.subject_alt_names.push(SanType::IpAddress(ip));
        } else {
            let dns_name = Ia5String::try_from(san.as_str())
                .map_err(|err| format!("invalid SAN '{san}': {err}"))?;
            params.subject_alt_names.push(SanType::DnsName(dns_name));
        }
    }

    Ok(params)
    
}

fn cert_contains_san(cert_pem: &str, expected_san: &str) -> bool {
    let cert = match CertificateParams::from_ca_cert_pem(cert_pem) {
        Ok(params) => params,
        Err(_) => return false,
    };

    cert.subject_alt_names.iter().any(|san| match san {
        SanType::DnsName(name) => name.as_ref() == expected_san,
        SanType::IpAddress(ip) => ip.to_string() == expected_san,
        _ => false,
    })
}

fn cert_contains_placeholder_san(cert_pem: &str) -> bool {
    let cert = match CertificateParams::from_ca_cert_pem(cert_pem) {
        Ok(params) => params,
        Err(_) => return true,
    };

    cert.subject_alt_names.iter().any(|san| match san {
        SanType::DnsName(name) => is_placeholder_host(name.as_ref()),
        SanType::IpAddress(ip) => is_placeholder_host(&ip.to_string()),
        _ => false,
    })
}

fn should_refresh_leaf_cert(cert_path: &Path, address_hint: &str, extra_subject_alt_names: &[String]) -> bool {
    
    let Some(existing_cert_pem) = fs::read_to_string(cert_path).ok() else {
        return true;
    };

    if cert_contains_placeholder_san(&existing_cert_pem) {
        return true;
    }

    let expected_sans = sanitize_subject_alt_names(address_hint, extra_subject_alt_names);
    let Some(expected_host) = expected_sans.iter().find(|san| !san.contains("localhost")) else {
        return false;
    };

    !cert_contains_san(&existing_cert_pem, expected_host)

}

fn certificate_material_is_valid(
    cert_path: &Path,
    key_path: &Path,
    ca_cert_path: &Path,
    ca_key_path: &Path,
) -> bool {

    let Ok(cert_pem) = fs::read_to_string(cert_path) else {
        return false;
    };
    let Ok(key_pem) = fs::read_to_string(key_path) else {
        return false;
    };
    let Ok(ca_cert_pem) = fs::read_to_string(ca_cert_path) else {
        return false;
    };
    let Ok(ca_key_pem) = fs::read_to_string(ca_key_path) else {
        return false;
    };

    let Ok(leaf_cert) = X509::from_pem(cert_pem.as_bytes()) else {
        return false;
    };
    let Ok(ca_cert) = X509::from_pem(ca_cert_pem.as_bytes()) else {
        return false;
    };
    let Ok(_leaf_key) = PKey::private_key_from_pem(key_pem.as_bytes()) else {
        return false;
    };
    let Ok(_ca_key) = PKey::private_key_from_pem(ca_key_pem.as_bytes()) else {
        return false;
    };

    let Ok(ca_public_key) = ca_cert.public_key() else {
        return false;
    };

    let leaf_subject_key_id = leaf_cert.subject_key_id().map(|id| id.as_slice().to_vec());
    let ca_subject_key_id = ca_cert.subject_key_id().map(|id| id.as_slice().to_vec());
    let leaf_issuer = leaf_cert.issuer_name().to_der().ok();
    let ca_subject = ca_cert.subject_name().to_der().ok();

    ca_cert.verify(&ca_public_key).is_ok()
        && leaf_cert.verify(&ca_public_key).is_ok()
        && leaf_subject_key_id != ca_subject_key_id
        && leaf_issuer.as_ref() == ca_subject.as_ref()

}

pub fn build_tls_enrollment_request(
    node_id: &str,
    address_hint: &str,
    extra_subject_alt_names: &[String],
) -> Result<TlsEnrollmentRequestMaterial, String> {

    let leaf_params =
        certificate_params_for_node(node_id, address_hint, extra_subject_alt_names)?;

    let leaf_key = KeyPair::generate().map_err(|err| format!("failed generating leaf key: {err}"))?;
    
    let csr = leaf_params
        .serialize_request(&leaf_key)
        .map_err(|err| format!("failed generating CSR: {err}"))?;

    Ok(TlsEnrollmentRequestMaterial {
        csr_pem: csr
            .pem()
            .map_err(|err| format!("failed serializing CSR PEM: {err}"))?,
        key_pem: leaf_key.serialize_pem(),
    })

}

pub fn sign_tls_enrollment_csr(
    node_data_dir: &Path,
    csr_pem: &str,
) -> Result<(String, String), String> {

    let (_, ca_cert_path, ca_key_path) = cluster_tls_paths(node_data_dir);

    if !(ca_cert_path.exists() && ca_key_path.exists()) {
        return Err("local CA material is missing; cannot sign CSR".to_string());
    }

    let (ca_cert, ca_key) = load_existing_ca(&ca_cert_path, &ca_key_path)?;
    let csr = CertificateSigningRequestParams::from_pem(csr_pem)
        .map_err(|err| format!("failed parsing CSR PEM: {err}"))?;

    let signed = csr
        .signed_by(&ca_cert, &ca_key)
        .map_err(|err| format!("failed signing CSR: {err}"))?;

    Ok((signed.pem(), ca_cert.pem()))

}

pub fn install_signed_p2p_tls(
    node_data_dir: &Path,
    _node_id: &str,
    key_pem: &str,
    node_cert_pem: &str,
    ca_cert_pem: &str,
) -> Result<AutoTlsPaths, String> {

    let (tls_dir, ca_cert_path, ca_key_path) = cluster_tls_paths(node_data_dir);
    std::fs::create_dir_all(&tls_dir)
        .map_err(|err| format!("failed to create tls dir '{}': {}", tls_dir.display(), err))?;

    let tls_uid = tls_certificate_uid_from_cert_pem(node_cert_pem)?;
    let cert_path = tls_dir.join(format!("{}-cert.pem", tls_uid));
    let key_path = tls_dir.join(format!("{}-key.pem", tls_uid));

    CertificateParams::from_ca_cert_pem(ca_cert_pem)
        .map_err(|err| format!("received CA cert PEM is invalid: {err}"))?;

    if !ca_cert_path.exists() {
        if ca_key_path.exists() {
            return Err(format!(
                "cannot install CA cert because CA key already exists at '{}'",
                ca_key_path.display()
            ));
        }
        std::fs::write(&ca_cert_path, ca_cert_pem).map_err(|err| {
            format!(
                "failed writing imported CA cert '{}': {}",
                ca_cert_path.display(),
                err
            )
        })?;
    }

    if !cert_path.exists() {
        std::fs::write(&cert_path, node_cert_pem)
            .map_err(|err| format!("failed writing node cert '{}': {}", cert_path.display(), err))?;
    }

    if !key_path.exists() {
        std::fs::write(&key_path, key_pem)
            .map_err(|err| format!("failed writing node key '{}': {}", key_path.display(), err))?;
    }

    Ok(AutoTlsPaths {
        cert_path,
        key_path,
        ca_path: ca_cert_path,
    })

}

fn load_existing_ca(ca_cert_path: &Path, ca_key_path: &Path) -> Result<(Certificate, KeyPair), String> {

    let ca_cert_pem = std::fs::read_to_string(ca_cert_path)
        .map_err(|err| format!("failed reading existing CA cert '{}': {}", ca_cert_path.display(), err))?;

    let ca_key_pem = std::fs::read_to_string(ca_key_path)
        .map_err(|err| format!("failed reading existing CA key '{}': {}", ca_key_path.display(), err))?;

    let ca_key = KeyPair::from_pem(&ca_key_pem)
        .map_err(|err| format!("failed parsing existing CA key: {err}"))?;

    let ca_params = CertificateParams::from_ca_cert_pem(&ca_cert_pem)
        .map_err(|err| format!("failed parsing existing CA cert: {err}"))?;

    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(|err| format!("failed rebuilding existing CA certificate params: {err}"))?;

    Ok((ca_cert, ca_key))

}

fn wait_for_ca_material(ca_cert_path: &Path, ca_key_path: &Path) -> Result<(), String> {

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if ca_cert_path.exists() && ca_key_path.exists() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    Err(format!(
        "timed out waiting for CA material '{}' and '{}'",
        ca_cert_path.display(),
        ca_key_path.display()
    ))

}

pub fn ensure_or_generate_tls_cert(
    node_data_dir: &Path,
    node_id: &str,
    address_hint: &str,
    extra_subject_alt_names: &[String],
) -> Result<AutoTlsPaths, String> {

    let (tls_dir, ca_cert_path, ca_key_path) = cluster_tls_paths(node_data_dir);

    std::fs::create_dir_all(&tls_dir)
        .map_err(|err| format!("failed to create tls dir '{}': {}", tls_dir.display(), err))?;

    let tls_uid = tls_certificate_uid(address_hint, extra_subject_alt_names);
    let cert_path = tls_dir.join(format!("{}-cert.pem", tls_uid));
    let key_path = tls_dir.join(format!("{}-key.pem", tls_uid));

    let have_ca = ca_cert_path.exists() && ca_key_path.exists();
    let have_leaf = cert_path.exists() && key_path.exists();

    let existing_material_is_valid = have_ca
        && have_leaf
        && certificate_material_is_valid(&cert_path, &key_path, &ca_cert_path, &ca_key_path);

    if have_ca && have_leaf && existing_material_is_valid && !should_refresh_leaf_cert(&cert_path, address_hint, extra_subject_alt_names) {
        return Ok(AutoTlsPaths {
            cert_path,
            key_path,
            ca_path: ca_cert_path,
        });
    }

    if have_leaf {
        let _ = std::fs::remove_file(&cert_path);
        let _ = std::fs::remove_file(&key_path);
    }

    let (ca_cert, ca_key) = if have_ca {
        if !certificate_material_is_valid(&cert_path, &key_path, &ca_cert_path, &ca_key_path) {
            let _ = std::fs::remove_file(&ca_cert_path);
            let _ = std::fs::remove_file(&ca_key_path);
        }
        if ca_cert_path.exists() && ca_key_path.exists() {
            load_existing_ca(&ca_cert_path, &ca_key_path)?
        } else {
            let mut ca_dn = DistinguishedName::new();
            ca_dn.push(DnType::CommonName, "distdb-p2p-ca");

            let mut ca_params = CertificateParams::default();
            ca_params.distinguished_name = ca_dn;
            ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

            let ca_key =
                KeyPair::generate().map_err(|err| format!("failed generating CA key: {err}"))?;
            let ca_cert = ca_params
                .self_signed(&ca_key)
                .map_err(|err| format!("failed generating CA cert: {err}"))?;

            std::fs::write(&ca_cert_path, ca_cert.pem()).map_err(|err| {
                format!("failed writing CA cert '{}': {}", ca_cert_path.display(), err)
            })?;

            std::fs::write(&ca_key_path, ca_key.serialize_pem()).map_err(|err| {
                format!("failed writing CA key '{}': {}", ca_key_path.display(), err)
            })?;

            (ca_cert, ca_key)
        }
    } else {
        let ca_lock_path = tls_dir.join(".ca-init.lock");

        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&ca_lock_path)
        {
            Ok(_) => {
                struct CaLockGuard {
                    path: PathBuf,
                }
                impl Drop for CaLockGuard {
                    fn drop(&mut self) {
                        let _ = std::fs::remove_file(&self.path);
                    }
                }
                let _guard = CaLockGuard { path: ca_lock_path };

                if ca_cert_path.exists() && ca_key_path.exists() {
                    load_existing_ca(&ca_cert_path, &ca_key_path)?
                } else {
                    let mut ca_dn = DistinguishedName::new();
                    ca_dn.push(DnType::CommonName, "distdb-p2p-ca");

                    let mut ca_params = CertificateParams::default();
                    ca_params.distinguished_name = ca_dn;
                    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
                    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

                    let ca_key =
                        KeyPair::generate().map_err(|err| format!("failed generating CA key: {err}"))?;
                    let ca_cert = ca_params
                        .self_signed(&ca_key)
                        .map_err(|err| format!("failed generating CA cert: {err}"))?;

                    std::fs::write(&ca_cert_path, ca_cert.pem()).map_err(|err| {
                        format!("failed writing CA cert '{}': {}", ca_cert_path.display(), err)
                    })?;

                    std::fs::write(&ca_key_path, ca_key.serialize_pem()).map_err(|err| {
                        format!("failed writing CA key '{}': {}", ca_key_path.display(), err)
                    })?;

                    (ca_cert, ca_key)
                }
            },

            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                wait_for_ca_material(&ca_cert_path, &ca_key_path)?;
                load_existing_ca(&ca_cert_path, &ca_key_path)?
            },

            Err(err) => {
                return Err(format!(
                    "failed to acquire CA initialization lock '{}': {}",
                    ca_lock_path.display(),
                    err
                ));
            }

        }

    };

    let leaf_params = certificate_params_for_node(node_id, address_hint, extra_subject_alt_names)?;

    let leaf_key = KeyPair::generate().map_err(|err| format!("failed generating leaf key: {err}"))?;

    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &ca_cert, &ca_key)
        .map_err(|err| format!("failed generating leaf cert: {err}"))?;

    std::fs::write(&cert_path, leaf_cert.pem())
        .map_err(|err| format!("failed writing node cert '{}': {}", cert_path.display(), err))?;

    std::fs::write(&key_path, leaf_key.serialize_pem())
        .map_err(|err| format!("failed writing node key '{}': {}", key_path.display(), err))?;

    Ok(AutoTlsPaths {
        cert_path,
        key_path,
        ca_path: ca_cert_path,
    })

}

#[cfg(test)]
#[path = "p2p_tls_test.rs"]
mod tests;

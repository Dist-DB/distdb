use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, CertificateSigningRequestParams,
    DistinguishedName, DnType, IsCa, KeyPair, SanType,
};

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

fn sanitize_file_component(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
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

fn sanitize_subject_alt_names(address_hint: &str, extra_subject_alt_names: &[String]) -> BTreeSet<String> {
    let mut san_candidates = BTreeSet::new();
    san_candidates.insert("localhost".to_string());
    san_candidates.insert(extract_host(address_hint));
    for san in extra_subject_alt_names {
        let san = san.trim();
        if !san.is_empty() {
            san_candidates.insert(san.to_string());
        }
    }
    san_candidates
}

fn certificate_params_for_node(
    node_id: &str,
    address_hint: &str,
    extra_subject_alt_names: &[String],
) -> Result<CertificateParams, String> {

    let mut leaf_dn = DistinguishedName::new();
    leaf_dn.push(DnType::CommonName, node_id);

    let san_candidates = sanitize_subject_alt_names(address_hint, extra_subject_alt_names);
    let mut params = CertificateParams::new(san_candidates.iter().cloned().collect::<Vec<_>>())
        .map_err(|err| format!("failed building leaf cert params: {err}"))?;
    params.distinguished_name = leaf_dn;

    for san in &san_candidates {
        if let Ok(ip) = san.parse::<IpAddr>() {
            params.subject_alt_names.push(SanType::IpAddress(ip));
        }
    }

    Ok(params)

}

pub fn build_tls_enrollment_request(
    node_id: &str,
    address_hint: &str,
    extra_subject_alt_names: &[String],
) -> Result<TlsEnrollmentRequestMaterial, String> {

    let leaf_params = certificate_params_for_node(node_id, address_hint, extra_subject_alt_names)?;
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

    Ok((
        signed.pem(),
        ca_cert.pem(),
    ))

}

pub fn install_signed_p2p_tls(
    node_data_dir: &Path,
    node_id: &str,
    key_pem: &str,
    node_cert_pem: &str,
    ca_cert_pem: &str,
) -> Result<AutoTlsPaths, String> {

    let (tls_dir, ca_cert_path, ca_key_path) = cluster_tls_paths(node_data_dir);
    std::fs::create_dir_all(&tls_dir)
        .map_err(|err| format!("failed to create tls dir '{}': {}", tls_dir.display(), err))?;

    let node_file = sanitize_file_component(node_id);
    let cert_path = tls_dir.join(format!("{}-cert.pem", node_file));
    let key_path = tls_dir.join(format!("{}-key.pem", node_file));

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
        std::fs::write(&cert_path, node_cert_pem).map_err(|err| {
            format!("failed writing node cert '{}': {}", cert_path.display(), err)
        })?;
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

pub fn load_p2p_ca_pem(node_data_dir: &Path) -> Result<Option<String>, String> {

    let (_, ca_cert_path, _) = cluster_tls_paths(node_data_dir);
    if !ca_cert_path.exists() {
        return Ok(None);
    }

    let pem = std::fs::read_to_string(&ca_cert_path).map_err(|err| {
        format!(
            "failed reading CA cert '{}': {}",
            ca_cert_path.display(),
            err
        )
    })?;

    Ok(Some(pem))

}

pub fn import_p2p_ca_pem_if_missing(node_data_dir: &Path, ca_cert_pem: &str) -> Result<bool, String> {

    let (tls_dir, ca_cert_path, ca_key_path) = cluster_tls_paths(node_data_dir);
    std::fs::create_dir_all(&tls_dir)
        .map_err(|err| format!("failed to create tls dir '{}': {}", tls_dir.display(), err))?;

    if ca_cert_path.exists() {
        return Ok(false);
    }

    if ca_key_path.exists() {
        return Err(format!(
            "cannot import CA cert because CA key already exists at '{}'",
            ca_key_path.display()
        ));
    }

    CertificateParams::from_ca_cert_pem(ca_cert_pem)
        .map_err(|err| format!("received CA cert PEM is invalid: {err}"))?;

    std::fs::write(&ca_cert_path, ca_cert_pem).map_err(|err| {
        format!(
            "failed writing imported CA cert '{}': {}",
            ca_cert_path.display(),
            err
        )
    })?;

    Ok(true)

}

fn load_existing_ca(ca_cert_path: &Path, ca_key_path: &Path) -> Result<(Certificate, KeyPair), String> {

    let ca_cert_pem = std::fs::read_to_string(ca_cert_path).map_err(|err| {
        format!(
            "failed reading existing CA cert '{}': {}",
            ca_cert_path.display(),
            err
        )
    })?;

    let ca_key_pem = std::fs::read_to_string(ca_key_path).map_err(|err| {
        format!(
            "failed reading existing CA key '{}': {}",
            ca_key_path.display(),
            err
        )
    })?;

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

pub fn ensure_or_generate_p2p_tls(
    node_data_dir: &Path,
    node_id: &str,
    address_hint: &str,
    extra_subject_alt_names: &[String],
) -> Result<AutoTlsPaths, String> {

    let (tls_dir, ca_cert_path, ca_key_path) = cluster_tls_paths(node_data_dir);
    
    std::fs::create_dir_all(&tls_dir)
        .map_err(|err| format!("failed to create tls dir '{}': {}", tls_dir.display(), err))?;

    let node_file = sanitize_file_component(node_id);
    let cert_path = tls_dir.join(format!("{}-cert.pem", node_file));
    let key_path = tls_dir.join(format!("{}-key.pem", node_file));

    let have_ca = ca_cert_path.exists() && ca_key_path.exists();
    let have_leaf = cert_path.exists() && key_path.exists();

    if have_ca && have_leaf {
        return Ok(AutoTlsPaths {
            cert_path,
            key_path,
            ca_path: ca_cert_path,
        });
    }

    let (ca_cert, ca_key) = if have_ca {
        load_existing_ca(&ca_cert_path, &ca_key_path)?
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
                let _guard = CaLockGuard {
                    path: ca_lock_path.clone(),
                };

                if ca_cert_path.exists() && ca_key_path.exists() {
                    load_existing_ca(&ca_cert_path, &ca_key_path)?
                } else {
                    let mut ca_dn = DistinguishedName::new();
                    ca_dn.push(DnType::CommonName, "distdb-p2p-ca");

                    let mut ca_params = CertificateParams::default();
                    ca_params.distinguished_name = ca_dn;
                    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);

                    let ca_key = KeyPair::generate()
                        .map_err(|err| format!("failed generating CA key: {err}"))?;
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

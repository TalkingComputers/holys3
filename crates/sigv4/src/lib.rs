#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! AWS `SigV4` signing and credential loading.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

pub trait CredentialProvider: Send + Sync {
    fn provide(&self) -> anyhow::Result<Credentials>;
}

pub struct EnvProvider;

impl CredentialProvider for EnvProvider {
    fn provide(&self) -> anyhow::Result<Credentials> {
        from_env().ok_or_else(|| anyhow::anyhow!("env creds not found"))
    }
}

pub struct ProfileProvider {
    pub profile: String,
}

impl CredentialProvider for ProfileProvider {
    fn provide(&self) -> anyhow::Result<Credentials> {
        let path = dirs_home()?.join(".aws/credentials");
        let body = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("no env creds and cannot read {}: {e}", path.display()))?;
        from_credentials_file(&body, &self.profile).ok_or_else(|| {
            anyhow::anyhow!("profile `{}` not found in {}", self.profile, path.display())
        })
    }
}

pub struct ChainProvider(pub Vec<Box<dyn CredentialProvider>>);

impl CredentialProvider for ChainProvider {
    fn provide(&self) -> anyhow::Result<Credentials> {
        let mut error = None;
        for provider in &self.0 {
            match provider.provide() {
                Ok(credentials) => return Ok(credentials),
                Err(err) => error = Some(err),
            }
        }
        let Some(err) = error else {
            anyhow::bail!("no credential providers configured");
        };
        Err(err)
    }
}

/// One header to attach to the outgoing request.
#[derive(Debug, PartialEq, Eq)]
pub struct SignedHeaders {
    pub authorization: String,
    pub x_amz_date: String,
    pub x_amz_content_sha256: String,
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = HmacSha256::new_from_slice(key).unwrap();
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Percent-encode per AWS rules: unreserved A-Za-z0-9-_.~ pass through; everything
/// else %XX uppercase; in path mode '/' passes through.
fn uri_encode(s: &str, is_path: bool) -> String {
    let mut out = String::new();
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b'/' if is_path => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Sign a GET (or HEAD) request with UNSIGNED-PAYLOAD.
/// `amz_date` is YYYYMMDD'T'HHMMSS'Z' basic format; `date` is "YYYYMMDD".
/// `canonical_query` must already be sorted+encoded (or empty).
/// `extra_signed` are additional (lowercase-name, value) header pairs to sign
/// (e.g. ("range", "bytes=0-9")); `host` is always signed.
#[allow(clippy::too_many_arguments)]
pub fn sign_get(
    creds: &Credentials,
    region: &str,
    host: &str,
    canonical_path: &str,
    canonical_query: &str,
    extra_signed: &[(&str, &str)],
    amz_date: &str,
    date: &str,
) -> SignedHeaders {
    sign_request(
        "GET",
        creds,
        region,
        host,
        canonical_path,
        canonical_query,
        extra_signed,
        amz_date,
        date,
        "UNSIGNED-PAYLOAD",
    )
}

#[allow(clippy::too_many_arguments)]
pub fn sign_get_with_payload_hash(
    creds: &Credentials,
    region: &str,
    host: &str,
    canonical_path: &str,
    canonical_query: &str,
    extra_signed: &[(&str, &str)],
    amz_date: &str,
    date: &str,
    payload_hash: &str,
) -> SignedHeaders {
    sign_request(
        "GET",
        creds,
        region,
        host,
        canonical_path,
        canonical_query,
        extra_signed,
        amz_date,
        date,
        payload_hash,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn sign_request(
    method: &str,
    creds: &Credentials,
    region: &str,
    host: &str,
    canonical_path: &str,
    canonical_query: &str,
    extra_signed: &[(&str, &str)],
    amz_date: &str,
    date: &str,
    payload_hash: &str,
) -> SignedHeaders {
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), host.into()),
        ("x-amz-content-sha256".into(), payload_hash.into()),
        ("x-amz-date".into(), amz_date.into()),
    ];
    for (k, v) in extra_signed {
        headers.push(((*k).to_owned(), (*v).to_owned()));
    }
    if let Some(tok) = &creds.session_token {
        headers.push(("x-amz-security-token".into(), tok.clone()));
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let signed_headers = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers = headers
        .iter()
        .map(|(k, v)| format!("{k}:{}\n", v.trim()))
        .collect::<String>();

    let canonical_request = format!(
        "{method}\n{}\n{}\n{}\n{}\n{}",
        uri_encode(canonical_path, true),
        canonical_query,
        canonical_headers,
        signed_headers,
        payload_hash
    );

    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let k_date = hmac(
        format!("AWS4{}", creds.secret_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, b"s3");
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope},SignedHeaders={signed_headers},Signature={signature}",
        creds.access_key
    );
    SignedHeaders {
        authorization,
        x_amz_date: amz_date.into(),
        x_amz_content_sha256: payload_hash.into(),
    }
}

/// Resolve credentials: env vars first, then a named profile in ~/.aws/credentials.
/// (`IMDSv2` is added in a later step; not covered by this unit test.)
pub fn from_env() -> Option<Credentials> {
    let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
    Some(Credentials {
        access_key,
        secret_key,
        session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
    })
}

/// Parse `[profile]` ... `access_key/secret/token` from an ini-style credentials file body.
pub fn from_credentials_file(body: &str, profile: &str) -> Option<Credentials> {
    let mut in_section = false;
    let (mut ak, mut sk, mut tok) = (None, None, None);
    for line in body.lines() {
        let line = line.trim();
        if line.starts_with('[') && line.ends_with(']') {
            in_section = &line[1..line.len() - 1] == profile;
            continue;
        }
        if !in_section {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            match k.trim() {
                "aws_access_key_id" => ak = Some(v.trim().to_owned()),
                "aws_secret_access_key" => sk = Some(v.trim().to_owned()),
                "aws_session_token" => tok = Some(v.trim().to_owned()),
                _ => {}
            }
        }
    }
    Some(Credentials {
        access_key: ak?,
        secret_key: sk?,
        session_token: tok,
    })
}

/// Public entry: env, then default profile in ~/.aws/credentials.
pub fn resolve(profile: &str) -> anyhow::Result<Credentials> {
    ChainProvider(vec![
        Box::new(EnvProvider),
        Box::new(ProfileProvider {
            profile: profile.to_owned(),
        }),
    ])
    .provide()
}

fn dirs_home() -> anyhow::Result<std::path::PathBuf> {
    Ok(std::path::PathBuf::from(std::env::var("HOME")?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aws_get_object_range_vector() {
        let creds = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        };
        let signed = sign_get(
            &creds,
            "us-east-1",
            "examplebucket.s3.amazonaws.com",
            "/test.txt",
            "",
            &[("range", "bytes=0-9")],
            "20130524T000000Z",
            "20130524",
        );
        assert!(signed.authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"
        ));
        assert!(signed
            .authorization
            .contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date"));
        let sig = signed.authorization.rsplit("Signature=").next().unwrap();
        assert_eq!(sig.len(), 64);
        assert!(sig.bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn aws_get_object_exact_signature() {
        let creds = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        };
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let signed = sign_get_with_payload_hash(
            &creds,
            "us-east-1",
            "examplebucket.s3.amazonaws.com",
            "/test.txt",
            "",
            &[("range", "bytes=0-9")],
            "20130524T000000Z",
            "20130524",
            empty,
        );
        let sig = signed.authorization.rsplit("Signature=").next().unwrap();
        assert_eq!(
            sig,
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn put_signature_has_expected_shape() {
        let creds = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        };
        let signed = sign_request(
            "PUT",
            &creds,
            "us-east-1",
            "examplebucket.s3.amazonaws.com",
            "/test.txt",
            "",
            &[],
            "20130524T000000Z",
            "20130524",
            "UNSIGNED-PAYLOAD",
        );
        assert!(signed
            .authorization
            .contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        let sig = signed.authorization.rsplit("Signature=").next().unwrap();
        assert_eq!(sig.len(), 64);
        assert!(sig.bytes().all(|b| b.is_ascii_hexdigit()));
    }
}

#[cfg(test)]
mod cred_tests {
    use super::*;

    #[test]
    fn parse_profile() {
        let body = "[default]\naws_access_key_id = AK1\naws_secret_access_key = SK1\n\n[prod]\naws_access_key_id=AK2\naws_secret_access_key=SK2\naws_session_token=TOK2\n";
        let d = from_credentials_file(body, "default").unwrap();
        assert_eq!(d.access_key, "AK1");
        assert!(d.session_token.is_none());
        let p = from_credentials_file(body, "prod").unwrap();
        assert_eq!(p.access_key, "AK2");
        assert_eq!(p.session_token.as_deref(), Some("TOK2"));
    }
}

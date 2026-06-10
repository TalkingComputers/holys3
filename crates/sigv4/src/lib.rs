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

/// One header to attach to the outgoing request.
#[derive(Debug, PartialEq, Eq)]
pub struct SignedHeaders {
    pub authorization: String,
    pub x_amz_date: String,
    pub x_amz_content_sha256: String,
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
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

pub fn encode_query_component(s: &str) -> String {
    uri_encode(s, false)
}

/// Encode a request path for both the canonical request and the request URL.
/// The same encoded string must be signed and sent.
pub fn encode_path(path: &str) -> String {
    uri_encode(path, true)
}

/// Sign a GET (or HEAD) request with UNSIGNED-PAYLOAD.
/// `amz_date` is YYYYMMDD'T'HHMMSS'Z' basic format; `date` is "YYYYMMDD".
/// `canonical_path` and `canonical_query` must already be encoded (see
/// [`encode_path`] / [`encode_query_component`]); the exact same strings must
/// be sent on the wire. `extra_signed` are additional (lowercase-name, value)
/// header pairs to sign (e.g. ("range", "bytes=0-9")); `host` is always signed.
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
#[cfg(test)]
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
        "{method}\n{canonical_path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
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

/// The active profile name: `$AWS_PROFILE` or "default".
pub fn profile_name() -> anyhow::Result<String> {
    match std::env::var("AWS_PROFILE") {
        Ok(profile) => Ok(profile),
        Err(std::env::VarError::NotPresent) => Ok("default".to_owned()),
        Err(err) => Err(err.into()),
    }
}

fn aws_dir() -> anyhow::Result<std::path::PathBuf> {
    Ok(std::path::PathBuf::from(std::env::var("HOME")?).join(".aws"))
}

/// Static credentials: env vars first, then the active profile in
/// ~/.aws/credentials. `None` when neither has keys (the SSO chain in
/// holys3-s3 takes over from there).
pub fn resolve_static() -> anyhow::Result<Option<Credentials>> {
    if let Some(creds) = from_env() {
        return Ok(Some(creds));
    }
    let path = aws_dir()?.join("credentials");
    let Ok(body) = std::fs::read_to_string(&path) else {
        return Ok(None);
    };
    Ok(from_credentials_file(&body, &profile_name()?))
}

/// SSO settings for one profile in ~/.aws/config (modern `sso-session`
/// style or legacy inline `sso_start_url`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsoProfile {
    pub profile: String,
    pub account_id: String,
    pub role_name: String,
    pub start_url: String,
    pub sso_region: String,
    pub session_name: Option<String>,
}

fn config_section<'a>(
    body: &'a str,
    header: String,
) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
    let mut in_section = false;
    body.lines().filter_map(move |line| {
        let line = line.trim();
        if line.starts_with('[') && line.ends_with(']') {
            in_section = line[1..line.len() - 1].trim() == header;
            return None;
        }
        if !in_section {
            return None;
        }
        line.split_once('=')
            .map(|(k, v)| (k.trim(), v.trim()))
            .filter(|(k, _)| !k.is_empty())
    })
}

/// Parse the SSO settings for `profile` out of a ~/.aws/config body.
/// `None` when the profile has no SSO configuration.
pub fn sso_profile_from_config(body: &str, profile: &str) -> Option<SsoProfile> {
    let header = if profile == "default" {
        "default".to_owned()
    } else {
        format!("profile {profile}")
    };
    let mut account_id = None;
    let mut role_name = None;
    let mut start_url = None;
    let mut sso_region = None;
    let mut session_name = None;
    for (key, value) in config_section(body, header) {
        match key {
            "sso_account_id" => account_id = Some(value.to_owned()),
            "sso_role_name" => role_name = Some(value.to_owned()),
            "sso_start_url" => start_url = Some(value.to_owned()),
            "sso_region" => sso_region = Some(value.to_owned()),
            "sso_session" => session_name = Some(value.to_owned()),
            _ => {}
        }
    }
    if let Some(session) = &session_name {
        let session_header = format!("sso-session {session}");
        for (key, value) in config_section(body, session_header) {
            match key {
                "sso_start_url" => start_url = Some(value.to_owned()),
                "sso_region" => sso_region = Some(value.to_owned()),
                _ => {}
            }
        }
    }
    Some(SsoProfile {
        profile: profile.to_owned(),
        account_id: account_id?,
        role_name: role_name?,
        start_url: start_url?,
        sso_region: sso_region?,
        session_name,
    })
}

/// SSO settings for the active profile, from ~/.aws/config on disk.
pub fn sso_profile() -> anyhow::Result<Option<SsoProfile>> {
    let path = aws_dir()?.join("config");
    let Ok(body) = std::fs::read_to_string(&path) else {
        return Ok(None);
    };
    Ok(sso_profile_from_config(&body, &profile_name()?))
}

/// The cache filename hash input is the session name when present, else the
/// start URL (matches botocore's `SSOTokenLoader`).
pub fn sso_token_cache_key(profile: &SsoProfile) -> String {
    let input = profile
        .session_name
        .as_deref()
        .unwrap_or(&profile.start_url);
    hex::encode(<sha1::Sha1 as Digest>::digest(input.as_bytes()))
}

/// Read the cached SSO access token for `profile`; errors actionably when
/// missing or expired.
pub fn read_sso_token(profile: &SsoProfile) -> anyhow::Result<String> {
    let path = aws_dir()?
        .join("sso/cache")
        .join(format!("{}.json", sso_token_cache_key(profile)));
    let login_hint = format!("run `aws sso login --profile {}`", profile.profile);
    let body = std::fs::read_to_string(&path).map_err(|_| {
        anyhow::anyhow!(
            "no cached SSO token for profile `{}`; {login_hint}",
            profile.profile
        )
    })?;
    let token: serde_json::Value = serde_json::from_str(&body)?;
    let expires_at = token["expiresAt"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("SSO token cache {} has no expiresAt", path.display()))?;
    let expires_at =
        time::OffsetDateTime::parse(expires_at, &time::format_description::well_known::Rfc3339)?;
    anyhow::ensure!(
        expires_at > time::OffsetDateTime::now_utc(),
        "SSO token for profile `{}` expired; {login_hint}",
        profile.profile
    );
    token["accessToken"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("SSO token cache {} has no accessToken", path.display()))
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

    #[test]
    fn encode_path_escapes_special_chars() {
        assert_eq!(encode_path("/a/b.txt"), "/a/b.txt");
        assert_eq!(encode_path("/a b+c#d?e%f"), "/a%20b%2Bc%23d%3Fe%25f");
        assert_eq!(encode_query_component("a/b"), "a%2Fb");
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

    #[test]
    fn parse_sso_session_style_config() {
        let body = "[profile speedtrain]\nsso_session = speedtrain\nsso_account_id = 381235349110\nsso_role_name = AdministratorAccess\nregion = us-east-2\n\n[sso-session speedtrain]\nsso_start_url = https://speedtrain.awsapps.com/start\nsso_region = us-west-2\n";
        let p = sso_profile_from_config(body, "speedtrain").unwrap();
        assert_eq!(p.account_id, "381235349110");
        assert_eq!(p.role_name, "AdministratorAccess");
        assert_eq!(p.start_url, "https://speedtrain.awsapps.com/start");
        assert_eq!(p.sso_region, "us-west-2");
        assert_eq!(p.session_name.as_deref(), Some("speedtrain"));
        // The cache file is keyed by the session name, per botocore.
        assert_eq!(
            sso_token_cache_key(&p),
            "fd66acda1ac8273e084bf508925ad8f1d566ec92"
        );
    }

    #[test]
    fn parse_legacy_inline_sso_config() {
        let body = "[profile old]\nsso_start_url = https://corp.awsapps.com/start\nsso_region = eu-west-1\nsso_account_id = 1\nsso_role_name = ReadOnly\n";
        let p = sso_profile_from_config(body, "old").unwrap();
        assert_eq!(p.start_url, "https://corp.awsapps.com/start");
        assert!(p.session_name.is_none());
        // Legacy style keys the cache by the start URL.
        assert_eq!(
            sso_token_cache_key(&p),
            hex::encode(<sha1::Sha1 as Digest>::digest(
                b"https://corp.awsapps.com/start"
            ))
        );
    }

    #[test]
    fn non_sso_profile_yields_none() {
        let body = "[profile plain]\nregion = us-east-1\n";
        assert!(sso_profile_from_config(body, "plain").is_none());
        assert!(sso_profile_from_config(body, "missing").is_none());
    }
}

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
    sign_get_with_payload_hash(
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
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), host.into()),
        ("x-amz-content-sha256".into(), payload_hash.into()),
        ("x-amz-date".into(), amz_date.into()),
    ];
    for (k, v) in extra_signed {
        headers.push(((*k).to_string(), (*v).to_string()));
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
        "GET\n{}\n{}\n{}\n{}\n{}",
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
}

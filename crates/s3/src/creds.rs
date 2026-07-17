//! Disk cache for resolved AWS SSO credentials.
//!
//! The Rust SDK resolves SSO role credentials on every invocation (~550ms
//! against the WAN); unlike the AWS CLI it never persists the derived
//! credentials. This provider caches the resolved credentials under the user
//! cache dir — the same trust model as the CLI's `~/.aws/cli/cache` — and
//! falls through to the real chain on a miss or near expiry, so long-running
//! builds still refresh.
//!
//! Persistence is scoped to SSO-configured profiles because SSO is the only
//! source whose identity inputs are all keyable files: the shared config
//! files select the account and role, and the SSO token cache holds the
//! login. Environment credentials, container roles, instance metadata, and
//! `credential_process` can all change identity without touching a keyable
//! input — and they resolve locally and near-instantly, so a disk cache buys
//! them nothing. Cache reads are self-healing: an unreadable, corrupt,
//! oversized, or stale entry is a miss, never an error.
//!
//! The SDK's in-memory identity cache refreshes ahead of the credentials'
//! real expiry by re-invoking this provider; the disk margin below only
//! decides whether a *new* process trusts an entry, so the two buffers
//! compose rather than conflict.

use anyhow::{Context, Result};
use aws_credential_types::provider::{self, future, ProvideCredentials, SharedCredentialsProvider};
use aws_credential_types::Credentials;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Refuse cached credentials this close to expiry: enough for clock skew and
/// an in-flight request; anything longer just re-resolves early.
const EXPIRY_MARGIN: Duration = Duration::from_secs(120);

/// Cached entries are a few hundred bytes; anything larger is not ours.
const MAX_ENTRY_BYTES: u64 = 64 * 1024;

#[derive(Serialize, Deserialize)]
struct StoredCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    account_id: Option<String>,
    expires_unix_secs: u64,
}

/// The complete cache key state, frozen once per process. The SDK also
/// freezes its view of every keyed input for the process lifetime: the
/// profile files at load and the SSO token in memory until it expires — so
/// a per-process snapshot is the *matching* granularity, and a fresh
/// `aws sso login` reaches the key on the next process.
///
/// Call [`key_base`] BEFORE `ConfigLoader::load` and verify it afterwards
/// with [`KeyBase::still_current`]: an edit to any keyed input in between —
/// the whole window in which the SDK reads them — lands on one side of that
/// bracket and voids the write, so credentials can never be stored under a
/// key describing inputs the SDK did not use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyBase {
    path: PathBuf,
    frozen: [u8; 32],
}

impl KeyBase {
    fn still_current(&self) -> bool {
        key_base().as_ref() == Some(self)
    }
}

/// Wraps the PROFILE-ONLY credentials provider, never the default chain:
/// the chain falls through to instance metadata when SSO fails, and those
/// credentials must never be persisted under the SSO key. With a pure-SSO
/// profile (the only mode that constructs this) the inner provider can only
/// yield SSO-derived credentials or fail loudly.
#[derive(Debug)]
pub(crate) struct DiskCachedProvider {
    inner: SharedCredentialsProvider,
    base: KeyBase,
    #[cfg(test)]
    verify_base: bool,
}

impl DiskCachedProvider {
    pub(crate) fn new(inner: SharedCredentialsProvider, base: KeyBase) -> Self {
        Self {
            inner,
            base,
            #[cfg(test)]
            verify_base: true,
        }
    }

    #[cfg(test)]
    fn with_path(inner: SharedCredentialsProvider, path: PathBuf) -> Self {
        Self {
            inner,
            base: KeyBase {
                path,
                frozen: [0; 32],
            },
            verify_base: false,
        }
    }

    fn base_is_current(&self) -> bool {
        #[cfg(test)]
        if !self.verify_base {
            return true;
        }
        self.base.still_current()
    }

    async fn load(&self) -> provider::Result {
        if let Some(credentials) = read_cached(&self.base.path, SystemTime::now()) {
            return Ok(credentials);
        }
        let credentials = self.inner.provide_credentials().await?;
        if credentials.expiry().is_some() {
            // The write-side bracket: only persist while every keyed input
            // still matches the pre-load snapshot. A failed cache write must
            // not fail the request it accelerates.
            if self.base_is_current() {
                let _ = write_cached(&self.base.path, &credentials);
            }
        }
        Ok(credentials)
    }
}

impl ProvideCredentials for DiskCachedProvider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(self.load())
    }
}

/// Reads a keyed input file. Absent is a legitimate state (hashed as empty);
/// any other read error disables caching rather than storing an entry under
/// the wrong key.
fn read_keyed_file(path: &Path) -> Option<Vec<u8>> {
    match std::fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Some(Vec::new()),
        Err(_) => None,
    }
}

/// The lowercase key of every `key = value` line inside sections whose
/// header `matches`. Indented lines are value continuations, not keys, and
/// the SDK lowercases keys, so `SSO_SESSION` and `sso_session` are one key.
fn section_keys(text: &str, matches: impl Fn(&str) -> bool) -> Vec<String> {
    let mut keys = Vec::new();
    let mut active = false;
    for raw in text.lines() {
        let line = raw.trim();
        if line.starts_with('[') {
            // The SDK strips inline comments from headers and tolerates
            // interior whitespace: `[ profile books ] # prod` names books.
            let uncommented = line.split(['#', ';']).next().unwrap_or_default();
            let header = uncommented
                .trim()
                .trim_start_matches('[')
                .trim_end_matches(']')
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            active = matches(&header);
        } else if active && !raw.starts_with([' ', '\t']) {
            if let Some((key, _)) = line.split_once('=') {
                keys.push(key.trim().to_lowercase());
            }
        }
    }
    keys
}

/// True when the active profile resolves through SSO and nothing else.
///
/// Enabling is strict: the SSO marker must sit in the section header form
/// the SDK actually reads from the config file (`[profile name]`, or
/// `[default]`). Disqualifying is lax: a competing credential source in any
/// header form that could plausibly bind to this profile — in either file,
/// since the credentials file wins for the same name — disables the cache.
/// When unsure, no cache: the failure mode of strictness is a slow first
/// call, the failure mode of laxness is caching an unkeyable identity.
fn profile_selects_sso(config: &str, credentials: &str, profile: &str) -> bool {
    let strict = |header: &str| {
        header == format!("profile {profile}") || (profile == "default" && header == "default")
    };
    if !section_keys(config, strict)
        .iter()
        .any(|key| key == "sso_session" || key == "sso_start_url")
    {
        return false;
    }
    let lax = |header: &str| header == profile || header == format!("profile {profile}");
    const COMPETING_SOURCES: [&str; 6] = [
        "credential_process",
        "credential_source",
        "web_identity_token_file",
        "role_arn",
        "source_profile",
        "aws_access_key_id",
    ];
    !section_keys(config, lax)
        .iter()
        .chain(section_keys(credentials, lax).iter())
        .any(|key| COMPETING_SOURCES.contains(&key.as_str()))
}

/// Everything SSO resolution reads to pick an identity: the profile
/// selection, both shared config files, and the SSO token cache.
/// Environments that resolve credentials some other way return `None`: no
/// cache. `AWS_DEFAULT_PROFILE` is deliberately ignored — the Rust SDK only
/// honors `AWS_PROFILE`, and the key must select exactly the profile the
/// SDK resolves.
pub(crate) fn key_base() -> Option<KeyBase> {
    for var in [
        "AWS_ACCESS_KEY_ID",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    ] {
        if std::env::var_os(var).is_some() {
            return None;
        }
    }
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()?;
    let home = Path::new(&home);
    let profile = std::env::var("AWS_PROFILE").unwrap_or_else(|_| "default".to_owned());
    let config_path = std::env::var("AWS_CONFIG_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home.join(".aws/config"));
    let credentials_path = std::env::var("AWS_SHARED_CREDENTIALS_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home.join(".aws/credentials"));
    let config = read_keyed_file(&config_path)?;
    let credentials = read_keyed_file(&credentials_path)?;
    if !profile_selects_sso(
        &String::from_utf8_lossy(&config),
        &String::from_utf8_lossy(&credentials),
        &profile,
    ) {
        return None;
    }
    let token_state = sso_token_cache_state(&home.join(".aws/sso/cache"))?;
    // A currently-valid login token is required: SSO resolution cannot have
    // succeeded without one, so whatever the chain returned past this point
    // (an IMDS fallthrough, say) depends on unkeyable inputs. No cache.
    if !token_state
        .iter()
        .any(|(_, bytes)| holds_unexpired_token(bytes))
    {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(&profile);
    hasher.update([0]);
    hasher.update(&config);
    hasher.update([0]);
    hasher.update(&credentials);
    hasher.update([0]);
    for var in ["AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_SSO"] {
        hasher.update(std::env::var(var).unwrap_or_default());
        hasher.update([0]);
    }
    for (name, bytes) in token_state {
        hasher.update(&name);
        hasher.update([0]);
        hasher.update(&bytes);
        hasher.update([0]);
    }
    let frozen: [u8; 32] = hasher.finalize().into();
    let key = frozen
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut path = seagrep_core::cache_home().ok()?;
    path.push("seagrep");
    path.push("credentials");
    path.push(format!("{key}.json"));
    Some(KeyBase { path, frozen })
}

/// True when the token file carries an `expiresAt` in the future. Token
/// cache timestamps are RFC 3339 UTC.
fn holds_unexpired_token(bytes: &[u8]) -> bool {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Token {
        expires_at: String,
    }
    let Ok(token) = serde_json::from_slice::<Token>(bytes) else {
        return false;
    };
    let Ok(expires_at) = aws_smithy_types::DateTime::from_str(
        &token.expires_at,
        aws_smithy_types::date_time::Format::DateTime,
    ) else {
        return false;
    };
    SystemTime::try_from(expires_at).is_ok_and(|expiry| expiry > SystemTime::now())
}

/// The SSO token cache contents, sorted by file name. A missing directory is
/// a legitimate (logged-out) state; a read error disables caching.
fn sso_token_cache_state(dir: &Path) -> Option<Vec<(String, Vec<u8>)>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Some(Vec::new()),
        Err(_) => return None,
    };
    let mut state = Vec::new();
    for entry in entries {
        let entry = entry.ok()?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".json") {
            continue;
        }
        state.push((name, std::fs::read(entry.path()).ok()?));
    }
    state.sort();
    Some(state)
}

fn read_cached(path: &Path, now: SystemTime) -> Option<Credentials> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .ok()?
        .take(MAX_ENTRY_BYTES + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > MAX_ENTRY_BYTES {
        return None;
    }
    let stored: StoredCredentials = serde_json::from_slice(&bytes).ok()?;
    let expiry =
        SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(stored.expires_unix_secs))?;
    if expiry <= now.checked_add(EXPIRY_MARGIN)? {
        return None;
    }
    let mut builder = Credentials::builder()
        .access_key_id(stored.access_key_id)
        .secret_access_key(stored.secret_access_key)
        .expiry(expiry)
        .provider_name("seagrep-disk-cache");
    builder.set_session_token(stored.session_token);
    builder.set_account_id(stored.account_id.map(Into::into));
    Some(builder.build())
}

fn write_cached(path: &Path, credentials: &Credentials) -> Result<()> {
    let expiry = credentials
        .expiry()
        .context("only expiring credentials are cached")?;
    let stored = StoredCredentials {
        access_key_id: credentials.access_key_id().to_owned(),
        secret_access_key: credentials.secret_access_key().to_owned(),
        session_token: credentials.session_token().map(str::to_owned),
        account_id: credentials.account_id().map(|id| id.as_str().to_owned()),
        expires_unix_secs: expiry
            .duration_since(SystemTime::UNIX_EPOCH)
            .context("credential expiry predates the unix epoch")?
            .as_secs(),
    };
    let dir = path.parent().context("cache path has no parent")?;
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    serde_json::to_writer(&mut file, &stored)?;
    file.persist(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn expiring_credentials(expiry: SystemTime) -> Credentials {
        Credentials::new("AKID", "SECRET", Some("TOKEN".into()), Some(expiry), "test")
    }

    #[test]
    fn cached_credentials_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.json");
        let now = SystemTime::now();
        let expiry = now + Duration::from_secs(3600);
        let credentials = Credentials::builder()
            .access_key_id("AKID")
            .secret_access_key("SECRET")
            .session_token("TOKEN")
            .account_id("381235349110")
            .expiry(expiry)
            .provider_name("test")
            .build();
        write_cached(&path, &credentials).unwrap();

        let cached = read_cached(&path, now).expect("fresh entry must hit");
        assert_eq!(cached.access_key_id(), "AKID");
        assert_eq!(cached.secret_access_key(), "SECRET");
        assert_eq!(cached.session_token(), Some("TOKEN"));
        assert_eq!(
            cached.account_id().map(|id| id.as_str()),
            Some("381235349110"),
            "account id must survive the round trip"
        );
        let cached_expiry = cached.expiry().expect("expiry survives the round trip");
        assert_eq!(
            cached_expiry
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            expiry
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "cache file must be private");
        }
    }

    #[test]
    fn near_expiry_corrupt_and_hostile_entries_miss() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.json");
        let now = SystemTime::now();

        write_cached(&path, &expiring_credentials(now + Duration::from_secs(60))).unwrap();
        assert!(
            read_cached(&path, now).is_none(),
            "an entry inside the expiry margin must miss"
        );

        std::fs::write(&path, b"not json").unwrap();
        assert!(
            read_cached(&path, now).is_none(),
            "corrupt entries must miss"
        );

        let overflow = serde_json::json!({
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
            "session_token": null,
            "account_id": null,
            "expires_unix_secs": u64::MAX,
        });
        std::fs::write(&path, serde_json::to_vec(&overflow).unwrap()).unwrap();
        assert!(
            read_cached(&path, now).is_none(),
            "an overflowing expiry must miss, not panic"
        );

        std::fs::write(&path, vec![b' '; (MAX_ENTRY_BYTES + 1) as usize]).unwrap();
        assert!(
            read_cached(&path, now).is_none(),
            "oversized entries must miss unread"
        );
        assert!(read_cached(dir.path().join("absent.json").as_path(), now).is_none());
    }

    #[test]
    fn only_pure_sso_profiles_enable_the_cache() {
        let sso = "[default]\nsso_session = corp\nregion = us-east-2\n\n[sso-session corp]\nsso_start_url = https://corp.awsapps.com/start\n";
        assert!(profile_selects_sso(sso, "", "default"));
        assert!(!profile_selects_sso(sso, "", "other"));

        let legacy = "[profile books]\nsso_start_url = https://corp.awsapps.com/start\n";
        assert!(profile_selects_sso(legacy, "", "books"));

        let iam = "[default]\nregion = us-east-2\n\n[profile books]\ncredential_process = /usr/bin/vault-helper\n";
        assert!(!profile_selects_sso(iam, "", "default"));
        assert!(!profile_selects_sso(iam, "", "books"));
        assert!(!profile_selects_sso("", "", "default"));
    }

    #[test]
    fn header_comments_and_whitespace_do_not_hide_sections() {
        // The SDK reads `[ profile books ] # prod` as profile books; the
        // disqualifier scan must see into it.
        let commented =
            "[ profile books ] # prod\nsso_session = corp\nrole_arn = arn:aws:iam::1:role/x\n";
        assert!(!profile_selects_sso(commented, "", "books"));

        let clean = "[ profile books ]\t; prod\nsso_session = corp\n";
        assert!(profile_selects_sso(clean, "", "books"));
    }

    #[test]
    fn expired_or_malformed_login_tokens_disable_the_cache() {
        assert!(holds_unexpired_token(
            br#"{"accessToken": "t", "expiresAt": "2999-01-01T00:00:00Z"}"#
        ));
        assert!(!holds_unexpired_token(
            br#"{"accessToken": "t", "expiresAt": "2020-01-01T00:00:00Z"}"#
        ));
        assert!(!holds_unexpired_token(br#"{"accessToken": "t"}"#));
        assert!(!holds_unexpired_token(
            br#"{"expiresAt": "not a timestamp"}"#
        ));
        assert!(!holds_unexpired_token(b"not json"));
    }

    #[test]
    fn competing_credential_sources_disable_the_cache() {
        // SSO fields mixed with another source in the same section: the
        // chain may not resolve via SSO, so nothing is keyable.
        let mixed = "[profile books]\nsso_start_url = https://corp.awsapps.com/start\ncredential_process = /usr/bin/vault-helper\n";
        assert!(!profile_selects_sso(mixed, "", "books"));

        let assumed = "[profile books]\nsso_session = corp\nrole_arn = arn:aws:iam::1:role/x\nsource_profile = base\n";
        assert!(!profile_selects_sso(assumed, "", "books"));

        // The credentials file wins over the config file for the same
        // profile name; a competing entry there must also disqualify.
        let sso = "[profile books]\nsso_session = corp\n";
        assert!(profile_selects_sso(sso, "", "books"));
        assert!(!profile_selects_sso(
            sso,
            "[books]\naws_access_key_id = AKID\n",
            "books"
        ));
        assert!(!profile_selects_sso(
            sso,
            "[books]\ncredential_process = /usr/bin/helper\n",
            "books"
        ));
        assert!(profile_selects_sso(
            sso,
            "[other]\naws_access_key_id = AKID\n",
            "books"
        ));
    }

    #[derive(Debug)]
    struct CountingChain {
        calls: Arc<AtomicUsize>,
        expiry: SystemTime,
    }

    impl ProvideCredentials for CountingChain {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            future::ProvideCredentials::ready(Ok(expiring_credentials(self.expiry)))
        }
    }

    #[test]
    fn provider_serves_repeat_lookups_without_the_chain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.json");
        let calls = Arc::new(AtomicUsize::new(0));
        let chain = SharedCredentialsProvider::new(CountingChain {
            calls: calls.clone(),
            expiry: SystemTime::now() + Duration::from_secs(3600),
        });
        let provider = DiskCachedProvider::with_path(chain, path.clone());

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let first = rt.block_on(provider.provide_credentials()).unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "first lookup resolves the chain"
        );
        assert!(path.exists(), "resolved credentials must be cached");

        let second = rt.block_on(provider.provide_credentials()).unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "repeat lookup must not hit the chain"
        );
        assert_eq!(second.access_key_id(), first.access_key_id());
        assert_eq!(second.session_token(), first.session_token());
    }
}

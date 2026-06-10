//! AWS IAM Identity Center (SSO) role credentials, from the token cache
//! `aws sso login` maintains. Endpoint and header shapes follow the
//! `GetRoleCredentials` operation in botocore's sso/2019-06-10 service model.

use anyhow::{Context, Result};
use holys3_sigv4::{encode_query_component, read_sso_token, Credentials, SsoProfile};

pub(crate) fn role_credentials(profile: &SsoProfile) -> Result<Credentials> {
    let token = read_sso_token(profile)?;
    let url = format!(
        "https://portal.sso.{}.amazonaws.com/federation/credentials?account_id={}&role_name={}",
        profile.sso_region,
        encode_query_component(&profile.account_id),
        encode_query_component(&profile.role_name),
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let body: serde_json::Value = rt.block_on(async {
        let response = reqwest::Client::new()
            .get(url)
            .header("x-amz-sso_bearer_token", &token)
            .send()
            .await
            .context("SSO portal request failed")?;
        anyhow::ensure!(
            response.status().is_success(),
            "SSO GetRoleCredentials returned HTTP {} for profile `{}`; try `aws sso login --profile {}`",
            response.status(),
            profile.profile,
            profile.profile
        );
        let bytes = response.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    })?;
    let role = &body["roleCredentials"];
    let field = |name: &str| -> Result<String> {
        role[name]
            .as_str()
            .map(str::to_owned)
            .with_context(|| format!("SSO roleCredentials missing {name}"))
    };
    Ok(Credentials {
        access_key: field("accessKeyId")?,
        secret_key: field("secretAccessKey")?,
        session_token: Some(field("sessionToken")?),
    })
}

//! SCRAM credentials: describe and alter.

use std::collections::HashMap;

use crate::error::Result;
use crate::protocol::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, ApiKey,
    DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse,
    ScramCredentialDeletion, ScramCredentialUpsertion, versions,
};

pub use crate::protocol::ScramCredentialInfo;

use super::AdminClient;
use super::driver::{Mode, Target, answer, exchange, negotiate};

admin_options! {
    /// Options for [`AdminClient::describe_user_scram_credentials`].
    DescribeUserScramCredentialsOptions {}
}

admin_options! {
    /// Options for [`AdminClient::alter_user_scram_credentials`].
    AlterUserScramCredentialsOptions {}
}

impl AdminClient {
    /// Describe users' SCRAM credentials (any broker); `None` describes every
    /// user. Returns a result per user.
    ///
    /// # Errors
    ///
    /// A request-level broker error, a closed client, or the deadline.
    pub async fn describe_user_scram_credentials(
        &self,
        users: Option<Vec<String>>,
        options: DescribeUserScramCredentialsOptions,
    ) -> Result<HashMap<String, Result<Vec<ScramCredentialInfo>>>> {
        let call = self.call("DescribeUserScramCredentials", Mode::Read, options.timeout)?;
        let users = &users;
        call.single(Target::AnyBroker, |conn| async move {
            let request = DescribeUserScramCredentialsRequest {
                users: users.clone(),
            };
            let version = negotiate(
                &conn,
                ApiKey::DescribeUserScramCredentials,
                versions::DESCRIBE_USER_SCRAM_CREDENTIALS_MIN,
                versions::DESCRIBE_USER_SCRAM_CREDENTIALS_MAX,
            )?;
            let response: DescribeUserScramCredentialsResponse = exchange(
                &conn,
                ApiKey::DescribeUserScramCredentials,
                version,
                &request,
            )
            .await?;
            answer(response.error_code, response.error_message)?;
            Ok(response
                .results
                .into_iter()
                .map(|r| {
                    let infos = answer(r.error_code, r.error_message).map(|()| r.credential_infos);
                    (r.user, infos)
                })
                .collect())
        })
        .await
    }

    /// Delete and upsert users' SCRAM credentials (controller). Returns a
    /// result per user.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client.
    pub async fn alter_user_scram_credentials(
        &self,
        deletions: Vec<ScramCredentialDeletion>,
        upsertions: Vec<ScramCredentialUpsertion>,
        options: AlterUserScramCredentialsOptions,
    ) -> Result<HashMap<String, Result<()>>> {
        let call = self.call("AlterUserScramCredentials", Mode::Write, options.timeout)?;
        let mut users: Vec<String> = deletions
            .iter()
            .map(|d| d.name.clone())
            .chain(upsertions.iter().map(|u| u.name.clone()))
            .collect();
        users.sort_unstable();
        users.dedup();
        let deletions = &deletions;
        let upsertions = &upsertions;
        Ok(call
            .fan_out(
                users,
                |_| Target::Controller,
                |conn, users| async move {
                    let request = AlterUserScramCredentialsRequest {
                        deletions: deletions
                            .iter()
                            .filter(|d| users.contains(&d.name))
                            .cloned()
                            .collect(),
                        upsertions: upsertions
                            .iter()
                            .filter(|u| users.contains(&u.name))
                            .cloned()
                            .collect(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::AlterUserScramCredentials,
                        versions::ALTER_USER_SCRAM_CREDENTIALS_MIN,
                        versions::ALTER_USER_SCRAM_CREDENTIALS_MAX,
                    )?;
                    let response: AlterUserScramCredentialsResponse =
                        exchange(&conn, ApiKey::AlterUserScramCredentials, version, &request)
                            .await?;
                    Ok(response
                        .results
                        .into_iter()
                        .map(|r| (r.user, answer(r.error_code, r.error_message)))
                        .collect())
                },
            )
            .await)
    }
}

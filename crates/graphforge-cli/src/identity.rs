//! Who a Version is credited to: author and committer resolution (ADR 0061).
//!
//! Each role resolves flag, then environment, then the user configuration file;
//! the committer defaults to the author. Credit is public recorded text, never
//! access control, so it lives in a per-person file and not in the keyring.
use clap::Args;
use graphforge_api::{GfError, ResearchSignature};
use std::io::Write;
use std::path::{Path, PathBuf};

const AUTHOR_ENV: &str = "GF_AUTHOR";
const COMMITTER_ENV: &str = "GF_COMMITTER";
const MAX_CONFIG_BYTES: usize = 64 * 1024;

/// Set or show the credited identity (`gf config user`).
#[derive(Args)]
pub(crate) struct UserArgs {
    /// Credited display name.
    #[arg(long)]
    name: Option<String>,
    /// Credited email address.
    #[arg(long)]
    email: Option<String>,
    /// Credited ORCID iD, `0000-0002-1825-0097`.
    #[arg(long)]
    orcid: Option<String>,
}

/// Author and committer given on the command line.
#[derive(Args, Default)]
pub(crate) struct IdentityFlags {
    /// Credited author, `Name <email>`; defaults to GF_AUTHOR, then `gf config user`.
    #[arg(long)]
    pub author: Option<String>,
    /// Credited committer, `Name <email>`; defaults to GF_COMMITTER, then the author.
    #[arg(long)]
    pub committer: Option<String>,
}

/// The `[user]` and `[committer]` tables of `config.toml`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct UserConfig {
    user: Option<ResearchSignature>,
    committer: Option<ResearchSignature>,
}

/// `graphforge/config.toml` under `$XDG_CONFIG_HOME` (default `~/.config`), or
/// `%APPDATA%` on Windows.
pub(crate) fn config_path() -> Result<PathBuf, GfError> {
    let base = if cfg!(windows) {
        std::env::var_os("APPDATA").map(PathBuf::from)
    } else {
        std::env::var_os("XDG_CONFIG_HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| Path::new(&home).join(".config")))
    };
    base.map(|base| base.join("graphforge").join("config.toml"))
        .ok_or_else(|| GfError::Validation("no user configuration directory is available".into()))
}

/// Parse `Name <email>` or a bare name. ORCID comes only from configuration.
pub(crate) fn parse_signature(text: &str) -> Result<ResearchSignature, GfError> {
    let text = text.trim();
    let (name, email) = match text.rsplit_once('<') {
        Some((name, rest)) => {
            let email = rest.strip_suffix('>').ok_or_else(|| {
                GfError::Validation("a signature must read `Name <email>`".into())
            })?;
            (name.trim(), Some(email.trim().to_owned()))
        }
        None => (text, None),
    };
    let signature = ResearchSignature {
        name: name.to_owned(),
        email,
        orcid: None,
    };
    signature.validate()?;
    Ok(signature)
}

/// Resolve the credited author and committer.
///
/// # Errors
/// `GF_RESEARCH_SIGNATURE_MISSING` when no author is configured anywhere, or a
/// validation error for a malformed value.
pub(crate) fn resolve(
    flags: &IdentityFlags,
    env: &dyn Fn(&str) -> Option<String>,
    config: &UserConfig,
) -> Result<(ResearchSignature, ResearchSignature), GfError> {
    let given = |flag: &Option<String>, variable: &str| {
        flag.clone()
            .or_else(|| env(variable).filter(|value| !value.trim().is_empty()))
            .as_deref()
            .map(parse_signature)
            .transpose()
    };
    let author = given(&flags.author, AUTHOR_ENV)?
        .or_else(|| config.user.clone())
        .ok_or_else(|| {
            GfError::Validation(
                "GF_RESEARCH_SIGNATURE_MISSING: no author is set; pass --author \"Name <email>\" or set GF_AUTHOR, or run `gf config user --name <name> --email <email>`"
                    .into(),
            )
        })?;
    let committer = given(&flags.committer, COMMITTER_ENV)?
        .or_else(|| config.committer.clone())
        .unwrap_or_else(|| author.clone());
    Ok((author, committer))
}

/// Resolve from the process environment and the user configuration file.
pub(crate) fn resolve_from_process(
    flags: &IdentityFlags,
) -> Result<(ResearchSignature, ResearchSignature), GfError> {
    let config = read_config(&config_path()?)?;
    resolve(flags, &|name| std::env::var(name).ok(), &config)
}

pub(crate) fn read_config(path: &Path) -> Result<UserConfig, GfError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(UserConfig::default());
        }
        Err(_) => return Err(GfError::Validation("cannot read config.toml".into())),
    };
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(GfError::Validation("config.toml is too large".into()));
    }
    let text = String::from_utf8(bytes)
        .map_err(|_| GfError::Validation("config.toml is not UTF-8".into()))?;
    parse_config(&text)
}

/// The documented subset of TOML: `[user]` and `[committer]` tables of
/// `name`, `email` and `orcid` string values. Anything else is refused.
fn parse_config(text: &str) -> Result<UserConfig, GfError> {
    #[derive(Default)]
    struct Fields {
        name: Option<String>,
        email: Option<String>,
        orcid: Option<String>,
    }
    let bad = |line: usize, why: &str| {
        GfError::Validation(format!("config.toml line {}: {why}", line + 1))
    };
    let (mut user, mut committer) = (Fields::default(), Fields::default());
    let mut table: Option<&str> = None;
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
        {
            table = Some(match name.trim() {
                "user" => "user",
                "committer" => "committer",
                _ => return Err(bad(index, "unknown table")),
            });
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| bad(index, "expected `key = \"value\"`"))?;
        let value = unquote(value.trim()).ok_or_else(|| bad(index, "expected a quoted string"))?;
        let fields = match table {
            Some("user") => &mut user,
            Some(_) => &mut committer,
            None => return Err(bad(index, "a key must follow a table header")),
        };
        match key.trim() {
            "name" => fields.name = Some(value),
            "email" => fields.email = Some(value),
            "orcid" => fields.orcid = Some(value),
            _ => return Err(bad(index, "unknown key")),
        }
    }
    let finish = |fields: Fields| -> Result<Option<ResearchSignature>, GfError> {
        let Some(name) = fields.name else {
            return Ok(None);
        };
        let signature = ResearchSignature {
            name,
            email: fields.email,
            orcid: fields.orcid,
        };
        signature.validate()?;
        Ok(Some(signature))
    };
    Ok(UserConfig {
        user: finish(user)?,
        committer: finish(committer)?,
    })
}

fn unquote(value: &str) -> Option<String> {
    let inner = value.strip_prefix('"')?.strip_suffix('"')?;
    let mut out = String::new();
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => out.push(match chars.next()? {
                '"' => '"',
                '\\' => '\\',
                _ => return None,
            }),
            '"' => return None,
            c => out.push(c),
        }
    }
    Some(out)
}

fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn render_config(config: &UserConfig) -> String {
    let mut out = String::new();
    for (table, signature) in [("user", &config.user), ("committer", &config.committer)] {
        let Some(signature) = signature else { continue };
        out.push_str(&format!("[{table}]\nname = {}\n", quote(&signature.name)));
        if let Some(email) = &signature.email {
            out.push_str(&format!("email = {}\n", quote(email)));
        }
        if let Some(orcid) = &signature.orcid {
            out.push_str(&format!("orcid = {}\n", quote(orcid)));
        }
        out.push('\n');
    }
    out
}

fn show(signature: &ResearchSignature) -> String {
    match &signature.email {
        Some(email) => format!("{} <{email}>", signature.name),
        None => signature.name.clone(),
    }
}

/// `gf config user`: set the given fields, or print the resolved credit.
pub(crate) fn run_config_user(
    args: &UserArgs,
    json: bool,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    let path = config_path()?;
    let mut config = read_config(&path)?;
    if args.name.is_some() || args.email.is_some() || args.orcid.is_some() {
        let mut user = config.user.take().unwrap_or(ResearchSignature {
            name: String::new(),
            email: None,
            orcid: None,
        });
        if let Some(name) = &args.name {
            user.name.clone_from(name);
        }
        if args.email.is_some() {
            user.email.clone_from(&args.email);
        }
        if args.orcid.is_some() {
            user.orcid.clone_from(&args.orcid);
        }
        user.validate()?;
        config.user = Some(user);
        write_config(&path, &config)?;
    }
    let (author, committer) = resolve(
        &IdentityFlags::default(),
        &|name| std::env::var(name).ok(),
        &config,
    )?;
    if json {
        serde_json::to_writer(
            &mut *output,
            &serde_json::json!({"author": author, "committer": committer}),
        )
        .map_err(|error| GfError::Execution(error.to_string()))?;
        writeln!(output)
    } else {
        writeln!(output, "author: {}", show(&author))
            .and_then(|()| writeln!(output, "committer: {}", show(&committer)))
    }
    .map_err(|error| GfError::Execution(error.to_string()))
}

fn write_config(path: &Path, config: &UserConfig) -> Result<(), GfError> {
    let directory = path
        .parent()
        .ok_or_else(|| GfError::Validation("config.toml has no directory".into()))?;
    std::fs::create_dir_all(directory)
        .map_err(|_| GfError::Validation("cannot create the configuration directory".into()))?;
    // Replace atomically so a crash never leaves a half-written identity.
    let mut staged = tempfile::NamedTempFile::new_in(directory)
        .map_err(|_| GfError::Validation("cannot stage config.toml".into()))?;
    staged
        .write_all(render_config(config).as_bytes())
        .and_then(|()| staged.as_file().sync_all())
        .map_err(|_| GfError::Validation("cannot write config.toml".into()))?;
    staged
        .persist(path)
        .map_err(|_| GfError::Validation("cannot replace config.toml".into()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signature(name: &str, email: Option<&str>) -> ResearchSignature {
        ResearchSignature {
            name: name.into(),
            email: email.map(Into::into),
            orcid: None,
        }
    }

    fn config() -> UserConfig {
        parse_config(
            "[user]\nname = \"File User\"\nemail = \"file@example.org\"\norcid = \"0000-0002-1825-0097\"\n",
        )
        .unwrap()
    }

    #[test]
    fn author_resolves_flag_then_env_then_file() {
        let env = |name: &str| (name == AUTHOR_ENV).then(|| "Env User <env@example.org>".into());
        let flags = IdentityFlags {
            author: Some("Flag User <flag@example.org>".into()),
            committer: None,
        };
        let (author, _) = resolve(&flags, &env, &config()).unwrap();
        assert_eq!(author, signature("Flag User", Some("flag@example.org")));
        let (author, _) = resolve(&IdentityFlags::default(), &env, &config()).unwrap();
        assert_eq!(author, signature("Env User", Some("env@example.org")));
        let (author, _) = resolve(&IdentityFlags::default(), &|_| None, &config()).unwrap();
        assert_eq!(author.name, "File User");
        assert_eq!(author.orcid.as_deref(), Some("0000-0002-1825-0097"));
    }

    #[test]
    fn committer_defaults_to_author() {
        let (author, committer) = resolve(&IdentityFlags::default(), &|_| None, &config()).unwrap();
        assert_eq!(committer, author);
        let env = |name: &str| (name == COMMITTER_ENV).then(|| "Env Committer".into());
        let (author, committer) = resolve(&IdentityFlags::default(), &env, &config()).unwrap();
        assert_eq!(committer, signature("Env Committer", None));
        assert_eq!(author.name, "File User");
    }

    #[test]
    fn missing_author_refuses_with_signature_missing() {
        let error = resolve(&IdentityFlags::default(), &|_| None, &UserConfig::default())
            .unwrap_err()
            .to_string();
        assert!(error.contains("GF_RESEARCH_SIGNATURE_MISSING"), "{error}");
        assert!(error.contains("GF_AUTHOR") && error.contains("gf config user"));
        // A malformed value is a validation error, not a silent skip.
        let flags = IdentityFlags {
            author: Some("Name <not-an-address>".into()),
            committer: None,
        };
        resolve(&flags, &|_| None, &UserConfig::default()).unwrap_err();
    }

    #[test]
    fn config_round_trips_and_refuses_unknown_content() {
        let parsed = config();
        assert_eq!(parse_config(&render_config(&parsed)).unwrap(), parsed);
        parse_config("[other]\nname = \"x\"\n").unwrap_err();
        parse_config("[user]\nnickname = \"x\"\n").unwrap_err();
        parse_config("[user]\nname = x\n").unwrap_err();
    }
}

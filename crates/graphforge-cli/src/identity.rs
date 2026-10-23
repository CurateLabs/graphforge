//! Who a Version is credited to: author and committer resolution (ADR 0061).
//!
//! Each role resolves flag, then environment, then the user configuration file;
//! the committer defaults to the author. Credit is public recorded text, never
//! access control, so it lives in a per-person file and not in the keyring.
use clap::Args;
use graphforge_api::{GfError, ResearchSignature};
use std::io::Write;
use std::path::{Path, PathBuf};
use toml_edit::{DocumentMut, Item, Table, Value};

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
    parse_config(&read_text(path)?.unwrap_or_default())
}

/// The file's text, or `None` when it does not exist.
fn read_text(path: &Path) -> Result<Option<String>, GfError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(GfError::Validation("cannot read config.toml".into())),
    };
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(GfError::Validation("config.toml is too large".into()));
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| GfError::Validation("config.toml is not UTF-8".into()))
}

fn parse_document(text: &str) -> Result<DocumentMut, GfError> {
    text.parse::<DocumentMut>()
        .map_err(|error| GfError::Validation(format!("config.toml is not valid TOML: {error}")))
}

/// The `[user]` and `[committer]` tables of the document; every other key and
/// table is left to the person who wrote it.
fn parse_config(text: &str) -> Result<UserConfig, GfError> {
    let document = parse_document(text)?;
    Ok(UserConfig {
        user: signature_in(&document, "user")?,
        committer: signature_in(&document, "committer")?,
    })
}

fn signature_in(document: &DocumentMut, table: &str) -> Result<Option<ResearchSignature>, GfError> {
    let Some(item) = document.get(table) else {
        return Ok(None);
    };
    let fields = item
        .as_table_like()
        .ok_or_else(|| GfError::Validation(format!("config.toml: [{table}] must be a table")))?;
    let text = |key: &str| -> Result<Option<String>, GfError> {
        fields
            .get(key)
            .map(|value| {
                value.as_str().map(str::to_owned).ok_or_else(|| {
                    GfError::Validation(format!("config.toml: {table}.{key} must be a string"))
                })
            })
            .transpose()
    };
    let Some(name) = text("name")? else {
        return Ok(None);
    };
    let signature = ResearchSignature {
        name,
        email: text("email")?,
        orcid: text("orcid")?,
    };
    signature.validate()?;
    Ok(Some(signature))
}

/// Set the given `[user]` keys in `text`, leaving every other byte of the file,
/// comments and unrelated tables included, as it was.
fn edit_config(text: &str, args: &UserArgs) -> Result<String, GfError> {
    let mut document = parse_document(text)?;
    if document.get("user").is_none() {
        // Comments after the last item stay above the table appended below them.
        let mut table = Table::new();
        let trailing = document.trailing().as_str().unwrap_or_default().to_owned();
        if !trailing.is_empty() {
            let spacing = if trailing.ends_with("\n\n") { "" } else { "\n" };
            table.decor_mut().set_prefix(format!("{trailing}{spacing}"));
            document.set_trailing("");
        }
        document["user"] = Item::Table(table);
    }
    let user = document["user"]
        .as_table_like_mut()
        .ok_or_else(|| GfError::Validation("config.toml: [user] must be a table".into()))?;
    for (key, value) in [
        ("name", &args.name),
        ("email", &args.email),
        ("orcid", &args.orcid),
    ] {
        let Some(value) = value else { continue };
        let mut replacement = Value::from(value.as_str());
        // Keep a trailing comment and the spacing of the line being edited.
        if let Some(old) = user.get(key).and_then(Item::as_value) {
            *replacement.decor_mut() = old.decor().clone();
        }
        user.insert(key, Item::Value(replacement));
    }
    if signature_in(&document, "user")?.is_none() {
        return Err(GfError::Validation(
            "a credited name is required: pass --name".into(),
        ));
    }
    Ok(document.to_string())
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
    let mut text = read_text(&path)?.unwrap_or_default();
    if args.name.is_some() || args.email.is_some() || args.orcid.is_some() {
        text = edit_config(&text, args)?;
        write_text(&path, &text)?;
    }
    let config = parse_config(&text)?;
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

fn write_text(path: &Path, text: &str) -> Result<(), GfError> {
    let directory = path
        .parent()
        .ok_or_else(|| GfError::Validation("config.toml has no directory".into()))?;
    std::fs::create_dir_all(directory)
        .map_err(|_| GfError::Validation("cannot create the configuration directory".into()))?;
    // Replace atomically so a crash never leaves a half-written identity.
    let mut staged = tempfile::NamedTempFile::new_in(directory)
        .map_err(|_| GfError::Validation("cannot stage config.toml".into()))?;
    staged
        .write_all(text.as_bytes())
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

    const COMMENTED: &str = "# Credit for my Versions.\n\n[user]\nname = \"Ada \\\"The Countess\\\" # not a comment\" # my name\nemail = \"ada@example.org\" # old address\n\n# Other tools read this table.\n[editor]\ncommand = \"vi # -p\"   # keep\nargs = [\"-c\", \"set nu\"]\n";

    #[test]
    fn config_edits_keep_comments_escapes_and_unrelated_tables() {
        let parsed = parse_config(COMMENTED).unwrap();
        assert_eq!(
            parsed.user.as_ref().unwrap().name,
            "Ada \"The Countess\" # not a comment"
        );
        let args = UserArgs {
            name: None,
            email: Some("countess@example.org".into()),
            orcid: Some("0000-0002-1825-0097".into()),
        };
        let edited = edit_config(COMMENTED, &args).unwrap();
        // Only the edited keys change: the old comment stays on the edited line,
        // and the name line, the other table and every comment are untouched.
        assert_eq!(
            edited,
            COMMENTED.replace(
                "email = \"ada@example.org\" # old address\n",
                "email = \"countess@example.org\" # old address\norcid = \"0000-0002-1825-0097\"\n",
            )
        );
        let reparsed = parse_config(&edited).unwrap().user.unwrap();
        assert_eq!(reparsed.email.as_deref(), Some("countess@example.org"));
        assert_eq!(reparsed.orcid.as_deref(), Some("0000-0002-1825-0097"));
        assert_eq!(
            edit_config(
                &edited,
                &UserArgs {
                    name: None,
                    email: None,
                    orcid: None
                }
            )
            .unwrap(),
            edited
        );
    }

    #[test]
    fn config_edits_create_the_table_and_refuse_bad_values() {
        let created = edit_config(
            "# mine\n",
            &UserArgs {
                name: Some("Grace Hopper".into()),
                email: None,
                orcid: None,
            },
        )
        .unwrap();
        assert_eq!(created, "# mine\n\n[user]\nname = \"Grace Hopper\"\n");
        assert_eq!(
            parse_config(&created).unwrap().user.unwrap().name,
            "Grace Hopper"
        );
        // A name is required before an email can be credited.
        edit_config(
            "",
            &UserArgs {
                name: None,
                email: Some("a@example.org".into()),
                orcid: None,
            },
        )
        .unwrap_err();
        // Invalid TOML and a wrongly typed key are refused, not skipped.
        parse_config("[user\nname = ").unwrap_err();
        parse_config("[user]\nname = 3\n").unwrap_err();
        parse_config("user = 1\n").unwrap_err();
    }
}

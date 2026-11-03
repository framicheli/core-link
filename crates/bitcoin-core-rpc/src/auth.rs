use std::path::{Path, PathBuf};
use std::sync::RwLock;

/// RPC credentials. `Debug` hides the password, so a `Client` or `Wallet`
/// can be logged.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Auth {
    /// No credentials.
    None,
    /// `-rpcuser`/`-rpcpassword` (or an `-rpcauth` user): user, password.
    UserPass(String, String),
    /// A Bitcoin Core `.cookie` file. Read when the client is built and
    /// again after a 401, so a long-lived `Client` survives a node restart
    /// (Core writes a new cookie on every start).
    Cookie(PathBuf),
}

impl Auth {
    /// Checks that `path` holds a `user:pass` cookie now and returns
    /// `Auth::Cookie(path)`.
    pub fn cookie(path: impl AsRef<Path>) -> std::io::Result<Auth> {
        read_cookie(path.as_ref())?;
        Ok(Auth::Cookie(path.as_ref().to_owned()))
    }
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Auth::None => f.write_str("None"),
            Auth::UserPass(user, _) => f
                .debug_tuple("UserPass")
                .field(user)
                .field(&"<hidden>")
                .finish(),
            Auth::Cookie(path) => f.debug_tuple("Cookie").field(path).finish(),
        }
    }
}

fn read_cookie(path: &Path) -> std::io::Result<(String, String)> {
    let s = std::fs::read_to_string(path)?;
    let (u, p) = s
        .trim()
        .split_once(':')
        .ok_or_else(|| std::io::Error::other("cookie file is not user:pass"))?;
    Ok((u.into(), p.into()))
}

pub(crate) type UserPass = (String, String);

/// The credentials a `Client` sends, shared by its clones and `Wallet`s.
/// Every request reads them; only a 401 with a cookie writes.
pub(crate) struct Credentials {
    auth: Auth,
    current: RwLock<Option<UserPass>>,
}

impl Credentials {
    /// A cookie that can't be read yet is read again on the first 401.
    pub(crate) fn new(auth: Auth) -> Credentials {
        let current = match &auth {
            Auth::None => None,
            Auth::UserPass(u, p) => Some((u.clone(), p.clone())),
            Auth::Cookie(path) => read_cookie(path).ok(),
        };
        Credentials {
            auth,
            current: RwLock::new(current),
        }
    }

    pub(crate) fn auth(&self) -> &Auth {
        &self.auth
    }

    pub(crate) fn current(&self) -> Option<UserPass> {
        self.current.read().unwrap().clone()
    }

    /// After a 401 sent with `sent`: re-reads the cookie and returns the new
    /// credentials if they differ from `sent`. `None` means "give up".
    pub(crate) fn refresh(&self, sent: &Option<UserPass>) -> Option<UserPass> {
        let Auth::Cookie(path) = &self.auth else {
            return None;
        };
        let fresh = read_cookie(path).ok()?;
        if sent.as_ref() == Some(&fresh) {
            return None;
        }
        *self.current.write().unwrap() = Some(fresh.clone());
        Some(fresh)
    }
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.auth.fmt(f)
    }
}

//! A throwaway regtest node for live tests: `bitcoind` from `BITCOIND_EXE`,
//! else `PATH` (or the release's `bitcoin-node` for IPC);
//! a fresh datadir, removed on drop with the process.
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const RPC_USER: &str = "u";
pub const RPC_PASSWORD: &str = "p";

pub struct TestNode {
    proc: Child,
    pub dir: PathBuf,
    pub rpc_port: u16,
    exe: PathBuf,
    args: Vec<String>,
}

impl TestNode {
    /// Starts with `-txindex -blockfilterindex`, no P2P, plus `extra_args`.
    /// Returns at once: callers retry their first RPC until it answers.
    pub fn start(extra_args: &[&str]) -> TestNode {
        let user = format!("-rpcuser={RPC_USER}");
        let pass = format!("-rpcpassword={RPC_PASSWORD}");
        let args: Vec<_> = [user.as_str(), pass.as_str()]
            .into_iter()
            .chain(extra_args.iter().copied())
            .collect();
        TestNode::start_exe(&bitcoind_exe(), &args)
    }

    /// `start` without `-rpcuser`/`-rpcpassword`: Core writes `cookie_path()`.
    pub fn start_cookie(extra_args: &[&str]) -> TestNode {
        TestNode::start_exe(&bitcoind_exe(), extra_args)
    }

    /// `<datadir>/regtest/.cookie`.
    pub fn cookie_path(&self) -> PathBuf {
        self.dir.join("regtest/.cookie")
    }

    /// Kills the process and starts it again on the same datadir and port.
    /// Core writes a new cookie on start.
    pub fn restart(&mut self) {
        let _ = self.proc.kill();
        let _ = self.proc.wait();
        self.proc = spawn(&self.exe, &self.dir, self.rpc_port, &self.args);
    }

    /// Core's multiprocess `bitcoin-node` from the same release, with
    /// `-ipcbind=unix`. `None` before Core 30, which ships no IPC.
    pub fn start_ipc(extra_args: &[&str]) -> Option<TestNode> {
        let exe = bitcoind_exe().canonicalize().ok()?;
        let node = exe.parent()?.parent()?.join("libexec/bitcoin-node");
        let user = format!("-rpcuser={RPC_USER}");
        let pass = format!("-rpcpassword={RPC_PASSWORD}");
        let args: Vec<_> = ["-ipcbind=unix", user.as_str(), pass.as_str()]
            .iter()
            .chain(extra_args)
            .copied()
            .collect();
        node.exists().then(|| TestNode::start_exe(&node, &args))
    }

    /// `<datadir>/regtest/node.sock`, where `-ipcbind=unix` listens.
    pub fn ipc_socket(&self) -> PathBuf {
        self.dir.join("regtest/node.sock")
    }

    fn start_exe(exe: &Path, extra_args: &[&str]) -> TestNode {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // Tests in one process can start in the same nanosecond.
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "bitcoin-rpc-test-node-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let rpc_port = free_port();
        let args: Vec<String> = extra_args.iter().map(|a| a.to_string()).collect();
        let proc = spawn(exe, &dir, rpc_port, &args);
        TestNode {
            proc,
            dir,
            rpc_port,
            exe: exe.to_owned(),
            args,
        }
    }

    pub fn rpc_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.rpc_port)
    }

    /// Blocks until the process exits, e.g. after the `stop` RPC.
    pub fn wait(&mut self) {
        let _ = self.proc.wait();
    }

    /// The last `lines` of `debug.log`, for failure messages.
    pub fn log_tail(&self, lines: usize) -> String {
        log_tail(&self.dir.join("regtest/debug.log"), lines)
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.proc.kill();
        let _ = self.proc.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spawn(exe: &Path, dir: &Path, rpc_port: u16, args: &[String]) -> Child {
    Command::new(exe)
        .arg(format!("-datadir={}", dir.display()))
        .args([
            "-regtest",
            "-txindex",
            "-blockfilterindex",
            "-listen=0",
            "-dnsseed=0",
            "-fixedseeds=0",
            "-printtoconsole=0",
        ])
        .arg(format!("-rpcport={rpc_port}"))
        .args(args)
        .spawn()
        .unwrap_or_else(|e| panic!("{}: {e}", exe.display()))
}

/// `BITCOIND_EXE`, else `bitcoind` from `PATH`.
fn bitcoind_exe() -> PathBuf {
    if let Ok(exe) = std::env::var("BITCOIND_EXE") {
        return exe.into();
    }
    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|p| p.join("bitcoind"))
                .find(|p| p.is_file())
        })
        .expect("bitcoind on PATH or BITCOIND_EXE")
}

/// A port nothing is listening on right now.
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn log_tail(path: &Path, lines: usize) -> String {
    let log = std::fs::read_to_string(path).unwrap_or_default();
    let all: Vec<_> = log.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

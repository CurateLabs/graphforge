//! Real file-lock contention with an injected vault and provider: no OS secrets.
use super::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

struct SharedStore {
    parent: std::path::PathBuf,
    value: Mutex<Option<String>>,
    attempts: mpsc::Sender<()>,
    reads: AtomicUsize,
}
impl Store for SharedStore {
    fn lock(&self, origin: &str) -> Result<Box<dyn StoreLock + '_>, GfError> {
        self.attempts.send(()).unwrap();
        Ok(Box::new(lock_in(&self.parent, origin)?))
    }
    fn read(&self, _: &str) -> Result<Option<String>, SavedCredentialError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.value.lock().unwrap().clone())
    }
    fn write(&self, _: &str, value: &str) -> Result<(), GfError> {
        *self.value.lock().unwrap() = Some(value.into());
        Ok(())
    }
    fn delete(&self, _: &str) -> Result<(), GfError> {
        *self.value.lock().unwrap() = None;
        Ok(())
    }
}
struct Provider {
    calls: AtomicUsize,
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    reply: Mutex<Option<HubResponse>>,
}
impl Transport for Provider {
    fn get(
        &self,
        _: &Url,
        _: Option<u64>,
        _: Option<&str>,
        _: u64,
    ) -> Result<crate::hub_http::HttpResponse, GfError> {
        panic!("unexpected object read")
    }
    fn exchange(&self, _: &HubRequest, _: usize) -> Result<HubResponse, GfError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.started.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
            return Ok(self.reply.lock().unwrap().take().unwrap());
        }
        // A second refresh using the old token would be rejected by WorkOS.
        Ok(reply(400, serde_json::json!({"error":"invalid_grant"})))
    }
}
fn reply(status: u16, body: serde_json::Value) -> HubResponse {
    HubResponse {
        status,
        headers: Default::default(),
        body: serde_json::to_vec(&body).unwrap(),
    }
}
fn granted() -> HubResponse {
    reply(
        200,
        serde_json::json!({"access_token":"new-access","refresh_token":"new-refresh","token_type":"Bearer","expires_in":300}),
    )
}
struct Harness {
    _root: tempfile::TempDir,
    store: Arc<SharedStore>,
    provider: Arc<Provider>,
    attempts: mpsc::Receiver<()>,
    started: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
}
impl Harness {
    fn new(response: HubResponse, expired: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let (attempts_tx, attempts) = mpsc::channel();
        let (started_tx, started) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let store = Arc::new(SharedStore {
            parent: root.path().into(),
            value: Mutex::new(Some(
                serde_json::to_string(&Session {
                    access_token: "old-access".into(),
                    refresh_token: "old-refresh".into(),
                    expires_at: if expired { 1 } else { 1000 },
                })
                .unwrap(),
            )),
            attempts: attempts_tx,
            reads: AtomicUsize::new(0),
        });
        let provider = Arc::new(Provider {
            calls: AtomicUsize::new(0),
            started: started_tx,
            release: Mutex::new(release_rx),
            reply: Mutex::new(Some(response)),
        });
        Self {
            _root: root,
            store,
            provider,
            attempts,
            started,
            release,
        }
    }
    fn wait_for_provider(&self) {
        self.started.recv_timeout(Duration::from_secs(10)).unwrap();
        self.attempts.recv_timeout(Duration::from_secs(10)).unwrap();
        self.assert_locked();
    }
    fn assert_locked(&self) {
        let (_directory, file) = lock_file_in(&self.store.parent, DEFAULT_HUB).unwrap();
        assert!(
            matches!(
                fs4::FileExt::try_lock(&file),
                Err(fs4::TryLockError::WouldBlock)
            ),
            "the provider call must retain the per-Hub lock"
        );
    }
    fn saved(&self) -> Session {
        serde_json::from_str(self.store.value.lock().unwrap().as_ref().unwrap()).unwrap()
    }
}
#[test]
fn concurrent_refresh_reads_rotated_pair_instead_of_deleting_it_on_stale_failure() {
    let harness = Harness::new(granted(), true);
    let store = harness.store.clone();
    let provider = harness.provider.clone();
    let first = std::thread::spawn(move || {
        session(
            store.as_ref(),
            provider.as_ref(),
            &origin(DEFAULT_HUB).unwrap(),
            500,
        )
        .unwrap()
        .unwrap()
        .access_token
    });
    harness.wait_for_provider();
    let store = harness.store.clone();
    let provider = harness.provider.clone();
    let second = std::thread::spawn(move || {
        session(
            store.as_ref(),
            provider.as_ref(),
            &origin(DEFAULT_HUB).unwrap(),
            500,
        )
        .unwrap()
        .unwrap()
        .access_token
    });
    harness
        .attempts
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    assert_eq!(harness.store.reads.load(Ordering::SeqCst), 1);
    harness.assert_locked();
    harness.release.send(()).unwrap();
    assert_eq!(first.join().unwrap(), "new-access");
    assert_eq!(second.join().unwrap(), "new-access");
    assert_eq!(harness.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(harness.saved().refresh_token, "new-refresh");
}
#[test]
fn stale_refresh_failure_cannot_delete_a_new_login() {
    let harness = Harness::new(
        reply(400, serde_json::json!({"error":"invalid_grant"})),
        true,
    );
    let store = harness.store.clone();
    let provider = harness.provider.clone();
    let refresh = std::thread::spawn(move || {
        session(
            store.as_ref(),
            provider.as_ref(),
            &origin(DEFAULT_HUB).unwrap(),
            500,
        )
        .is_err()
    });
    harness.wait_for_provider();
    let store = harness.store.clone();
    let login = std::thread::spawn(move || {
        save(
            store.as_ref(),
            &origin(DEFAULT_HUB).unwrap(),
            granted(),
            500,
        )
        .unwrap()
        .access_token
    });
    harness
        .attempts
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    harness.assert_locked();
    harness.release.send(()).unwrap();
    assert!(refresh.join().unwrap());
    assert_eq!(login.join().unwrap(), "new-access");
    assert_eq!(harness.saved().refresh_token, "new-refresh");
}
#[test]
fn logout_finishes_revocation_and_deletion_before_a_new_login_is_saved() {
    let harness = Harness::new(reply(204, serde_json::Value::Null), false);
    let store = harness.store.clone();
    let provider = harness.provider.clone();
    let logout = std::thread::spawn(move || {
        super::logout(
            store.as_ref(),
            provider.as_ref(),
            &origin(DEFAULT_HUB).unwrap(),
            500,
        )
        .unwrap()
    });
    harness.wait_for_provider();
    let store = harness.store.clone();
    let login = std::thread::spawn(move || {
        save(
            store.as_ref(),
            &origin(DEFAULT_HUB).unwrap(),
            granted(),
            500,
        )
        .unwrap()
        .access_token
    });
    harness
        .attempts
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    harness.assert_locked();
    harness.release.send(()).unwrap();
    logout.join().unwrap();
    assert_eq!(login.join().unwrap(), "new-access");
    assert_eq!(harness.saved().refresh_token, "new-refresh");
}
#[test]
fn credential_locks_are_private_stable_and_independent_per_origin() {
    let root = tempfile::tempdir().unwrap();
    let lock = lock_in(root.path(), DEFAULT_HUB).unwrap();
    let (_directory, same) = lock_file_in(root.path(), DEFAULT_HUB).unwrap();
    assert!(matches!(
        fs4::FileExt::try_lock(&same),
        Err(fs4::TryLockError::WouldBlock)
    ));
    let (_directory, other) = lock_file_in(root.path(), "https://different.example").unwrap();
    fs4::FileExt::try_lock(&other).unwrap();
    let identity = graphforge_filesystem::file_identity(&same).unwrap();
    assert_eq!(same.metadata().unwrap().len(), 0);
    drop(lock);
    fs4::FileExt::try_lock(&same).unwrap();
    assert_eq!(
        graphforge_filesystem::file_identity(&same).unwrap(),
        identity
    );
}
#[cfg(unix)]
#[test]
fn credential_locks_reject_symlinks_hardlinks_and_insecure_permissions() {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    let root = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let directory = root.path().join(".graphforge-cli-auth-locks");
    symlink(other.path(), &directory).unwrap();
    assert!(lock_in(root.path(), DEFAULT_HUB).is_err());
    std::fs::remove_file(&directory).unwrap();
    graphforge_filesystem::create_private_directory(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(lock_in(root.path(), DEFAULT_HUB).is_err());
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let name = lock_name(DEFAULT_HUB);
    let path = directory.join(name);
    let target = other.path().join("file");
    std::fs::write(&target, b"").unwrap();
    symlink(&target, &path).unwrap();
    assert!(lock_in(root.path(), DEFAULT_HUB).is_err());
    std::fs::remove_file(&path).unwrap();
    std::fs::hard_link(&target, &path).unwrap();
    assert!(lock_in(root.path(), DEFAULT_HUB).is_err());
}

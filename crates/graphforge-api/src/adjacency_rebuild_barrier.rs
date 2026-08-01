use graphforge_core::GfError;
use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock, mpsc};
use std::time::Duration;
use uuid::Uuid;
const PHASE: &str = "refresh-begun-before-publication";
const DEADLINE: Duration = Duration::from_secs(5);
struct Armed {
    cookie: Uuid,
    stale: Option<Uuid>,
    phase: mpsc::SyncSender<PathBuf>,
    release: mpsc::Receiver<()>,
    deadline: Duration,
}
enum State {
    Idle(Option<Uuid>),
    Armed(Armed),
    Spent(Uuid),
}
fn state() -> &'static Mutex<State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(State::Idle(None)))
}
pub(crate) fn serial_test_guard() -> MutexGuard<'static, ()> {
    static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
    SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
thread_local! { static PRESENTED: Cell<Option<Uuid>> = const { Cell::new(None) }; }
pub(crate) struct Presentation(Option<Uuid>);
impl Drop for Presentation {
    fn drop(&mut self) {
        PRESENTED.set(self.0);
    }
}
pub(crate) fn present(cookie: Uuid) -> Presentation {
    Presentation(PRESENTED.replace(Some(cookie)))
}
pub(crate) struct Controller {
    cookie: Uuid,
    phase: mpsc::Receiver<PathBuf>,
    release: mpsc::SyncSender<()>,
    deadline: Duration,
}
impl Controller {
    pub(crate) fn arm() -> Result<Self, GfError> {
        Self::arm_for(DEADLINE)
    }
    fn arm_for(deadline: Duration) -> Result<Self, GfError> {
        let cookie = Uuid::now_v7();
        let (phase_tx, phase) = mpsc::sync_channel(1);
        let (release, release_rx) = mpsc::sync_channel(1);
        let mut state = state().lock().expect("adjacency barrier lock poisoned");
        let stale = if let State::Idle(stale) = &*state {
            *stale
        } else {
            return Err(error("mismatched", "controller", "already_armed"));
        };
        *state = State::Armed(Armed {
            cookie,
            stale,
            phase: phase_tx,
            release: release_rx,
            deadline,
        });
        Ok(Self {
            cookie,
            phase,
            release,
            deadline,
        })
    }
    pub(crate) const fn cookie(&self) -> Uuid {
        self.cookie
    }
    pub(crate) fn wait(&self) -> Result<PathBuf, GfError> {
        self.phase.recv_timeout(self.deadline).map_err(|status| {
            error(
                "matched",
                "controller",
                match status {
                    mpsc::RecvTimeoutError::Timeout => "worker_timeout",
                    mpsc::RecvTimeoutError::Disconnected => "worker_disconnected",
                },
            )
        })
    }
    pub(crate) fn release(&self) -> Result<(), GfError> {
        self.release.try_send(()).map_err(|status| {
            error(
                "matched",
                "controller",
                match status {
                    mpsc::TrySendError::Full(()) => "release_replayed",
                    mpsc::TrySendError::Disconnected(()) => "worker_disconnected",
                },
            )
        })
    }
}
impl Drop for Controller {
    fn drop(&mut self) {
        let mut state = state().lock().expect("adjacency barrier lock poisoned");
        if matches!(&*state, State::Armed(value) if value.cookie == self.cookie)
            || matches!(&*state, State::Spent(value) if *value == self.cookie)
        {
            *state = State::Idle(Some(self.cookie));
        }
    }
}
pub(crate) fn hit(staged: &Path) -> Result<(), GfError> {
    let Some(cookie) = PRESENTED.get() else {
        return Ok(());
    };
    let armed = {
        let mut state = state().lock().expect("adjacency barrier lock poisoned");
        if !matches!(&*state, State::Armed(value) if value.cookie == cookie) {
            return Ok(());
        }
        match std::mem::replace(&mut *state, State::Spent(cookie)) {
            State::Armed(value) => value,
            State::Idle(_) | State::Spent(_) => unreachable!(),
        }
    };
    armed
        .phase
        .try_send(staged.to_path_buf())
        .map_err(|_| error("matched", "worker", "controller_disconnected"))?;
    armed
        .release
        .recv_timeout(armed.deadline)
        .map_err(|status| {
            error(
                "matched",
                "worker",
                match status {
                    mpsc::RecvTimeoutError::Timeout => "release_timeout",
                    mpsc::RecvTimeoutError::Disconnected => "controller_disconnected",
                },
            )
        })
}
fn classify(value: Option<&str>) -> &'static str {
    let Some(value) = value else { return "absent" };
    let Ok(cookie) = Uuid::parse_str(value) else {
        return "malformed";
    };
    match &*state().lock().expect("adjacency barrier lock poisoned") {
        State::Armed(value) if value.cookie == cookie => "matched",
        State::Armed(value) if value.stale == Some(cookie) => "stale",
        State::Spent(value) if *value == cookie => "replayed",
        State::Idle(_) | State::Armed(_) | State::Spent(_) => "mismatched",
    }
}
fn error(cookie_state: &str, worker: &str, status: &str) -> GfError {
    GfError::Storage(format!(
        "adjacency test barrier failed: phase={PHASE} cookie_state={cookie_state} worker={worker} status={status}"
    ))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cookie_lifecycle_is_fail_closed_single_use_and_sanitized() {
        let _serial = serial_test_guard();
        assert_eq!(
            (classify(None), classify(Some("bad"))),
            ("absent", "malformed")
        );
        let control = Controller::arm().unwrap();
        let cookie = control.cookie();
        let text = cookie.to_string();
        assert_eq!(classify(Some(&text)), "matched");
        assert_eq!(classify(Some(&Uuid::now_v7().to_string())), "mismatched");
        {
            let _wrong = present(Uuid::now_v7());
            hit(Path::new("wrong")).unwrap();
        }
        let worker = std::thread::spawn(move || {
            let _cookie = present(cookie);
            hit(Path::new("stage"))
        });
        assert_eq!(control.wait().unwrap(), Path::new("stage"));
        assert_eq!(classify(Some(&text)), "replayed");
        control.release().unwrap();
        worker.join().unwrap().unwrap();
        let _replay = present(cookie);
        hit(Path::new("replay")).unwrap();
        drop(control);
        let replacement = Controller::arm().unwrap();
        assert_eq!(classify(Some(&text)), "stale");
        drop(replacement);
        let control = Controller::arm_for(Duration::from_millis(10)).unwrap();
        let cookie = control.cookie();
        let _cookie = present(cookie);
        let diagnostic = hit(Path::new("timeout")).unwrap_err().to_string();
        assert!(diagnostic.contains("status=release_timeout"));
        assert!(!diagnostic.contains(&cookie.to_string()));
    }
}

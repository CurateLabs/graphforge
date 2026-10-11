//! The bulk builder's write-once, barrier, kill-rerun and budget criteria,
//! proven on the public import route (#1965, a slice of #1881).

mod barriers;
mod child;
mod kill_rerun;
mod support;
mod workers;
mod write_once;

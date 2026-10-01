#!/usr/bin/env python3
"""Small source-guard regression tests; no storage engine execution."""

import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "guard", Path(__file__).with_name("check-direct-fsync.py")
)
GUARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GUARD)


class DirectFsyncGuard(unittest.TestCase):
    def test_literals_comments_and_nested_comments_do_not_count(self):
        source = (
            "fn write() { /* .sync_all(); /* .sync(); */ */ "
            'let s = r#".sync_data()"#; // .sync()\n }'
        )
        self.assertEqual(GUARD.sites("caller.rs", source), [])

    def test_new_direct_methods_and_native_calls_are_rejected(self):
        source = (
            "fn write() { file.sync_all()?; ObservedSync::observed_sync_all(&file)?; "
            "unsafe { libc::fsync(fd); } }"
        )
        sites = GUARD.sites("caller.rs", source)
        self.assertEqual(len(sites), 3)
        self.assertTrue(all(GUARD.ALLOWED.get(key, 0) == 0 for key, _ in sites))

    def test_alias_and_function_item_cannot_bypass_guard(self):
        source = (
            "fn write() { Barrier::observed_sync_all(&file)?; "
            "let fence = File::sync_all; fence(&file)?; }"
        )
        self.assertEqual(len(GUARD.sites("caller.rs", source)), 2)

    def test_exception_is_specific_to_the_function_and_method(self):
        path = "crates/graphforge-storage/src/filesystem_admission.rs"
        self.assertEqual(GUARD.ALLOWED[(path, "run_probe", "observed_sync_all")], 1)
        self.assertEqual(GUARD.ALLOWED.get((path, "new_writer", "observed_sync_all"), 0), 0)
        self.assertEqual(GUARD.ALLOWED.get((path, "run_probe", "sync_data"), 0), 0)


if __name__ == "__main__":
    unittest.main()

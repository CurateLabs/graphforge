"""Shared macros for first-party GraphForge rust_library / rust_test targets."""

load("@crates//:defs.bzl", "aliases", "all_crate_deps")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

def gf_rust_library(name, deps = [], compile_data = [], crate_features = [], **kwargs):
    """rust_library wired to crate_universe deps for this package's Cargo.toml."""
    rust_library(
        name = name,
        srcs = kwargs.pop("srcs", native.glob(["src/**/*.rs"])),
        aliases = aliases(),
        compile_data = compile_data,
        crate_features = crate_features,
        edition = "2024",
        proc_macro_deps = all_crate_deps(proc_macro = True),
        visibility = kwargs.pop("visibility", ["//visibility:public"]),
        deps = all_crate_deps(normal = True) + deps,
        **kwargs
    )

def gf_rust_test(name, crate, deps = [], crate_features = [], **kwargs):
    """Unit-test target for a gf_rust_library (cfg(test) modules)."""
    rust_test(
        name = name,
        aliases = aliases(
            normal_dev = True,
            proc_macro_dev = True,
        ),
        crate = crate,
        crate_features = crate_features,
        edition = "2024",
        proc_macro_deps = all_crate_deps(proc_macro_dev = True),
        deps = all_crate_deps(normal_dev = True) + deps,
        **kwargs
    )

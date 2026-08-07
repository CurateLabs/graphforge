"""Shared macros for first-party GraphForge rust_library / rust_test / binary / cdylib targets."""

load("@crates//:defs.bzl", "aliases", "all_crate_deps")
load("@rules_rust//cargo:defs.bzl", "cargo_build_script")
load(
    "@rules_rust//rust:defs.bzl",
    "rust_binary",
    "rust_doc_test",
    "rust_library",
    "rust_shared_library",
    "rust_test",
)

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

def gf_rust_doc_test(name, crate, deps = [], crate_features = [], **kwargs):
    """Documentation tests for a gf_rust_library (`rustdoc --test`).

    `rust_test(crate = ...)` only covers `#[cfg(test)]` modules — doctests are a
    separate pass and must be modeled explicitly so they cannot silently drop
    out of `//:ci_rust_tests`.
    """
    rust_doc_test(
        name = name,
        crate = crate,
        crate_features = crate_features,
        deps = deps,
        **kwargs
    )

def gf_rust_integration_test(
        name,
        srcs,
        crate,
        deps = [],
        data = [],
        crate_features = [],
        rustc_env = {},
        env = {},
        size = "medium",
        timeout = None,
        use_libtest_harness = True,
        crate_root = None,
        **kwargs):
    """Cargo-style integration test (`tests/*.rs`) with hermetic CARGO_MANIFEST_DIR."""
    package_name = native.package_name()
    merged_rustc_env = {"CARGO_MANIFEST_DIR": package_name}
    merged_rustc_env.update(rustc_env)
    test_kwargs = dict(kwargs)
    if timeout != None:
        test_kwargs["timeout"] = timeout
    if crate_root != None:
        test_kwargs["crate_root"] = crate_root
    rust_test(
        name = name,
        srcs = srcs,
        aliases = aliases(
            normal_dev = True,
            proc_macro_dev = True,
        ),
        crate_features = crate_features,
        data = data,
        edition = "2024",
        env = env,
        proc_macro_deps = all_crate_deps(proc_macro = True) + all_crate_deps(proc_macro_dev = True),
        rustc_env = merged_rustc_env,
        size = size,
        use_libtest_harness = use_libtest_harness,
        deps = all_crate_deps(normal = True) + all_crate_deps(normal_dev = True) + [crate] + deps,
        **test_kwargs
    )

def gf_cargo_build_script(name, deps = [], data = [], crate_features = [], **kwargs):
    """cargo_build_script wired to crate_universe build-deps for this package."""
    cargo_build_script(
        name = name,
        srcs = kwargs.pop("srcs", ["build.rs"]),
        aliases = aliases(build = True, build_proc_macro = True),
        crate_features = crate_features,
        data = data,
        edition = "2024",
        proc_macro_deps = all_crate_deps(build_proc_macro = True),
        visibility = kwargs.pop("visibility", ["//visibility:private"]),
        deps = all_crate_deps(build = True) + deps,
        **kwargs
    )

def gf_rust_binary(name, deps = [], data = [], compile_data = [], crate_features = [], **kwargs):
    """rust_binary wired to crate_universe deps for this package's Cargo.toml."""
    rust_binary(
        name = name,
        srcs = kwargs.pop("srcs", native.glob(["src/**/*.rs"])),
        aliases = aliases(),
        compile_data = compile_data,
        crate_features = crate_features,
        data = data,
        edition = "2024",
        proc_macro_deps = all_crate_deps(proc_macro = True),
        visibility = kwargs.pop("visibility", ["//visibility:public"]),
        deps = all_crate_deps(normal = True) + deps,
        **kwargs
    )

def gf_rust_shared_library(name, deps = [], compile_data = [], crate_features = [], **kwargs):
    """rust_shared_library (cdylib) wired to crate_universe deps for this package."""
    rust_shared_library(
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

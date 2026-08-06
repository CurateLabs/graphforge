//! Embed the canonical project-local skill bundle in the Rust-owned CLI.
//!
//! Skill files are copied into `OUT_DIR` before `include_bytes!` so the same
//! build script works under Cargo and Bazel (`cargo_build_script`), where the
//! original tree may only be available as a declared `data` input during the
//! build-script action.

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

fn collect(root: &Path, directory: &Path, files: &mut Vec<PathBuf>) {
    let root_manifest = root.join("manifest.json");
    for entry in fs::read_dir(directory).expect("read canonical project-skills directory") {
        let path = entry.expect("read canonical project-skill entry").path();
        if path.is_dir() {
            collect(root, &path, files);
        } else if path != root_manifest {
            path.strip_prefix(root)
                .expect("project skill remains under canonical root");
            files.push(path);
        }
    }
}

fn rust_string(value: &str) -> String {
    let escaped: String = value.chars().flat_map(char::escape_default).collect();
    format!("\"{escaped}\"")
}

fn project_skills_root(manifest_dir: &Path) -> PathBuf {
    // Bazel `cargo_build_script` sets this to the declared manifest input so the
    // skill tree is resolved from runfiles/data rather than a source-tree walk.
    if let Some(manifest) = env::var_os("GRAPHFORGE_PROJECT_SKILLS_MANIFEST") {
        return PathBuf::from(manifest)
            .parent()
            .expect("project-skills manifest path has a parent directory")
            .to_path_buf();
    }
    manifest_dir.join("../../project-skills")
}

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("out dir"));
    let root = project_skills_root(&manifest_dir);
    println!("cargo:rerun-if-env-changed=GRAPHFORGE_PROJECT_SKILLS_MANIFEST");
    println!("cargo:rerun-if-changed={}", root.display());

    let manifest = root.join("manifest.json");
    let mut files = Vec::new();
    for name in ["graphforge-bootstrap", "graphforge-build-knowledge"] {
        collect(&root, &root.join(name), &mut files);
    }
    files.sort();
    println!("cargo:rerun-if-changed={}", manifest.display());
    for path in &files {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    let embed_root = out_dir.join("project_skills_embed");
    fs::create_dir_all(&embed_root).expect("create embedded project-skills directory");
    let embedded_manifest = embed_root.join("manifest.json");
    fs::copy(&manifest, &embedded_manifest).expect("copy project-skills manifest into OUT_DIR");

    let mut embedded_relatives = Vec::new();
    for path in &files {
        let relative = path
            .strip_prefix(&root)
            .expect("project skill remains under canonical root")
            .to_string_lossy()
            .replace('\\', "/");
        let dest = embed_root.join(&relative);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).expect("create embedded project-skill parent");
        }
        fs::copy(path, &dest).expect("copy project-skill file into OUT_DIR");
        embedded_relatives.push(relative);
    }

    // Prefer paths relative to this generated file so Bazel sandboxed rustc can
    // resolve `include_bytes!` against the cargo_build_script OUT_DIR tree.
    let manifest_literal = rust_string("project_skills_embed/manifest.json");
    let mut generated =
        format!("const PROJECT_SKILL_MANIFEST: &[u8] = include_bytes!({manifest_literal});\n");
    generated
        .push_str("const PROJECT_SKILL_FILES: &[graphforge_api::SkillBundleFile<'static>] = &[\n");
    for relative in embedded_relatives {
        let relative_literal = rust_string(&relative);
        let include_rel = format!("project_skills_embed/{relative}");
        let path_literal = rust_string(&include_rel);
        writeln!(
            generated,
            "    graphforge_api::SkillBundleFile {{ path: {relative_literal}, bytes: include_bytes!({path_literal}) }},"
        )
        .expect("write generated project-skill entry");
    }
    generated.push_str("];\n");
    fs::write(out_dir.join("project_skills.rs"), generated)
        .expect("write embedded project-skill module");
}

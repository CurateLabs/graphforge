//! Embed the canonical project-local skill bundle in the Rust-owned CLI.

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

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let root = manifest_dir.join("../../project-skills");
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

    let manifest_literal = rust_string(&manifest.to_string_lossy());
    let mut generated =
        format!("const PROJECT_SKILL_MANIFEST: &[u8] = include_bytes!({manifest_literal});\n");
    generated
        .push_str("const PROJECT_SKILL_FILES: &[graphforge_api::SkillBundleFile<'static>] = &[\n");
    for path in files {
        let relative = path
            .strip_prefix(&root)
            .expect("project skill remains under canonical root")
            .to_string_lossy()
            .replace('\\', "/");
        let relative_literal = rust_string(&relative);
        let path_literal = rust_string(&path.to_string_lossy());
        writeln!(
            generated,
            "    graphforge_api::SkillBundleFile {{ path: {relative_literal}, bytes: include_bytes!({path_literal}) }},"
        )
        .expect("write generated project-skill entry");
    }
    generated.push_str("];\n");
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").expect("out dir")).join("project_skills.rs"),
        generated,
    )
    .expect("write embedded project-skill module");
}

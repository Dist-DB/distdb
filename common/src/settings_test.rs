use super::*;

#[test]
fn registry_is_sorted_and_unique() {

    for registry in [ALL, IMPORT_ALL] {
        let mut sorted = registry.to_vec();
        sorted.sort_unstable();
        sorted.dedup();

        assert_eq!(sorted.as_slice(), registry, "registries must stay sorted and duplicate free");
    }

}

#[test]
fn registry_names_are_namespaced() {

    for name in ALL {
        assert!(name.starts_with("DISTDB_"), "{name} should be namespaced");
    }

    for name in IMPORT_ALL {
        assert!(name.starts_with("IMPORT_"), "{name} should keep its import prefix");
    }

}

#[test]
fn flag_accepts_documented_truthy_values() {

    // SAFETY: single-threaded test process mutating a name no other test reads.
    for value in ["1", "true", "TRUE", " yes ", "on"] {
        unsafe { std::env::set_var("DISTDB_TEST_FLAG", value) };
        assert!(flag("DISTDB_TEST_FLAG", false), "{value} should be truthy");
    }

    for value in ["0", "false", "no", "off", "maybe"] {
        unsafe { std::env::set_var("DISTDB_TEST_FLAG", value) };
        assert!(!flag("DISTDB_TEST_FLAG", true), "{value} should be falsy");
    }

    unsafe { std::env::remove_var("DISTDB_TEST_FLAG") };
    assert!(flag("DISTDB_TEST_FLAG", true));
    assert!(!flag("DISTDB_TEST_FLAG", false));

}

#[test]
fn numeric_parsers_fall_back_on_unusable_values() {

    unsafe { std::env::set_var("DISTDB_TEST_COUNT", "not-a-number") };
    assert_eq!(positive_usize("DISTDB_TEST_COUNT", 7), 7);

    unsafe { std::env::set_var("DISTDB_TEST_COUNT", "0") };
    assert_eq!(positive_usize("DISTDB_TEST_COUNT", 7), 7);
    assert_eq!(usize_allowing_zero("DISTDB_TEST_COUNT", 7), 0);

    unsafe { std::env::set_var("DISTDB_TEST_COUNT", " 42 ") };
    assert_eq!(positive_usize("DISTDB_TEST_COUNT", 7), 42);

    unsafe { std::env::remove_var("DISTDB_TEST_COUNT") };

}

#[test]
fn text_treats_blank_as_unset() {

    unsafe { std::env::set_var("DISTDB_TEST_TEXT", "   ") };
    assert_eq!(text("DISTDB_TEST_TEXT"), None);

    unsafe { std::env::set_var("DISTDB_TEST_TEXT", " host-a ") };
    assert_eq!(text("DISTDB_TEST_TEXT"), Some("host-a".to_string()));

    unsafe { std::env::remove_var("DISTDB_TEST_TEXT") };

}

/// Guards against a setting being read somewhere in the workspace without being
/// declared here, which is how names previously drifted from scripts and docs.
#[test]
fn every_environment_name_in_the_workspace_is_registered() {

    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("common crate should live inside the workspace")
        .to_path_buf();

    let mut unregistered = Vec::new();
    let mut pending = vec![workspace_root.clone()];

    while let Some(dir) = pending.pop() {

        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };

        for entry in entries.flatten() {

            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();

            if path.is_dir() {
                if !matches!(name.as_ref(), "target" | "artifacts" | "coverage" | ".git") {
                    pending.push(path);
                }
                continue;
            }

            if path.extension().is_none_or(|ext| ext != "rs") {
                continue;
            }

            // The registry itself is where the literals are allowed to live.
            if path.ends_with("settings.rs") || path.ends_with("settings_test.rs") {
                continue;
            }

            let Ok(source) = std::fs::read_to_string(&path) else {
                continue;
            };

            for literal in source.split("env::var(\"").skip(1) {
                let Some(name) = literal.split('"').next() else {
                    continue;
                };

                if !name.starts_with("DISTDB_") && !name.starts_with("IMPORT_") {
                    continue;
                }

                if ALL.contains(&name) || IMPORT_ALL.contains(&name) {
                    continue;
                }

                unregistered.push(format!("{name} in {}", path.display()));
            }
        }
    }

    unregistered.sort();
    unregistered.dedup();

    assert!(
        unregistered.is_empty(),
        "settings read but not declared in common::settings: {unregistered:#?}",
    );

}

//! End-to-end CLI tests over a local corpus: rg-style invocation, output
//! formats, exit codes. Everything runs piped, so defaults are
//! no-heading / no-line-numbers / no color.

use assert_cmd::Command;
use predicates::prelude::*;

struct Corpus {
    dir: tempfile::TempDir,
    index: tempfile::TempDir,
}

fn corpus() -> Corpus {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("alpha.log"),
        "one ERROR here\nclean line\nanother ERROR line\ntail\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("beta.log"),
        "all quiet\nnothing to see\na.b literal\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("gamma.txt"), "ERROR ERROR double\n").unwrap();
    std::fs::write(
        dir.path().join("bundle.zip"),
        holys3_core::testutil::encode::zip(&[
            ("logs/app.log", b"before\nZIPNEEDLE one\nafter\n"),
            ("other.txt", b"ZIPNEEDLE two\n"),
        ]),
    )
    .unwrap();
    let index = tempfile::tempdir().unwrap();
    holys3()
        .args(["index"])
        .arg(dir.path())
        .arg("--out")
        .arg(index.path())
        .assert()
        .success();
    Corpus { dir, index }
}

fn holys3() -> Command {
    Command::cargo_bin("holys3").unwrap()
}

#[test]
fn index_watch_flags_are_paired() {
    let target = tempfile::tempdir().unwrap();
    holys3()
        .arg("index")
        .arg(target.path())
        .arg("--watch")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--interval"));
    holys3()
        .arg("index")
        .arg(target.path())
        .args(["--interval", "1"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--watch"));
    holys3()
        .arg("index")
        .arg(target.path())
        .args(["--watch", "--interval", "0"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("greater than 0"));
}

#[test]
fn index_json_reports_update() {
    let target = tempfile::tempdir().unwrap();
    let index = tempfile::tempdir().unwrap();
    std::fs::write(target.path().join("event.log"), "needle\n").unwrap();
    let output = holys3()
        .arg("index")
        .arg(target.path())
        .arg("--out")
        .arg(index.path())
        .arg("--json")
        .output()
        .unwrap();
    assert!(output.status.success());
    let lines = String::from_utf8(output.stdout).unwrap();
    let events = lines
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 1);
    let event = events.first().unwrap().as_object().unwrap();
    assert_eq!(event.len(), 10);
    assert_eq!(event["type"], "indexed");
    assert_eq!(event["cycle"], 1);
    assert_eq!(event["target"], target.path().to_str().unwrap());
    assert!(event["duration_ms"].as_u64().is_some());
    assert_eq!(event["added"], 1);
    assert_eq!(event["removed"], 0);
    assert_eq!(event["total_docs"], 1);
    assert_eq!(event["segments"], 1);
    assert_eq!(event["compacted"], false);
    assert_eq!(event["up_to_date"], false);
}

fn search(c: &Corpus) -> Command {
    let mut cmd = holys3();
    cmd.arg("ERROR")
        .arg(c.dir.path())
        .arg("--index")
        .arg(c.index.path());
    cmd
}

fn search_pattern(c: &Corpus, pattern: &str) -> Command {
    let mut cmd = holys3();
    cmd.arg(pattern)
        .arg(c.dir.path())
        .arg("--index")
        .arg(c.index.path());
    cmd
}

fn key(c: &Corpus, name: &str) -> String {
    // Index keys are canonicalized at build time and /-separated on every
    // platform
    c.dir
        .path()
        .canonicalize()
        .unwrap()
        .join(name)
        .display()
        .to_string()
        .replace('\\', "/")
}

fn sorted_lines(output: &[u8]) -> Vec<String> {
    let mut lines: Vec<String> = String::from_utf8_lossy(output)
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

#[test]
fn exit_codes() {
    let c = corpus();
    search(&c).assert().success();
    search_pattern(&c, "NO_SUCH_TOKEN").assert().code(1);
    search_pattern(&c, "(").assert().code(2);
    holys3().args(["x", "s3://"]).assert().code(2);
    holys3().assert().code(2); // missing args (clap usage error)
}

#[test]
fn object_cache_flags_are_paired_and_s3_only() {
    holys3()
        .args([
            "needle",
            "s3://bucket",
            "--object-cache",
            "/tmp/holys3-cache",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--object-cache-cap"));
    holys3()
        .args(["needle", "s3://bucket", "--object-cache-cap", "1024"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--object-cache"));
    holys3()
        .args([
            "needle",
            "s3://bucket",
            "--object-cache",
            "/tmp/holys3-cache",
            "--object-cache-cap",
            "0",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("greater than 0"));

    let c = corpus();
    search_pattern(&c, "ERROR")
        .args([
            "--object-cache",
            "/tmp/holys3-cache",
            "--object-cache-cap",
            "1024",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("only applies to s3://"));
}

#[test]
fn prints_object_cache_help() {
    holys3()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Cache immutable S3 source bodies under DIR",
        ))
        .stdout(predicate::str::contains(
            "Limit the source-object cache to BYTES",
        ));
}

#[test]
fn piped_default_format_and_flags() {
    let c = corpus();
    let out = search(&c).output().unwrap();
    assert_eq!(
        sorted_lines(&out.stdout),
        vec![
            format!("{}:another ERROR line", key(&c, "alpha.log")),
            format!("{}:one ERROR here", key(&c, "alpha.log")),
            format!("{}:ERROR ERROR double", key(&c, "gamma.txt")),
        ]
    );
    let out = search(&c).arg("-n").output().unwrap();
    assert!(
        sorted_lines(&out.stdout).contains(&format!("{}:1:one ERROR here", key(&c, "alpha.log")))
    );
    let out = search(&c).args(["--column"]).output().unwrap();
    assert!(
        sorted_lines(&out.stdout).contains(&format!("{}:1:5:one ERROR here", key(&c, "alpha.log")))
    );
    // heading mode groups under a key header
    let out = search_pattern(&c, "one ERROR")
        .arg("--heading")
        .arg("-N")
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        format!("{}\none ERROR here\n", key(&c, "alpha.log"))
    );
}

#[test]
fn context_rendering() {
    let c = corpus();
    // alpha.log lines: 1 ERROR, 2 clean, 3 ERROR, 4 tail -> -C1 merges all
    let out = search_pattern(&c, "ERROR")
        .args(["-C", "1", "-g", "alpha.log", "-n"])
        .output()
        .unwrap();
    let k = key(&c, "alpha.log");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        format!("{k}:1:one ERROR here\n{k}-2-clean line\n{k}:3:another ERROR line\n{k}-4-tail\n")
    );
    let out = search_pattern(&c, "another")
        .args(["-B", "1", "-g", "alpha.log", "-n"])
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        format!("{k}-2-clean line\n{k}:3:another ERROR line\n")
    );
}

#[test]
fn pattern_semantics() {
    let c = corpus();
    // -F: a.b is literal, must not match "all" via the dot
    let out = search_pattern(&c, "a.b")
        .arg("-F")
        .arg("-l")
        .output()
        .unwrap();
    assert_eq!(sorted_lines(&out.stdout), vec![key(&c, "beta.log")]);
    // -i
    search_pattern(&c, "error").assert().code(1);
    search_pattern(&c, "error").arg("-i").assert().success();
    // -S: lowercase pattern is insensitive, capitalized is not
    search_pattern(&c, "error").arg("-S").assert().success();
    search_pattern(&c, "Error").arg("-S").assert().code(1);
    // -w
    search_pattern(&c, "ERRO").assert().success();
    search_pattern(&c, "ERRO").arg("-w").assert().code(1);
    // multiple -e OR
    let mut cmd = holys3();
    cmd.args(["-e", "quiet", "-e", "tail"])
        .arg(c.dir.path())
        .arg("--index")
        .arg(c.index.path())
        .arg("-l");
    let out = cmd.output().unwrap();
    assert_eq!(
        sorted_lines(&out.stdout),
        vec![key(&c, "alpha.log"), key(&c, "beta.log")]
    );
    // -m 1 caps matching lines per doc
    let out = search(&c)
        .args(["-m", "1", "-g", "alpha.log"])
        .output()
        .unwrap();
    assert_eq!(sorted_lines(&out.stdout).len(), 1);
}

#[test]
fn modes() {
    let c = corpus();
    let out = search(&c).arg("-l").output().unwrap();
    assert_eq!(
        sorted_lines(&out.stdout),
        vec![key(&c, "alpha.log"), key(&c, "gamma.txt")]
    );
    // -c counts lines; --count-matches counts occurrences
    let out = search(&c).arg("-c").output().unwrap();
    assert_eq!(
        sorted_lines(&out.stdout),
        vec![
            format!("{}:2", key(&c, "alpha.log")),
            format!("{}:1", key(&c, "gamma.txt"))
        ]
    );
    let out = search(&c).arg("--count-matches").output().unwrap();
    assert_eq!(
        sorted_lines(&out.stdout),
        vec![
            format!("{}:2", key(&c, "alpha.log")),
            format!("{}:2", key(&c, "gamma.txt"))
        ]
    );
    // -q: silent, exit by match presence
    let out = search(&c).arg("-q").output().unwrap();
    assert!(out.stdout.is_empty());
    assert!(out.status.success());
    search_pattern(&c, "NO_SUCH_TOKEN")
        .arg("-q")
        .assert()
        .code(1);
}

#[test]
fn glob_filters() {
    let c = corpus();
    let out = search(&c).args(["-l", "-g", "*.txt"]).output().unwrap();
    assert_eq!(sorted_lines(&out.stdout), vec![key(&c, "gamma.txt")]);
    let out = search(&c).args(["-l", "-g", "!*.txt"]).output().unwrap();
    assert_eq!(sorted_lines(&out.stdout), vec![key(&c, "alpha.log")]);
}

#[test]
fn json_wire_format() {
    let c = corpus();
    let out = search(&c).arg("--json").output().unwrap();
    let lines: Vec<serde_json::Value> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.last().unwrap()["type"], "summary");
    let types: Vec<&str> = lines.iter().map(|v| v["type"].as_str().unwrap()).collect();
    assert!(types.contains(&"begin") && types.contains(&"match") && types.contains(&"end"));
    let m = lines.iter().find(|v| v["type"] == "match").unwrap();
    let data = &m["data"];
    assert!(data["line_number"].as_u64().is_some());
    let text = data["lines"]["text"].as_str().unwrap();
    let sub = &data["submatches"][0];
    let (s, e) = (
        sub["start"].as_u64().unwrap() as usize,
        sub["end"].as_u64().unwrap() as usize,
    );
    assert_eq!(&text[s..e], sub["match"]["text"].as_str().unwrap());
    let summary = &lines.last().unwrap()["data"]["stats"];
    assert_eq!(summary["matched_lines"].as_u64().unwrap(), 3);
    assert_eq!(summary["matches"].as_u64().unwrap(), 4);
}

#[test]
fn archive_members_support_output_modes_and_globs() {
    let c = corpus();
    let bundle = key(&c, "bundle.zip");
    let app = format!("{bundle}!/logs/app.log");
    let other = format!("{bundle}!/other.txt");

    let out = search_pattern(&c, "ZIPNEEDLE").arg("-l").output().unwrap();
    assert_eq!(sorted_lines(&out.stdout), [app.clone(), other]);

    let out = search_pattern(&c, "ZIPNEEDLE")
        .args(["-g", "*.log", "-n", "-C", "1"])
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        format!("{app}-1-before\n{app}:2:ZIPNEEDLE one\n{app}-3-after\n")
    );

    let out = search_pattern(&c, "ZIPNEEDLE")
        .args(["-g", "*.log", "-c"])
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&out.stdout), format!("{app}:1\n"));

    let out = search_pattern(&c, "ZIPNEEDLE")
        .args(["-g", "*.log", "--json"])
        .output()
        .unwrap();
    let paths = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let value = serde_json::from_str::<serde_json::Value>(line).unwrap();
            (value["type"] == "match")
                .then(|| value["data"]["path"]["text"].as_str().unwrap().to_owned())
        })
        .collect::<Vec<_>>();
    assert_eq!(paths, [app]);
}

#[test]
fn color_control() {
    let c = corpus();
    let out = search(&c).args(["--color", "always"]).output().unwrap();
    assert!(String::from_utf8_lossy(&out.stdout).contains("\x1b["));
    let out = search(&c).output().unwrap();
    assert!(!String::from_utf8_lossy(&out.stdout).contains("\x1b["));
}

#[test]
fn anchors_match_at_line_boundaries() {
    // rg semantics: ^/$ anchor at every line, not just the object edges
    let c = corpus();
    let out = search_pattern(&c, "^tail").arg("-l").output().unwrap();
    assert_eq!(sorted_lines(&out.stdout), vec![key(&c, "alpha.log")]);
    let out = search_pattern(&c, "ERROR line$")
        .arg("-l")
        .output()
        .unwrap();
    assert_eq!(sorted_lines(&out.stdout), vec![key(&c, "alpha.log")]);
    search_pattern(&c, "^nowhere$").assert().code(1);
}

#[test]
fn max_count_zero_yields_nothing_in_every_mode() {
    let c = corpus();
    for flags in [
        vec!["-m", "0"],
        vec!["-m", "0", "-l"],
        vec!["-m", "0", "-q"],
        vec!["-m", "0", "-c"],
    ] {
        let out = search(&c).args(&flags).output().unwrap();
        assert!(out.stdout.is_empty(), "stdout not empty for {flags:?}");
        assert_eq!(out.status.code(), Some(1), "exit code for {flags:?}");
    }
}

#[test]
fn json_emits_context_messages() {
    let c = corpus();
    let out = search_pattern(&c, "another")
        .args(["--json", "-C", "1"])
        .output()
        .unwrap();
    let types: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert!(
        types.contains(&"context".to_owned()),
        "no context messages: {types:?}"
    );
}

#[test]
fn nonexistent_local_target_errors() {
    let c = corpus();
    let mut cmd = holys3();
    cmd.arg("ERROR")
        .arg("/definitely/not/a/real/dir")
        .arg("--index")
        .arg(c.index.path());
    cmd.assert().code(2);
}

#[test]
fn local_index_cannot_contain_target() {
    let target = tempfile::tempdir().unwrap();
    std::fs::write(target.path().join("source.log"), "needle\n").unwrap();
    holys3()
        .arg("index")
        .arg(target.path())
        .arg("--out")
        .arg(target.path())
        .assert()
        .code(2)
        .stderr(predicate::str::contains("must not contain the target"));
}

#[test]
fn local_target_does_not_escape_directory() {
    let indexed = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let index = tempfile::tempdir().unwrap();
    std::fs::write(indexed.path().join("outside.log"), "needle\n").unwrap();
    std::fs::write(target.path().join("inside.log"), "clean\n").unwrap();
    holys3()
        .arg("index")
        .arg(indexed.path())
        .arg("--out")
        .arg(index.path())
        .assert()
        .success();
    holys3()
        .arg("needle")
        .arg(target.path())
        .arg("--index")
        .arg(index.path())
        .arg("-l")
        .assert()
        .code(1)
        .stdout(predicate::str::is_empty());
}

#[test]
fn local_index_detects_same_size_same_mtime_rewrite() {
    let data = tempfile::tempdir().unwrap();
    let index = tempfile::tempdir().unwrap();
    let path = data.path().join("event.log");
    std::fs::write(&path, "alpha\n").unwrap();
    holys3()
        .arg("index")
        .arg(data.path())
        .arg("--out")
        .arg(index.path())
        .assert()
        .success();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    std::fs::write(&path, "bravo\n").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(modified))
        .unwrap();
    holys3()
        .arg("index")
        .arg(data.path())
        .arg("--out")
        .arg(index.path())
        .assert()
        .success();
    holys3()
        .arg("bravo")
        .arg(data.path())
        .arg("--index")
        .arg(index.path())
        .assert()
        .success();
}

#[test]
fn literal_newline_in_pattern_is_rejected() {
    let c = corpus();
    search_pattern(&c, "ERROR\ntail").assert().code(2);
}

#[test]
fn broken_pipe_is_not_an_error() {
    let c = corpus();
    let assert = search(&c).assert();
    assert.success().stdout(predicate::str::contains("ERROR"));
}

use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use serde::Serialize;

use mls_playground::staircase_runner::{
    aggregate_csv, parse_worker_layout, run_dir_for, validate_run_id,
};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    run_dir: Option<String>,

    #[arg(long)]
    run_id: Option<String>,

    #[arg(long, default_value = "benchmark_output")]
    output_dir: String,

    #[arg(long)]
    layout_file: Option<String>,

    #[arg(long)]
    workers_file: Option<String>,

    #[arg(long)]
    allow_partial: bool,

    #[arg(long)]
    manifest: Option<String>,
}

#[derive(Serialize)]
struct AggregationManifest {
    run_id: Option<String>,
    started_at: String,
    finished_at: String,
    aggregation_mode: String,
    aggregation_status: String,
    events_written: u64,
    input_files_expected: usize,
    input_files_found: usize,
    input_files_missing: Vec<String>,
    malformed_records: Vec<serde_json::Value>,
    truncated_records: Vec<serde_json::Value>,
    runner_events_included: bool,
    output_path: String,
    error_message: Option<String>,
    temporary_file_retained: Option<String>,
}

fn timestamp_now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

fn main() -> Result<()> {
    let args = <Args as Parser>::parse();

    let run_dir: PathBuf = if let Some(dir) = &args.run_dir {
        PathBuf::from(dir)
    } else {
        let run_id = args
            .run_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Either --run-dir or --run-id must be provided"))?
            .clone();
        validate_run_id(&run_id)?;
        run_dir_for(&args.output_dir, &run_id)
    };

    let started_at = timestamp_now();

    if !run_dir.exists() {
        return Err(anyhow::anyhow!(
            "Run directory does not exist: {}",
            run_dir.display()
        ));
    }

    let layout = if let Some(layout_path) = &args.layout_file {
        Some(parse_worker_layout(&PathBuf::from(layout_path))?)
    } else {
        let default_path = run_dir.join("worker_layout.json");
        if default_path.exists() {
            Some(parse_worker_layout(&default_path)?)
        } else {
            None
        }
    };

    let worker_ids: Vec<String> = if let Some(workers_file) = &args.workers_file {
        let content = std::fs::read_to_string(workers_file)
            .with_context(|| format!("Failed to read workers file '{}'", workers_file))?;
        let mut ids = Vec::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((id, _)) = line.split_once('=') {
                ids.push(id.to_string());
            }
        }
        ids
    } else {
        let mut ids: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(&run_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(client_id) = name_str
                .strip_prefix("client-")
                .and_then(|s| s.strip_suffix(".jsonl"))
            {
                ids.push(client_id.to_string());
            }
        }
        ids.sort();
        ids
    };

    let expected_count = worker_ids.len();
    let mut found_count: usize = 0;
    let mut missing_files: Vec<String> = Vec::new();
    for id in &worker_ids {
        let path = run_dir.join(format!("client-{}.jsonl", id));
        if path.exists() {
            found_count += 1;
        } else {
            missing_files.push(id.clone());
        }
    }

    if worker_ids.is_empty() {
        return Err(anyhow::anyhow!(
            "No workers found in {}",
            if args.workers_file.is_some() {
                format!("workers file")
            } else {
                format!("run directory {}", run_dir.display())
            }
        ));
    }

    let events_path = run_dir.join("events.csv");
    let tmp_path = run_dir.join("events.csv.tmp");

    let agg_result = aggregate_csv(&run_dir, &worker_ids, &layout);

    let finished_at = timestamp_now();
    let mut manifest = AggregationManifest {
        run_id: args.run_id.clone(),
        started_at: started_at.clone(),
        finished_at: finished_at.clone(),
        aggregation_mode: if args.allow_partial {
            "partial".to_string()
        } else {
            "strict".to_string()
        },
        aggregation_status: String::new(),
        events_written: 0,
        input_files_expected: expected_count,
        input_files_found: found_count,
        input_files_missing: missing_files,
        malformed_records: Vec::new(),
        truncated_records: Vec::new(),
        runner_events_included: run_dir.join("runner_events.jsonl").exists(),
        output_path: events_path.to_string_lossy().to_string(),
        error_message: None,
        temporary_file_retained: None,
    };

    match agg_result {
        Ok(()) => {
            manifest.aggregation_status = if args.allow_partial {
                "partial_success".to_string()
            } else {
                "complete_success".to_string()
            };
            println!("Aggregation complete: {}", events_path.display());
        }
        Err(e) => {
            if args.allow_partial {
                manifest.aggregation_status = "partial_failure".to_string();
                manifest.error_message = Some(format!("{:#}", e));
                if tmp_path.exists() {
                    manifest.temporary_file_retained = Some(tmp_path.to_string_lossy().to_string());
                }
                eprintln!("[aggregate] Partial failure (--allow-partial): {:#}", e);
            } else {
                manifest.aggregation_status = "failed".to_string();
                manifest.error_message = Some(format!("{:#}", e));
                let manifest_path_clone = args
                    .manifest
                    .as_ref()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| run_dir.join("aggregation_manifest.json"));
                if let Ok(json) = serde_json::to_string_pretty(&manifest) {
                    let _ = fs::write(&manifest_path_clone, json);
                }
                return Err(e);
            }
        }
    }

    if tmp_path.exists() && !events_path.exists() {
        let _ = fs::rename(&tmp_path, &events_path);
    }

    let manifest_path = args
        .manifest
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| run_dir.join("aggregation_manifest.json"));
    if let Ok(json) = serde_json::to_string_pretty(&manifest) {
        let _ = fs::write(&manifest_path, json);
        println!("Manifest written: {}", manifest_path.display());
    }

    Ok(())
}

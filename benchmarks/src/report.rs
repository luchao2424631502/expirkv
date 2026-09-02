//! Strict five-run median aggregation and dependency-free Markdown/SVG output.

use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use crate::{BackendKind, BenchMode, CsvError, CsvFile, CsvRow, Workload, require_exact_matrix};

#[derive(Clone, Debug, PartialEq)]
pub struct SummaryRow {
    pub workload: Workload,
    pub thread_count: usize,
    pub rustkv_ops_per_second: f64,
    pub leveldb_ops_per_second: f64,
    pub rustkv_p50_us: f64,
    pub leveldb_p50_us: f64,
    pub rustkv_p95_us: f64,
    pub leveldb_p95_us: f64,
    pub rustkv_p99_us: f64,
    pub leveldb_p99_us: f64,
    pub rustkv_records_per_second: Option<f64>,
    pub leveldb_records_per_second: Option<f64>,
}

impl SummaryRow {
    pub fn throughput_ratio(&self) -> f64 {
        self.rustkv_ops_per_second / self.leveldb_ops_per_second
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReportSummary {
    pub mode: BenchMode,
    pub environment_id: String,
    pub rustkv_commit: String,
    pub leveldb_commit: String,
    pub rows: Vec<SummaryRow>,
}

pub fn summarize_formal(rows: &[CsvRow]) -> Result<ReportSummary, ReportError> {
    require_exact_matrix(rows, BenchMode::Formal)?;
    summarize(rows, BenchMode::Formal, 5, &[1, 10, 100, 1_000])
}

pub fn summarize_smoke(rows: &[CsvRow]) -> Result<ReportSummary, ReportError> {
    require_exact_matrix(rows, BenchMode::Smoke)?;
    summarize(rows, BenchMode::Smoke, 1, &[1, 10])
}

pub fn generate_formal_report(
    csv_path: impl AsRef<Path>,
    output_directory: impl AsRef<Path>,
) -> Result<PathBuf, ReportError> {
    generate_report(
        csv_path.as_ref(),
        output_directory.as_ref(),
        BenchMode::Formal,
    )
}

pub fn generate_smoke_report(
    csv_path: impl AsRef<Path>,
    output_directory: impl AsRef<Path>,
) -> Result<PathBuf, ReportError> {
    generate_report(
        csv_path.as_ref(),
        output_directory.as_ref(),
        BenchMode::Smoke,
    )
}

fn generate_report(
    csv_path: &Path,
    output_directory: &Path,
    mode: BenchMode,
) -> Result<PathBuf, ReportError> {
    if !csv_path.is_absolute() || !output_directory.is_absolute() {
        return Err(ReportError::InvalidPath(
            "report CSV and output paths must be absolute".to_owned(),
        ));
    }
    if output_directory.exists() {
        return Err(ReportError::OutputExists(output_directory.to_path_buf()));
    }
    let csv = CsvFile::load(csv_path)?;
    let summary = match mode {
        BenchMode::Formal => summarize_formal(csv.rows())?,
        BenchMode::Smoke => summarize_smoke(csv.rows())?,
    };
    fs::create_dir(output_directory)
        .map_err(|error| io_error("create report directory", output_directory, error))?;
    let report_directory = fs::canonicalize(output_directory)
        .map_err(|error| io_error("canonicalize report directory", output_directory, error))?;
    let csv_path = fs::canonicalize(csv_path)
        .map_err(|error| io_error("canonicalize raw CSV", csv_path, error))?;
    let csv_link = relative_path(&report_directory, &csv_path)?;
    for workload in Workload::ALL {
        let rows = summary
            .rows
            .iter()
            .filter(|row| row.workload == workload)
            .collect::<Vec<_>>();
        let path = report_directory.join(format!("{}.svg", workload.as_str()));
        write_synced(&path, render_svg(workload, &rows))?;
    }
    let report_path = report_directory.join("report.md");
    write_synced(&report_path, render_markdown(&summary, &csv_link))?;
    File::open(&report_directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync report directory", &report_directory, error))?;
    Ok(report_path)
}

fn summarize(
    rows: &[CsvRow],
    mode: BenchMode,
    repetitions: usize,
    thread_counts: &[usize],
) -> Result<ReportSummary, ReportError> {
    let first = rows.first().ok_or(ReportError::EmptyInput)?;
    let mut summary_rows = Vec::with_capacity(Workload::ALL.len() * thread_counts.len());
    for workload in Workload::ALL {
        for thread_count in thread_counts {
            let rustkv = select(rows, BackendKind::RustKv, workload, *thread_count);
            let leveldb = select(rows, BackendKind::LevelDb, workload, *thread_count);
            if rustkv.len() != repetitions || leveldb.len() != repetitions {
                return Err(ReportError::RepetitionCount {
                    workload,
                    thread_count: *thread_count,
                    expected: repetitions,
                    rustkv: rustkv.len(),
                    leveldb: leveldb.len(),
                });
            }
            let auxiliary = matches!(
                workload,
                Workload::RangeScan | Workload::BatchPut | Workload::BatchDelete
            );
            summary_rows.push(SummaryRow {
                workload,
                thread_count: *thread_count,
                rustkv_ops_per_second: median_required(&rustkv, |row| row.ops_per_second)?,
                leveldb_ops_per_second: median_required(&leveldb, |row| row.ops_per_second)?,
                rustkv_p50_us: median_required(&rustkv, |row| row.p50_latency_us)?,
                leveldb_p50_us: median_required(&leveldb, |row| row.p50_latency_us)?,
                rustkv_p95_us: median_required(&rustkv, |row| row.p95_latency_us)?,
                leveldb_p95_us: median_required(&leveldb, |row| row.p95_latency_us)?,
                rustkv_p99_us: median_required(&rustkv, |row| row.p99_latency_us)?,
                leveldb_p99_us: median_required(&leveldb, |row| row.p99_latency_us)?,
                rustkv_records_per_second: auxiliary
                    .then(|| median_required(&rustkv, |row| row.records_per_second))
                    .transpose()?,
                leveldb_records_per_second: auxiliary
                    .then(|| median_required(&leveldb, |row| row.records_per_second))
                    .transpose()?,
            });
        }
    }
    Ok(ReportSummary {
        mode,
        environment_id: first.environment_id.clone(),
        rustkv_commit: first.rustkv_commit.clone(),
        leveldb_commit: first.leveldb_commit.clone(),
        rows: summary_rows,
    })
}

fn select(
    rows: &[CsvRow],
    backend: BackendKind,
    workload: Workload,
    thread_count: usize,
) -> Vec<&CsvRow> {
    rows.iter()
        .filter(|row| {
            row.backend == backend && row.workload == workload && row.thread_count == thread_count
        })
        .collect()
}

fn median_required(
    rows: &[&CsvRow],
    select: impl Fn(&CsvRow) -> Option<f64>,
) -> Result<f64, ReportError> {
    let mut values = rows
        .iter()
        .map(|row| select(row).ok_or_else(|| ReportError::MissingMetric(row.run_id.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    if values.iter().any(|value| !value.is_finite()) {
        return Err(ReportError::NonFiniteMetric);
    }
    values.sort_by(f64::total_cmp);
    Ok(values[values.len() / 2])
}

fn render_markdown(summary: &ReportSummary, csv_link: &Path) -> String {
    let mode = match summary.mode {
        BenchMode::Formal => "正式",
        BenchMode::Smoke => "Smoke（非正式性能结果）",
    };
    let configuration = match summary.mode {
        BenchMode::Formal => {
            "数据记录 10,000,000；Key 为 8 字节全零 namespace 加 8 字节大端编号；Value 为固定种子生成的相同确定性 1 KiB 内容；点查 Uniform 且 100% 命中；Range 100 条；Batch 100 条；并发 1/10/100/1000；每单元重复 5 次；`sync=false`；压缩关闭；随机种子 20260720；write buffer 4 MiB；block cache 8 MiB；block size 4 KiB；restart interval 16；max open files 1000；max table file size 2 MiB。"
        }
        BenchMode::Smoke => {
            "非正式小配置：数据记录 1,000；点查请求 100；Range 请求 20；Key 16 Bytes；Value 1 KiB；Range 100 条；Batch 100 条；并发仅 1/10；每单元 1 次；其余 Backend 对应配置与正式模式一致。"
        }
    };
    let execution_model = "每个 RunUnit 使用独立新目录，直接执行 Load → 关闭 → 重开初始验证 → 关闭 → 独立打开 Run；只有 Barrier 释放后的 Run 请求计时。读取负载在 Run 前完整顺序预热，写入和删除不做额外操作预热；Run 后关闭重开并全量验证终态。不使用模板、COW 克隆或物理目录复制。";
    let mut output = format!(
        "# RustKV 与 LevelDB 性能对比报告\n\n\
         - 模式：{mode}\n\
         - 环境 ID：`{}`（Mac 环境详情由该 ID 对应的环境记录提供）\n\
         - RustKV commit：`{}`\n\
         - LevelDB commit：`{}`\n\
         - 主吞吐量单位：`ops/s`\n\
         - 延迟单位：`us/请求`\n\n\
         ## 固定配置\n\n\
         {configuration}\n\n\
         ## 执行模型与计时边界\n\n\
         {execution_model}\n\n",
        summary.environment_id, summary.rustkv_commit, summary.leveldb_commit,
    );
    for workload in Workload::ALL {
        output.push_str(&format!(
            "## {}\n\n![{} throughput]({}.svg)\n\n",
            workload.as_str(),
            workload.as_str(),
            workload.as_str()
        ));
        output.push_str("| 并发 | RustKV ops/s | LevelDB ops/s | RustKV/LevelDB | RustKV P50 us/请求 | LevelDB P50 us/请求 | RustKV P95 us/请求 | LevelDB P95 us/请求 | RustKV P99 us/请求 | LevelDB P99 us/请求 | RustKV records/s | LevelDB records/s |\n");
        output.push_str("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for row in summary.rows.iter().filter(|row| row.workload == workload) {
            output.push_str(&format!(
                "| {} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} | {} | {} |\n",
                row.thread_count,
                row.rustkv_ops_per_second,
                row.leveldb_ops_per_second,
                row.throughput_ratio(),
                row.rustkv_p50_us,
                row.leveldb_p50_us,
                row.rustkv_p95_us,
                row.leveldb_p95_us,
                row.rustkv_p99_us,
                row.leveldb_p99_us,
                format_auxiliary(row.rustkv_records_per_second),
                format_auxiliary(row.leveldb_records_per_second),
            ));
        }
        output.push('\n');
    }
    output.push_str("## 正确性结论\n\n所有纳入汇总的运行均为错误数 0，且计时后关闭、重开与全量终态验证成功；失败运行不会进入本报告。\n\n");
    output.push_str(&format!(
        "原始 CSV：[raw results]({})\n",
        csv_link.display()
    ));
    output
}

fn render_svg(workload: Workload, rows: &[&SummaryRow]) -> String {
    const WIDTH: f64 = 800.0;
    const HEIGHT: f64 = 480.0;
    const LEFT: f64 = 90.0;
    const RIGHT: f64 = 30.0;
    const TOP: f64 = 45.0;
    const BOTTOM: f64 = 70.0;
    let plot_width = WIDTH - LEFT - RIGHT;
    let plot_height = HEIGHT - TOP - BOTTOM;
    let max_value = rows
        .iter()
        .flat_map(|row| [row.rustkv_ops_per_second, row.leveldb_ops_per_second])
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let x = |index: usize| {
        if rows.len() == 1 {
            LEFT + plot_width / 2.0
        } else {
            LEFT + plot_width * index as f64 / (rows.len() - 1) as f64
        }
    };
    let y = |value: f64| TOP + plot_height * (1.0 - value / max_value);
    let mut svg = format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"800\" height=\"480\" viewBox=\"0 0 800 480\">\n<rect width=\"800\" height=\"480\" fill=\"white\"/>\n<text x=\"400\" y=\"25\" text-anchor=\"middle\" font-family=\"sans-serif\" font-size=\"18\">{} throughput (ops/s)</text>\n<line x1=\"90\" y1=\"45\" x2=\"90\" y2=\"410\" stroke=\"black\"/>\n<line x1=\"90\" y1=\"410\" x2=\"770\" y2=\"410\" stroke=\"black\"/>\n",
        workload.as_str()
    );
    for tick in 0..=4 {
        let value = max_value * tick as f64 / 4.0;
        let tick_y = y(value);
        svg.push_str(&format!("<line x1=\"85\" y1=\"{tick_y:.3}\" x2=\"770\" y2=\"{tick_y:.3}\" stroke=\"#dddddd\"/><text x=\"80\" y=\"{:.3}\" text-anchor=\"end\" font-family=\"sans-serif\" font-size=\"11\">{value:.0}</text>\n", tick_y + 4.0));
    }
    for (index, row) in rows.iter().enumerate() {
        let tick_x = x(index);
        svg.push_str(&format!("<text x=\"{tick_x:.3}\" y=\"430\" text-anchor=\"middle\" font-family=\"sans-serif\" font-size=\"12\">{}</text>\n", row.thread_count));
    }
    svg.push_str("<text x=\"430\" y=\"462\" text-anchor=\"middle\" font-family=\"sans-serif\" font-size=\"13\">OS threads</text>\n<text x=\"18\" y=\"225\" text-anchor=\"middle\" transform=\"rotate(-90 18 225)\" font-family=\"sans-serif\" font-size=\"13\">ops/s</text>\n");
    for (name, color, values) in [
        (
            "RustKV",
            "#1565c0",
            rows.iter()
                .map(|row| row.rustkv_ops_per_second)
                .collect::<Vec<_>>(),
        ),
        (
            "LevelDB",
            "#ef6c00",
            rows.iter()
                .map(|row| row.leveldb_ops_per_second)
                .collect::<Vec<_>>(),
        ),
    ] {
        let points = values
            .iter()
            .enumerate()
            .map(|(index, value)| format!("{:.3},{:.3}", x(index), y(*value)))
            .collect::<Vec<_>>()
            .join(" ");
        svg.push_str(&format!(
            "<polyline fill=\"none\" stroke=\"{color}\" stroke-width=\"2\" points=\"{points}\"/>\n"
        ));
        for (index, value) in values.iter().enumerate() {
            svg.push_str(&format!(
                "<circle cx=\"{:.3}\" cy=\"{:.3}\" r=\"4\" fill=\"{color}\"/>\n",
                x(index),
                y(*value)
            ));
        }
        let legend_x = if name == "RustKV" { 580 } else { 675 };
        svg.push_str(&format!("<line x1=\"{legend_x}\" y1=\"25\" x2=\"{}\" y2=\"25\" stroke=\"{color}\" stroke-width=\"2\"/><text x=\"{}\" y=\"29\" font-family=\"sans-serif\" font-size=\"12\">{name}</text>\n", legend_x + 20, legend_x + 25));
    }
    svg.push_str("</svg>\n");
    svg
}

fn format_auxiliary(value: Option<f64>) -> String {
    value.map_or_else(|| "N/A".to_owned(), |value| format!("{value:.6}"))
}

fn relative_path(from_directory: &Path, to: &Path) -> Result<PathBuf, ReportError> {
    let from = from_directory.components().collect::<Vec<_>>();
    let to = to.components().collect::<Vec<_>>();
    if from.first() != to.first() {
        return Err(ReportError::InvalidPath(
            "report and CSV are on different filesystem roots".to_owned(),
        ));
    }
    let common = from
        .iter()
        .zip(&to)
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = PathBuf::new();
    for component in &from[common..] {
        if matches!(component, Component::Normal(_)) {
            relative.push("..");
        }
    }
    for component in &to[common..] {
        relative.push(component.as_os_str());
    }
    Ok(relative)
}

fn write_synced(path: &Path, contents: String) -> Result<(), ReportError> {
    let mut file = File::create(path).map_err(|error| io_error("create", path, error))?;
    file.write_all(contents.as_bytes())
        .map_err(|error| io_error("write", path, error))?;
    file.sync_all()
        .map_err(|error| io_error("sync", path, error))
}

#[derive(Debug)]
pub enum ReportError {
    Csv(CsvError),
    InvalidPath(String),
    OutputExists(PathBuf),
    Io(String),
    EmptyInput,
    RepetitionCount {
        workload: Workload,
        thread_count: usize,
        expected: usize,
        rustkv: usize,
        leveldb: usize,
    },
    MissingMetric(String),
    NonFiniteMetric,
}

impl From<CsvError> for ReportError {
    fn from(error: CsvError) -> Self {
        Self::Csv(error)
    }
}

impl fmt::Display for ReportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "report error: {self:?}")
    }
}

impl Error for ReportError {}

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> ReportError {
    ReportError::Io(format!("{operation} {} failed: {error}", path.display()))
}

use std::process::ExitCode;

fn main() -> ExitCode {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    match kv_bench::parse_cli(arguments).and_then(kv_bench::execute_cli) {
        Ok(output) => {
            print!("{output}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("kv_bench: {error}");
            if matches!(error, kv_bench::CliError::Usage(_)) {
                eprintln!("run `kv_bench --help` for usage");
            }
            ExitCode::from(error.exit_code())
        }
    }
}

use std::process::ExitCode;

fn main() -> ExitCode {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let first = arguments.next();
    let has_extra = arguments.next().is_some();

    match (first.as_deref(), has_extra) {
        (Some(argument), false) if argument == "--help" || argument == "-h" => {
            print!("{}", kv_bench::help_text());
            ExitCode::SUCCESS
        }
        (Some(argument), false) if argument == "--version" || argument == "-V" => {
            println!("{}", kv_bench::version_text());
            ExitCode::SUCCESS
        }
        (Some(argument), _) => {
            eprintln!(
                "kv_bench: unsupported command in stage B0: {}",
                argument.to_string_lossy()
            );
            eprintln!("run `kv_bench --help` for the available commands");
            ExitCode::from(2)
        }
        (None, false) => {
            eprintln!("kv_bench: a command is required; run `kv_bench --help`");
            ExitCode::from(2)
        }
        (None, true) => unreachable!("extra arguments cannot exist without a first argument"),
    }
}

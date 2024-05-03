use delta_kernel::actions::get_log_schema;
use delta_kernel::actions::visitors::{
    AddVisitor, MetadataVisitor, ProtocolVisitor, RemoveVisitor,
};
use delta_kernel::client::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::client::default::DefaultEngineInterface;
use delta_kernel::engine_data::{GetData, TypedGetData};
use delta_kernel::scan::ScanBuilder;
use delta_kernel::schema::{DataType, SchemaRef, StructField};
use delta_kernel::{DataVisitor, DeltaResult, Table};

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the table to inspect
    #[arg(short, long)]
    path: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Print the most recent version of the table
    TableVersion,
    /// Show the table's metadata
    Metadata,
    /// Show the table's schema
    Schema,
    /// Show all `Add` actions recorded on the table
    Adds,
    /// Show each action from the log-segments
    Actions {
        /// Show the log in forward order (default is to show it going backwards in time)
        #[arg(short, long)]
        forward: bool,
    },
}

fn main() -> ExitCode {
    env_logger::init();
    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            println!("{e:#?}");
            ExitCode::FAILURE
        }
    }
}

enum Action {
    Metadata(delta_kernel::actions::Metadata, usize),
    Protocol(delta_kernel::actions::Protocol, usize),
    Remove(delta_kernel::actions::Remove, usize),
    Add(delta_kernel::actions::Add, usize),
}

impl Action {
    fn row(&self) -> usize {
        match self {
            Action::Metadata(_, row) => *row,
            Action::Protocol(_, row) => *row,
            Action::Remove(_, row) => *row,
            Action::Add(_, row) => *row,
        }
    }
}

fn fields_in(field: &StructField) -> usize {
    if let DataType::Struct(ref inner) = field.data_type {
        let mut count = 0;
        for field in inner.fields() {
            count += fields_in(field);
        }
        count
    } else {
        1
    }
}

struct LogVisitor {
    actions: Vec<Action>,
    add_offset: usize,
    remove_offset: usize,
    protocol_offset: usize,
    metadata_offset: usize,
    previous_rows_seen: usize,
}

impl LogVisitor {
    fn new(log_schema: &SchemaRef) -> LogVisitor {
        let mut offset = 0;
        let mut add_offset = 0;
        let mut remove_offset = 0;
        let mut protocol_offset = 0;
        let mut metadata_offset = 0;
        for field in log_schema.fields() {
            match field.name().as_str() {
                "add" => add_offset = offset,
                "remove" => remove_offset = offset,
                "protocol" => protocol_offset = offset,
                "metaData" => metadata_offset = offset,
                _ => {}
            }
            offset += fields_in(field);
        }
        LogVisitor {
            actions: vec![],
            add_offset,
            remove_offset,
            protocol_offset,
            metadata_offset,
            previous_rows_seen: 0,
        }
    }
}

impl DataVisitor for LogVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            if let Some(path) = getters[self.add_offset].get_opt(i, "add.path")? {
                self.actions.push(Action::Add(
                    AddVisitor::visit_add(i, path, &getters[self.add_offset..])?,
                    self.previous_rows_seen + i,
                ));
            }
            if let Some(path) = getters[self.remove_offset].get_opt(i, "remove.path")? {
                self.actions.push(Action::Remove(
                    RemoveVisitor::visit_remove(i, path, &getters[self.remove_offset..])?,
                    self.previous_rows_seen + i,
                ));
            }
            if let Some(id) = getters[self.metadata_offset].get_opt(i, "metadata.id")? {
                self.actions.push(Action::Metadata(
                    MetadataVisitor::visit_metadata(i, id, &getters[self.metadata_offset..])?,
                    self.previous_rows_seen + i,
                ));
            }
            if let Some(min_reader_version) =
                getters[self.protocol_offset].get_opt(i, "protocol.min_reader_version")?
            {
                self.actions.push(Action::Protocol(
                    ProtocolVisitor::visit_protocol(
                        i,
                        min_reader_version,
                        &getters[self.protocol_offset..],
                    )?,
                    self.previous_rows_seen + i,
                ));
            }
        }
        self.previous_rows_seen += row_count;
        Ok(())
    }
}

fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();
    let url = url::Url::parse(&cli.path)?;
    let engine_interface = DefaultEngineInterface::try_new(
        &url,
        HashMap::<String, String>::new(),
        Arc::new(TokioBackgroundExecutor::new()),
    )?;

    let table = Table::new(url);
    let snapshot = table.snapshot(&engine_interface, None)?;

    match &cli.command {
        Commands::TableVersion => {
            println!("Latest table version: {}", snapshot.version());
        }
        Commands::Metadata => {
            println!("{:#?}", snapshot.metadata());
        }
        Commands::Schema => {
            println!("{:#?}", snapshot.schema());
        }
        Commands::Adds => {
            use delta_kernel::actions::Add;
            let scan = ScanBuilder::new(snapshot).build();
            let files: Vec<Add> = scan
                .files(&engine_interface)
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            println!("{:#?}", files);
        }
        Commands::Actions { forward } => {
            let log_schema = Arc::new(get_log_schema().clone());
            let actions = snapshot._log_segment().replay(
                &engine_interface,
                log_schema.clone(),
                log_schema.clone(),
                None,
            )?;

            let mut visitor = LogVisitor::new(&log_schema);
            for action in actions {
                action?.0.extract(log_schema.clone(), &mut visitor)?;
            }

            if *forward {
                visitor
                    .actions
                    .sort_by(|a, b| a.row().partial_cmp(&b.row()).unwrap());
            } else {
                visitor
                    .actions
                    .sort_by(|a, b| b.row().partial_cmp(&a.row()).unwrap());
            }
            for action in visitor.actions.iter() {
                match action {
                    Action::Metadata(md, row) => println!("\nAction {row}:\n{:#?}", md),
                    Action::Protocol(p, row) => println!("\nAction {row}:\n{:#?}", p),
                    Action::Remove(r, row) => println!("\nAction {row}:\n{:#?}", r),
                    Action::Add(a, row) => println!("\nAction {row}:\n{:#?}", a),
                }
            }
        }
    };
    Ok(())
}

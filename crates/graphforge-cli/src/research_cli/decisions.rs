//! CLI validation for caller-produced provider-neutral decision batches.
use std::{
    io::{Read, Write},
    path::PathBuf,
};

use arrow::ipc::writer::StreamWriter;
use clap::{Args, Subcommand};
use graphforge_api::{DecisionBatchV1, GfError, GraphForge};

#[derive(Subcommand)]
pub(crate) enum DecisionCommand {
    /// Validate a JSON decision batch and write Arrow decision_result/1 IPC.
    Validate(ValidateArgs),
}

#[derive(Args)]
pub(crate) struct ValidateArgs {
    /// JSON DecisionBatchV1 file supplied by the caller or producer.
    #[arg(long)]
    file: PathBuf,
    /// Output Arrow IPC file; parent directories must already exist.
    #[arg(long)]
    output: PathBuf,
}

fn validate_file(path: PathBuf) -> Result<DecisionBatchV1, GfError> {
    let file = std::fs::File::open(path)
        .map_err(|_| GfError::Validation("cannot read decision batch file".into()))?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| GfError::Validation("cannot read decision batch file".into()))?;
    if bytes.len() > 1024 * 1024 {
        return Err(GfError::Api {
            code: graphforge_api::ApiErrorCode::ResourceLimit,
            message: "decision batch input exceeds its byte bound".into(),
        });
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| GfError::Validation("invalid decision batch JSON contract".into()))
}

fn encode_arrow(
    result: &arrow::record_batch::RecordBatch,
    output: impl Write,
) -> Result<(), GfError> {
    let mut writer = StreamWriter::try_new(output, result.schema().as_ref())
        .map_err(|_| GfError::Validation("cannot initialize decision Arrow IPC".into()))?;
    writer
        .write(result)
        .map_err(|_| GfError::Validation("cannot write decision Arrow IPC".into()))?;
    writer
        .finish()
        .map_err(|_| GfError::Validation("cannot finish decision Arrow IPC".into()))
}

pub(crate) fn run(
    _graph: &GraphForge,
    command: DecisionCommand,
    output: &mut dyn Write,
) -> Result<(), GfError> {
    match command {
        DecisionCommand::Validate(args) => {
            let batch = validate_file(args.file)?;
            let result = batch.validate()?;
            let mut ipc = Vec::new();
            encode_arrow(&result, &mut ipc)?;
            std::fs::write(args.output, ipc)
                .map_err(|_| GfError::Validation("cannot write decision Arrow result".into()))?;
            writeln!(output, "validated {} decision rows", result.num_rows())
                .map_err(|_| GfError::Validation("cannot write CLI output".into()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::encode_arrow;
    use arrow::record_batch::RecordBatch;
    use graphforge_api::{DecisionBatchV1, GfError};
    use std::{
        cell::Cell,
        io::{self, Write},
        rc::Rc,
    };
    use uuid::Uuid;

    struct TestWriter {
        calls: Rc<Cell<usize>>,
        fail_on: Option<usize>,
    }

    impl Write for TestWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let call = self.calls.get() + 1;
            self.calls.set(call);
            if self.fail_on == Some(call) {
                Err(io::Error::other("injected Arrow IPC write failure"))
            } else {
                Ok(bytes.len())
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn decision_batch() -> RecordBatch {
        let question = Uuid::now_v7();
        let request = serde_json::json!({
            "input": {
                "generation_uuid": Uuid::now_v7(),
                "version_uuid": null,
                "projection_sha256": vec![1; 32],
                "selection_sha256": vec![2; 32],
                "selected_item_uuids": [],
            },
            "producer": { "name": "coverage fixture" },
            "questions": [{
                "question_uuid": question,
                "text": "Continue?",
                "item_uuids": [],
                "kind": { "kind": "choice", "allowed_choices": ["yes"] },
            }],
            "results": [{
                "question_uuid": question,
                "item_uuid": null,
                "status": "answered",
                "value": { "kind": "choice", "value": "yes" },
            }],
        });
        serde_json::from_value::<DecisionBatchV1>(request)
            .unwrap()
            .validate()
            .unwrap()
    }

    #[test]
    fn arrow_ipc_encoder_reports_initialization_write_and_finish_failures() {
        let result = decision_batch();
        let calls = Rc::new(Cell::new(0));
        encode_arrow(
            &result,
            TestWriter {
                calls: calls.clone(),
                fail_on: None,
            },
        )
        .unwrap();

        let mut failures = Vec::new();
        for fail_on in 1..=calls.get() {
            let error = encode_arrow(
                &result,
                TestWriter {
                    calls: Rc::new(Cell::new(0)),
                    fail_on: Some(fail_on),
                },
            );
            if let Err(GfError::Validation(message)) = error {
                failures.push(message);
            }
        }
        for expected in [
            "cannot initialize decision Arrow IPC",
            "cannot write decision Arrow IPC",
            "cannot finish decision Arrow IPC",
        ] {
            assert!(failures.iter().any(|message| message == expected));
        }
    }
}

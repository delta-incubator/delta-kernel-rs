use std::path::Path;
use std::sync::Arc;

use acceptance::read_dat_case;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;

fn reader_test(path: &Path) -> datatest_stable::Result<()> {
    let root_dir = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        path.parent().unwrap().to_str().unwrap()
    );

    // TODO(zach): skip iceberg_compat_v1 test until DAT is fixed
    if path.ends_with("iceberg_compat_v1/test_case_info.json") {
        return Ok(());
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let case = read_dat_case(root_dir).unwrap();
            let table_root = case.table_root().unwrap();
            let engine = Arc::new(
                DefaultEngine::try_new(
                    &table_root,
                    std::iter::empty::<(&str, &str)>(),
                    Arc::new(TokioBackgroundExecutor::new()),
                )
                .unwrap(),
            );

            case.assert_metadata(engine.clone()).await.unwrap();
            acceptance::data::assert_scan_data(engine.clone(), &case)
                .await
                .unwrap();
        });
    Ok(())
}

datatest_stable::harness!(
    reader_test,
    "tests/dat/out/reader_tests/generated/",
    r"test_case_info\.json"
);

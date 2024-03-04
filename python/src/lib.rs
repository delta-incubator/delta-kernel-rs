use std::sync::Arc;

use arrow::pyarrow::PyArrowType;
use deltakernel::simple_client::data::SimpleData;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use url::Url;

use arrow::record_batch::RecordBatch;
use deltakernel::EngineInterface;
use deltakernel::simple_client::SimpleClient;

struct KernelError(deltakernel::Error);

impl From<KernelError> for PyErr {
    fn from(error: KernelError) -> Self {
        PyValueError::new_err(format!("Kernel error: {}", error.0))
    }
}

impl From<deltakernel::Error> for KernelError {
    fn from(delta_kernel_error: deltakernel::Error) -> Self {
        Self(delta_kernel_error)
    }
}

type DeltaPyResult<T> = std::result::Result<T, KernelError>;

#[pyclass]
struct Table(deltakernel::Table);

#[pymethods]
impl Table {
    #[new]
    fn new(location: &str) -> DeltaPyResult<Self> {
        let location = Url::parse(location).map_err(|e| deltakernel::Error::InvalidUrl(e))?;
        let table = deltakernel::Table::new(location);
        Ok(Table(table))
    }

    fn snapshot(&self, engine_interface: &PythonInterface) -> Snapshot {
        let snapshot = self.0.snapshot(engine_interface.0.as_ref(), None).unwrap();
        Snapshot(snapshot)
    }
}

#[pyclass]
struct Snapshot(Arc<deltakernel::snapshot::Snapshot>);

#[pymethods]
impl Snapshot {
    fn version(&self) -> deltakernel::Version {
        self.0.version()
    }
}

#[pyclass]
struct ScanBuilder(deltakernel::scan::ScanBuilder);

#[pymethods]
impl ScanBuilder {
    #[new]
    fn new(snapshot: &Snapshot) -> ScanBuilder {
        let sb = deltakernel::scan::ScanBuilder::new(snapshot.0.clone());
        ScanBuilder(sb)
    }

    fn build(&self) -> Scan {
        let scan = self.0.clone().build();
        Scan(scan)
    }
}

#[pyclass]
struct Scan(deltakernel::scan::Scan);

#[pymethods]
impl Scan {
    fn execute(&self, engine_interface: &PythonInterface) -> DeltaPyResult<ScanResultIter> {
        let results = self.0.execute(engine_interface.0.as_ref())?;
        Ok(ScanResultIter {
            iter: Box::new(results.into_iter().map(|res| {
                // TODO(nick) PyResult isn't a PyObject (no IntoPy for Result), so we can't return
                // results from iteration
                let data = res.raw_data.unwrap();
                let record_batch: RecordBatch = data.into_any().downcast::<SimpleData>().unwrap().into();
                PyArrowType(record_batch)
            }))
        })
    }
}



#[pyclass]
struct ScanResultIter {
    iter: Box<dyn Iterator<Item = PyArrowType<RecordBatch>> + Send>,
}

#[pymethods]
impl ScanResultIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyArrowType<RecordBatch>> {
        slf.iter.next()
    }
}

#[pyclass]
struct PythonInterface(Box<dyn EngineInterface + Send>);

#[pymethods]
impl PythonInterface {
    #[new]
    fn new() -> Self {
        let simple_client = SimpleClient::new();
        PythonInterface(Box::new(simple_client))
    }
}

/// Define the delta_kernel_python module. The name of this function _must_ be
/// `delta_kernel_python`, and _must_ the `lib.name` setting in the `Cargo.toml`, otherwise Python
/// will not be able to import the module.
#[pymodule]
fn delta_kernel_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Table>()?;
    m.add_class::<PythonInterface>()?;
    m.add_class::<Snapshot>()?;
    m.add_class::<ScanBuilder>()?;
    m.add_class::<Scan>()?;
    Ok(())
}

use pyo3::prelude::*;
use arrow::pyarrow::ToPyArrow;
mod reader;
mod data;

#[pyfunction]
fn read_dicom(path: &str) -> PyResult<Py<PyAny>> {
    let dicom_reader = reader::DicomReader::new(path);
    let record_batch = data::create_record_batch(dicom_reader);
    Python::with_gil(|py| {
        let pyarrow_record_batch = record_batch.to_pyarrow(py).unwrap();
        let pandas_dataframe = pyarrow_record_batch.call_method0(py, "to_pandas").unwrap();
        Ok(pandas_dataframe)
    })
}

#[pymodule]
fn dicom_reader(_module: &Bound<'_, PyModule>) -> PyResult<()> {
    Python::with_gil(|py| {
        if let Ok(pandas) = Python::import_bound(py, "pandas") {
            pandas.add_function(wrap_pyfunction!(read_dicom, &pandas).unwrap()).unwrap();
        }
    });
    Ok(())
}

use pyo3::prelude::*;
use arrow::pyarrow::ToPyArrow;
mod reader;
mod data;

fn read_dicom(path: &str) -> Py<PyAny> {
    let dicom_reader = reader::DicomReader::new(path);
    let record_batch = data::create_record_batch(dicom_reader);
    Python::with_gil(|py| {
        record_batch.to_pyarrow(py).unwrap()
    })
}

#[pyfunction]
#[pyo3(name="read_dicom")]
fn read_dicom_to_pandas(path: &str) -> PyResult<Py<PyAny>> {
    let pyarrow_record_batch = read_dicom(path);
    Python::with_gil(|py| {
        let pandas_dataframe = pyarrow_record_batch.call_method0(py, "to_pandas").unwrap();
        Ok(pandas_dataframe)
    })
}

#[pyfunction]
#[pyo3(name="read_dicom")]
fn read_dicom_to_polars(path: &str) -> PyResult<Py<PyAny>> {
    let pyarrow_record_batch = read_dicom(path);
    Python::with_gil(|py| {
        let polars = Python::import_bound(py, "polars").unwrap();
        let from_arrow = polars.getattr("from_arrow").unwrap();
        let args = (pyarrow_record_batch,);
        let polars_dataframe = from_arrow.call1(args).unwrap().into();
        Ok(polars_dataframe)
    })
}

#[pymodule]
fn dicom_reader(_module: &Bound<'_, PyModule>) -> PyResult<()> {
    Python::with_gil(|py| {
        if let Ok(pandas) = Python::import_bound(py, "pandas") {
            pandas.add_function(wrap_pyfunction!(read_dicom_to_pandas, &pandas).unwrap()).unwrap();
        }

        if let Ok(polars) = Python::import_bound(py, "polars") {
            polars.add_function(wrap_pyfunction!(read_dicom_to_polars, &polars).unwrap()).unwrap();
        }
    });
    Ok(())
}

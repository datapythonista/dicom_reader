use std::ffi::CStr;
use std::os::raw::c_char;
use serde::Deserialize;
use serde_json;

mod file_ops;
mod dicom_data;


#[derive(Deserialize)]
struct DicomReaderArgs {
    directory_path: String,
}

#[no_mangle]
pub extern "C" fn read_dicom(args_json_ptr: *const c_char) {
    let args_json = unsafe { CStr::from_ptr(args_json_ptr) };

    let args: DicomReaderArgs = serde_json::from_str(args_json.to_str().unwrap()).expect("wrong arguments");

    let paths = file_ops::iter_directory(&args.directory_path, vec!["dcm".into()]);
    let result_length = paths.len();

    for path in paths {
        let dicom_obj = dicom_data::DicomData::new(&path);

        // TODO Instead of printing values this will create and return an Apache Arrow structure
        println!("path={}, \
                 num_rows={}, \
                 meta_information_group_length={}, \
                 modality={}, \
                 patient_name={}, \
                 patient_birth_date={:?}, \
                 pixel_data_len={}",
                 path.display(),
                 result_length,
                 dicom_obj.meta().information_group_length,
                 dicom_obj.modality().unwrap_or("<None>".to_string()),
                 dicom_obj.patient_name().unwrap_or("<None>".to_string()),
                 dicom_obj.patient_birth_date(),
                 dicom_obj.pixel_data().unwrap_or(vec![]).len(),
        );
    }
}

use std::path::Path;
use chrono::NaiveDate;
use dicom::object::{open_file, DefaultDicomObject, FileMetaTable};
use dicom::dictionary_std::tags;
use dicom::pixeldata::PixelDecoder;


pub struct DicomData {
    dicom_obj: DefaultDicomObject,
}

impl DicomData {
    pub fn new(path: &Path) -> Self {
        let dicom_obj = open_file(&path).unwrap();
        DicomData { dicom_obj }
    }

    pub fn meta(&self) -> &FileMetaTable {
        self.dicom_obj.meta()
    }
    pub fn modality(&self) -> Option<String> {
        if let Ok(value) = self.dicom_obj.element(tags::MODALITY) {
            if let Ok(value) = value.to_str() {
                return Some(value.into_owned());
            }
        }
        None
    }
    pub fn patient_name(&self) -> Option<String> {
        if let Ok(value) = self.dicom_obj.element(tags::PATIENT_NAME) {
            if let Ok(value) = value.to_str() {
                return Some(value.into_owned());
            }
        }
        None
    }
    pub fn patient_birth_date(&self) -> Option<NaiveDate> {
        if let Ok(value) = self.dicom_obj.element(tags::PATIENT_BIRTH_DATE) {
            if let Ok(value) = value.to_date() {
                if let Ok(value) = value.to_naive_date() {
                    return Some(value);
                }
            }
        }
        None
    }
    pub fn pixel_data(&self) -> Option<Vec<u16>> {
        if let Ok(pixel_data) = self.dicom_obj.decode_pixel_data() {
            if let Ok(pixel_data) = pixel_data.to_ndarray::<u16>() {
                return Some(pixel_data.as_slice()?.to_vec());
            }
        }
        None
    }
}

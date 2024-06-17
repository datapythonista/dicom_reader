use std::collections::HashSet;
use dicom::pixeldata::PixelDecoder;
use dicom::dictionary_std::tags;

/// A standard representation of a Dicom image
///
/// This is not standard in the dimensions, but in the bits used to represent the data.
/// All the voxels are represented in 16 bits HU.
pub struct DicomImage {
    pub columns: usize,
    pub rows: usize,
    pub frames: usize,
    pub voxels: Vec<i16>,
}
impl DicomImage {
    fn new(files: Vec<&std::path::Path>) -> Self {
        let options = dicom::pixeldata::ConvertOptions::new()
            .with_modality_lut(dicom::pixeldata::ModalityLutOption::None)
            .with_voi_lut(dicom::pixeldata::VoiLutOption::Default)
            .with_bit_depth(dicom::pixeldata::BitDepthOption::Auto);

        let first_dicom_file = dicom::object::open_file(files[0]).unwrap();
        let pixel_data = first_dicom_file.decode_pixel_data().unwrap();
        let columns = pixel_data.columns() as usize;
        let rows = pixel_data.rows() as usize;
        let frames = files.len();

        let modality = first_dicom_file.element(tags::MODALITY).unwrap().to_str().unwrap();

        if modality != "CT" && modality != "PT" {
            panic!("Only CT scans are supported. Found modality={modality}");
        }
        if pixel_data.bits_allocated() != 16 {
            panic!("Only 16 bits pixels are supported. Found bits_allocated={}", pixel_data.bits_allocated());
        }
        if pixel_data.samples_per_pixel() != 1 {
            panic!("Only monochrome files are supported. Found samples_per_pixel={}", pixel_data.samples_per_pixel());
        }

        let mut ct_scan = DicomImage { columns, rows, frames, voxels: Vec::with_capacity(columns * rows * frames) };

        for current_file in files {
            if let Ok(dicom_file) = dicom::object::open_file(current_file) {
                ct_scan.voxels.extend_from_slice(&dicom_file.decode_pixel_data().unwrap().to_vec_with_options(&options).unwrap());
            }
        }
        ct_scan
    }
}
impl std::fmt::Debug for DicomImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CTScan({}x{}x{})", self.columns, self.rows, self.frames)
    }
}

pub struct DicomReader {
    dicom_directories: Vec<String>,
    index: usize,
}
impl DicomReader {
    pub fn new(path: &str) -> Self {
        let mut dicom_directories = HashSet::new();

        for entry in walkdir::WalkDir::new(path).sort_by_file_name().into_iter().filter_map(|x| x.ok()) {
            if entry.path().extension().is_some() && entry.path().extension().unwrap() == "dcm" {
                dicom_directories.insert(entry.path().parent().unwrap().to_str().unwrap().to_owned());
            }
        }
        DicomReader {
            dicom_directories: dicom_directories.into_iter().collect::<Vec<_>>(),
            index: 0,
        }
    }
}
impl Iterator for DicomReader {
    type Item = DicomImage;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.dicom_directories.len() {
            None
        } else {

            let files = std::fs::read_dir(&self.dicom_directories[self.index]).unwrap();
            let dicom_files = files.filter(|x| x.as_ref().unwrap().path().extension().unwrap() == "dcm")
                                   .map(|x| x.unwrap().path())
                                   .collect::<Vec<_>>();

            self.index += 1;
            Some(DicomImage::new(dicom_files.iter().map(|x| x.as_path()).collect()))
        }
    }
}

use dicom::dictionary_std::tags;
use dicom::pixeldata::PixelDecoder;


fn print_dicom_data(path: &str) {
    let maybe_dicom_obj = dicom::object::open_file(&path);
    if maybe_dicom_obj.is_err() {
        println!("!!! Error opening {path} file !!!");
        return;
    }
    let dicom_obj = maybe_dicom_obj.unwrap();

    let modality = dicom_obj.element(tags::MODALITY).unwrap().to_str().unwrap().into_owned();

    let maybe_pixel_data = dicom_obj.decode_pixel_data(); //.to_ndarray::<u16>().unwrap();
    if maybe_pixel_data.is_err() {
        println!("!!! Error decoding {path} file !!!");
        return;
    }
    let pixel_data = maybe_pixel_data.unwrap();

    let columns = pixel_data.columns();
    let rows = pixel_data.rows();
    let frames = pixel_data.number_of_frames();
    let rescale_slope = pixel_data.rescale().unwrap()[0].slope;
    let rescale_intercept = pixel_data.rescale().unwrap()[0].intercept;
    let bits_allocated = pixel_data.bits_allocated();
    let bits_stored = pixel_data.bits_stored();
    let high_bit = pixel_data.high_bit();
    let pixel_representation_is_signed = pixel_data.pixel_representation() == dicom::pixeldata::PixelRepresentation::Signed;

    let raw_data = pixel_data.data();

    let options = dicom::pixeldata::ConvertOptions::new()
        .with_modality_lut(dicom::pixeldata::ModalityLutOption::None)
        .with_voi_lut(dicom::pixeldata::VoiLutOption::Default)
        .with_bit_depth(dicom::pixeldata::BitDepthOption::Auto);
    let decoded_data: Vec<i16> = pixel_data.to_vec_with_options(&options).unwrap();

    //let window_center = pixel_data.window().unwrap().unwrap()[0].center;
    //let window_width = pixel_data.window().unwrap().unwrap()[0].width;
    //let photometric_interpretation = pixel_data.photometric_interpretation();


    println!("{path}");
    println!("\tmodality={modality}");
    println!("\tcolumns={columns}");
    println!("\trows={rows}");
    println!("\tframes={frames}");
    println!("\trescale_slope={rescale_slope}");
    println!("\trescale_intercept={rescale_intercept}");
    println!("\tbits_allocated={bits_allocated}");
    println!("\tbits_stored={bits_stored}");
    println!("\thigh_bit={high_bit}");
    println!("\tpixel_representation_is_signed={pixel_representation_is_signed}");

    println!("\traw_data:{:?} (length={})", &raw_data[0..8], raw_data.len());
    println!("\tdata={:?} (length={})", &decoded_data[0..8], decoded_data.len());

    //println!("\twindow_center={window_center}");
    //println!("\twindow_width={window_width}");
    //println!("\tphotometric_interpretation={photometric_interpretation:?}");

    // println!("{pixel_data:?}");
}

fn main() {
    //let path = "/home/mgarcia/src/dicom_reader/data/tciaDownload/pat1/1-002.dcm";
    let path = "/home/mgarcia/src/dicom_reader/data/manifest-1684259732535/Prostate-Anatomical-Edge-Cases/Prostate-AEC-001/11-17-1992-NA-RX SIMULATION-82988/2.000000-Pelvis-13578/1-001.dcm";
    print_dicom_data(&path);

    //let data_dir = "/home/mgarcia/src/dicom_reader/data/manifest-1684259732535";
    let data_dir = "/home/mgarcia/src/dicom_reader/data/manifest-1677266205028";
    for entry in walkdir::WalkDir::new(data_dir).into_iter().filter_map(|x| x.ok()) {
        let path = entry.path();
        if let Some(ext) = path.extension() {
            if ext == "dcm" {
                //println!("{:?}", path);
                print_dicom_data(&path.to_str().unwrap());
            }
        }
    }
}

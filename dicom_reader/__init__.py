
import ctypes
import json
import pathlib
import platform


def call_dicom_reader(data_directory, lib_name):
    """TODO
    Args: 
      
    ----------
    Returns:
    
    """
    args = {'directory_path': str(data_directory)}
    args_str = json.dumps(args)
    args_buffer = args_str.encode('utf-8')
    args_ptr = ctypes.c_char_p(args_buffer)

    if platform.system() == 'Linux':
        lib = ctypes.cdll.LoadLibrary(lib_name + '.so')
    elif platform.system() == 'Darwin':
        lib = ctypes.CDLL(lib_name + '.dylib')
    else:
        raise NotImplementedError('call_dicom_reader not implemented for your operating system')

    lib.read_dicom(args_ptr)


if __name__ == '__main__':
    data_directory = pathlib.Path(__file__).resolve().parent.parent / 'data'/ 'tciaDownload' / 'pat1'
    call_dicom_reader(data_directory, lib_name='libdicom_reader')   

#: All readers mapped to their respective valid extensions
FILE_READER2EXTENSIONS = {
    "TEXT_READER": [
        'txt', 'md', 'rst', 'py',
        'html', 'css', 'js', 'json', 'svg'
    ],
    "IMAGE_READER": [
        'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'webp',
        'tif', 'ico', 'psd', 'raw', 'arw', 'cr2', 'crw',
        'dcr', 'dng', 'erf', 'kdc', 'mos', 'nef', 'nrw', 'orf',
        'pef', 'raf', 'rw2', 'srw', 'x3f'
    ],
    "BYTE_READER": [
        'zip', 'rar', '7z', 'gz', 'bz2', 'xz', 'tar', 'tgz', 'xz',
        'iso', 'dmg', 'img', 'bin', 'exe', 'dll', 'msi', 'apk',
        'ipa', 'deb', 'rpm', 'cab', 'pkg', 'mpkg', 'msi', 'msp',
        'mst', 'msu', 'msp', 'mse', 'makefile', '']
}

#: Filename for the trace file generated in checkpoint directories
TRACE_FILENAME = 'trace.json'

#: File extensions that are considered text files for line-level diff
TEXT_EXTENSIONS = set(FILE_READER2EXTENSIONS["TEXT_READER"])

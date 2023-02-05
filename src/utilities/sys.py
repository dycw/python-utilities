from sys import version_info

VERSION_MAJOR_MINOR = (version_info.major, version_info.minor)
PYTHON_AT_LEAST_3_11 = VERSION_MAJOR_MINOR >= (3, 11)

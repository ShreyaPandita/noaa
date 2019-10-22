py_binary(
    name = "goes",
    srcs = ["goes.py"],
    main = "goes.py",
    visibility = ["//visibility:private"],
    deps = [
        ":goes_16_metadata_reader_lib",
        ":tracking_cloudsql",
        "//cloud/bigstore/util:bigstore_file_register",
        "//file/colossus/public:cns",
        "//pyglib",
        "//pyglib:gfile",
        "//pyglib/flags",
        "//pyglib/logging",
    ],
)

py_binary(
    name = "pysqltest",
    srcs = ["pysqltest.py"],
    main = "pysqltest.py",
    deps = [
        "//cloud/bigstore/util:bigstore_file_register",
        "//pyglib",
        "//pyglib:gfile",
        "//pyglib/flags",
        "//pyglib/logging",
        "//third_party/py/MySQLdb",
        # "//third_party/py/concurrent:futures",
    ],
)

py_library(
    name = "tracking_cloudsql",
    srcs = ["tracking_cloudsql.py"],
    deps = [
        "//base/python:pywrapbase",
        "//pyglib",
        "//third_party/py/MySQLdb",
    ],
)

py_library(
    name = "goes_16_metadata_reader_lib",
    srcs = [
        "goes_16_metadata_reader.py",
    ],
    deps = [
        "//third_party/py/h5py",
    ],
)

genmpm(
    name = "goes_mpm",
    package_name = "test/gtf/goes",
    srcs = [":goes.par"],
)

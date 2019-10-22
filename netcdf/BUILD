py_binary(
    name = "netcdf_plugin",
    srcs = ["plugins/netcdf_plugin.py"],
    visibility = ["//visibility:private"],
    deps = [":netcdf_plugin_main_lib"],
)

py_library(
    name = "netcdf_plugin_main_lib",
    srcs = ["plugins/netcdf_plugin.py"],
    deps = [
        ":netcdf_metadata_reader",
        ":netcdf_mysql",
        "//cloud/bigstore/util:bigstore_file_register",
        "//file/colossus/public:cns",
        "//pyglib",
        "//pyglib:gfile",
        "//pyglib/flags",
    ],
)

py_test(
    name = "netcdf_plugin_test",
    size = "small",
    srcs = ["plugins/netcdf_plugin_test.py"],
    data = [
        ":plugins/testdata/rad.nc",
        ":plugins/testdata/test_config.json",
    ],
    deps = [
        ":netcdf_plugin_main_lib",
        "//pyglib",
        "//pyglib:gfile",
        "//testing/pybase",
        "//third_party/py/mock",
    ],
)

py_library(
    name = "netcdf_mysql",
    srcs = ["plugins/netcdf_mysql.py"],
    deps = [
        "//base/python:pywrapbase",
        "//pyglib",
        "//third_party/py/MySQLdb",
    ],
)

py_test(
    name = "netcdf_mysql_test",
    size = "small",
    srcs = ["plugins/netcdf_mysql_test.py"],
    deps = [
        ":netcdf_mysql",
        "//pyglib",
        "//pyglib:gfile",
        "//pyglib/flags",
        "//testing/mysql:mysql_py",
        "//testing/pybase",
    ],
)

py_library(
    name = "netcdf_metadata_reader",
    srcs = [
        "plugins/netcdf_metadata_reader.py",
    ],
    deps = [
        "//pyglib:gfile",
        "//third_party/py/h5py",
    ],
)

py_test(
    name = "netcdf_metadata_reader_test",
    size = "small",
    srcs = ["plugins/netcdf_metadata_reader_test.py"],
    data = [
        ":plugins/testdata/rad.nc",
        ":plugins/testdata/test_config.json",
    ],
    deps = [
        ":netcdf_metadata_reader",
        "//pyglib",
        "//pyglib:gfile",
        "//testing/pybase",
    ],
)

genmpm(
    name = "netcdf_plugin_mpm",
    package_name = "test/gtf/netcdf_plugin",
    srcs = [":netcdf_plugin.par"],
)

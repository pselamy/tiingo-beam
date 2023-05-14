load("@pip_deps//:requirements.bzl", "requirement")

py_library(
    name = "endpoints",
    srcs = ["endpoints.py"],
    visibility = ["//visibility:public"],
    deps = [
        requirement("websocket-client"),
        ":models",
        ":parsers",
        ":subscriptions",
    ],
)

py_test(
    name = "endpoints_test",
    srcs = ["endpoints_test.py"],
    deps = [
        ":endpoints",
    ],
)

py_library(
    name = "firehose",
    srcs = ["firehose.py"],
    visibility = ["//visibility:public"],
    deps = [
        requirement("apache-beam"),
        ":endpoints",
    ],
)

py_library(
    name = "models",
    srcs = ["models.py"],
)

py_library(
    name = "parsers",
    srcs = ["parsers.py"],
    deps = [
        requirement("python-dateutil"),
        ":models",
    ],
)

py_test(
    name = "parsers_test",
    srcs = ["parsers_test.py"],
    deps = [
        requirement("hypothesis"),
        requirement("pytz"),
        ":parsers",
    ],
)

py_library(
    name = "subscriptions",
    srcs = ["subscriptions.py"],
)

py_test(
    name = "subscriptions_test",
    srcs = ["subscriptions_test.py"],
    deps = [
        requirement("hypothesis"),
        ":subscriptions",
    ],
)

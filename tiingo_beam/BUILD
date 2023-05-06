load("@pip_deps//:requirements.bzl", "requirement")

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

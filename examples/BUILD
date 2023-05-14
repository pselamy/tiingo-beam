load("@pip_deps//:requirements.bzl", "requirement")

py_binary(
    name = "log_trades",
    srcs = ["log_trades.py"],
    deps = [
        requirement("apache-beam"),
        "//tiingo_beam:endpoints",
        "//tiingo_beam:firehose",
    ],
)

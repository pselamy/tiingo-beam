load("@pip_deps//:requirements.bzl", "requirement")

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

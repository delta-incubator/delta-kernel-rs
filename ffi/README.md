# delta-kernel-rs ffi

This crate provides a c foreign function internface (ffi) for delta-kernel-rs.

## Building
You can build static and shared-libraries, as well as the include headers by simply running:

```sh
cargo build [--release] [--features default-engine]
```

to build and run the C program which excercises FFI:

```sh
table=../kernel/tests/data/table-without-dv-small make run
```



This will place libraries in the root `target` dir (`../target/[debug,release]` from the directory containing this README), and headers in `../target/ffi-headers`. In that directory there will be a `delta_kernel_ffi.h` file, which is the C header, and a `delta_kernel_ffi.hpp` which is the C++ header.

### C/C++ Extension (VSCode)

By default the VSCode C/C++ Extension does not use any defines flags. You can open `settings.json` and set the following line:
```
    "C_Cpp.default.defines": [
        "DEFINE_DEFAULT_ENGINE",
        "DEFINE_SYNC_ENGINE"
    ]
```
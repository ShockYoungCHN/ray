"""Standalone build script for ``external_storage_fast.pyx``.

Phase 1 ships outside of bazel.  Run this once after changes to the .pyx;
it produces ``external_storage_fast*.so`` next to the .pyx so the
runtime import in ``external_storage.py`` resolves cleanly:

    cd python/ray/_private && python build_external_storage_fast.py build_ext --inplace

Why not bazel right now?  The bazel ``pyx_library`` rule (used by
_raylet) carries a lot of Ray-internal symbol-export and ABI plumbing
that this module does not need — it only links against pyarrow, which
is a runtime-resolved Python import.  We will move into bazel once the
.pyx contract is validated end-to-end.
"""

from setuptools import setup, Extension
from Cython.Build import cythonize


extensions = [
    Extension(
        name="external_storage_fast",
        sources=["external_storage_fast.pyx"],
        # No C++ libs needed at link time — pyarrow is imported at
        # runtime via Python's standard import machinery.  Cython's
        # libc.string / libc.stdint cimports resolve from the system
        # libc, no extra link options.
        language="c++",
        extra_compile_args=["-std=c++17", "-O3"],
    ),
]

setup(
    name="external_storage_fast",
    ext_modules=cythonize(
        extensions,
        language_level=3,
        # Show Cython's annotation HTML for tuning — drop in production.
        annotate=False,
    ),
)

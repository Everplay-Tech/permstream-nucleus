from setuptools import setup
from torch.utils.cpp_extension import BuildExtension, CppExtension
import os

# Fleshed out PyTorch C++ Extension configuration
setup(
    name='permstream_pytorch_loader',
    version='0.1.0',
    description='PermStream Nucleus high-throughput data loader for PyTorch',
    ext_modules=[
        CppExtension(
            name='permstream_loader',
            sources=['src/extension.cpp'],
            # We would link against the libpermstream cdylib here in a real build
            # libraries=['permstream'],
            # library_dirs=[os.path.abspath('../../target/release')],
            extra_compile_args=['-O3', '-std=c++17'],
        )
    ],
    cmdclass={
        'build_ext': BuildExtension
    }
)

from setuptools import setup, find_packages

setup(
    name="permstream-torch",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "grpcio",
        "grpcio-tools",
        "torch",
        "numpy",
    ],
    author="PermStream Nucleus",
    description="High-throughput AI Data Loader for PermStream archives",
)

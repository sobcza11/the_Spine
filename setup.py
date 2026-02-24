from setuptools import setup, find_packages

setup(
    name="spine",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        # Add any dependencies your project needs, e.g., 'numpy', 'pandas', etc.
    ],
)
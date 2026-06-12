from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="delta-chronicle",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Cross-table temporal causality engine for Delta Lake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/YOUR_USERNAME/delta-chronicle",
    packages=find_packages(exclude=["tests*", "demo*"]),
    python_requires=">=3.10",
    install_requires=[
        "pyspark>=3.5.0",
        "delta-spark>=3.2.0",
        "networkx>=3.0",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
    ],
    keywords="spark delta-lake data-lineage causality databricks",
)
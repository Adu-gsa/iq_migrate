from setuptools import setup, find_packages

setup(
    name="data_ingestion",
    version="0.1.0",
    description="FOIA Data Ingestion Project",
    author="Your Name",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[],
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "wheel_smoke_test=ingest_table.wheel_entrypoints:wheel_smoke_test",
            "ingest_table_dispatcher=ingest_table.wheel_entrypoints:ingest_table_dispatcher",
            "bronze_to_silver_ingestion=ingest_table.wheel_entrypoints:bronze_to_silver_ingestion",
        ]
    },
)
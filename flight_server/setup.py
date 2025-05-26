from setuptools import setup, find_packages

setup(
    name="flight-server",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi>=0.109.0",
        "uvicorn>=0.27.0",
        "prometheus-client>=0.19.0",
        "duckdb>=0.11.0",
        "boto3>=1.34.0",
        "pyarrow>=20.0.0",
        "pydantic>=2.0.0",
        "psutil>=5.9.0"
    ],
    python_requires=">=3.12",
)

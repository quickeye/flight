from setuptools import setup, find_packages

setup(
    name="flight-client",
    version="0.1.0",
    description="Client library for interacting with the Flight Server",
    packages=find_packages(),
    install_requires=[
        "requests>=2.31.0",
        "pyarrow>=20.0.0",
        "pandas>=2.2.3"
    ],
    entry_points={
        'console_scripts': [
            'flight-client=flight_client.client:main'
        ]
    }
)

from setuptools import find_packages, setup

setup(
    name="motor-decorator",
    packages=find_packages(),
    version="0.0.3.4",
    description="Decorator for motor library",
    author="Timur Galiev",
    author_email="timkin12321@gmail.com",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/FeltsAzn/motor-decorator",
    license="LICENSE",
    install_requires=[
        "pydantic>=2.5.3",
        "motor>=3.3.2"
    ],
)

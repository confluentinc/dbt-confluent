#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-confluentcloud"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.7.0"
description = """The ConfluentCloud adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="fdolce@confluent.io",
    author_email="fdolce@confluent.io",
    url="https://github.com/confluentinc/dbt-confluent-cloud",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.7.0.",
        "dbt-common<1.0"
        "dbt-adapter~=0.1.0a2"
    ],
)

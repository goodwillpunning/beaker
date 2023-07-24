from setuptools import setup, find_packages

setup(
    name="beaker",
    version="0.1",
    packages=['beaker'],
    #package_dir={"": "src"},
    # packages=find_packages(where="src"),
    author="Will Girten",
    author_email="will.girten@databricks.com",
    description="Setup and run queries on Databricks dbsql warehouse.",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/goodwillpunning/beaker",
    #license='MIT',
)

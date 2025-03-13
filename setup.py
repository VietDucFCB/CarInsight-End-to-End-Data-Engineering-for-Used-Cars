from setuptools import setup, find_packages

setup(
    name="data-pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "apache-airflow>=2.7.1",
        "apache-airflow-providers-apache-kafka>=1.1.0",
        "kafka-python>=2.0.2",
        "confluent-kafka>=2.2.0",
        "pandas>=2.1.0",
        "numpy>=1.24.3",
        "beautifulsoup4>=4.12.2",
        "requests>=2.31.0",
        "minio>=7.1.15",
        "psycopg2-binary>=2.9.6",
        "flask>=2.3.3",
    ],
    author="Viet Duc",
    author_email="Kkagiuma1@gmail.com",
    description="Data pipeline with Airflow and Kafka",
    keywords="data, pipeline, etl, airflow, kafka",
    python_requires=">=3.9",
)
FROM debian:bookworm-slim

# Suppress interactive prompts.
ENV DEBIAN_FRONTEND=noninteractive

# Install utilities required by Spark scripts.
RUN apt update && apt install -y procps tini && apt clean

# Add extra jars.
ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars/
ENV SPARK_EXTRA_CLASSPATH='/opt/spark/jars/*'
RUN mkdir -p "${SPARK_EXTRA_JARS_DIR}"
COPY jars/postgresql-42.6.0.jar "${SPARK_EXTRA_JARS_DIR}"
COPY jars/spark-bigquery-with-dependencies_2.12-0.31.1.jar "${SPARK_EXTRA_JARS_DIR}"

# Install and configure Mambaforge.
ENV CONDA_HOME=/opt/mambaforge
ENV PYSPARK_PYTHON=${CONDA_HOME}/bin/python
ENV PYSPARK_DRIVER_PYTHON=${CONDA_HOME}/bin/python
ENV PATH=${CONDA_HOME}/bin:${PATH}
COPY scripts/Mambaforge-Linux-x86_64.sh ./
RUN bash Mambaforge-Linux-x86_64.sh -b -p ${CONDA_HOME} && rm Mambaforge-Linux-x86_64.sh

# Install Conda and pip packages.
COPY environment-dataproc.yml poetry.toml pyproject.toml ./
RUN mamba env update --name base --file environment-dataproc.yml --prune && mamba clean -afy
RUN poetry config virtualenvs.path /
RUN poetry env use ${PYSPARK_PYTHON}
RUN poetry install --no-root --no-cache --no-interaction && rm -rf /root/.cache/pypoetry

# Create the 'yarn_docker_user' group/user.
# The GID and UID must be 1099. Home directory is required.
RUN groupadd -g 1099 yarn_docker_user
RUN useradd -u 1099 -g 1099 -d /home/yarn_docker_user -m yarn_docker_user
RUN usermod -aG root yarn_docker_user
USER yarn_docker_user

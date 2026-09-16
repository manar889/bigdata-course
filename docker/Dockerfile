# syntax=docker/dockerfile:1.7
# ----------------------------------------------------------------------------
# Distributed Systems -> Big Data : student environment
#
# VERSION PINS ARE DELIBERATE. Read before "upgrading" anything:
#   Python 3.11  -> PySpark 3.5.x does NOT support 3.12. Bump Python and Spark dies.
#   Java 17      -> Spark 3.5 is certified on 8/11/17. Java 21 works for 4.x, not reliably 3.5.
#   PySpark 3.5.9-> matches "Learning Spark, 2nd Edition", which is your S4/S6/S9 reading.
#                   Spark 4.x renamed/changed enough UI and plan output to break your
#                   S7 screenshots mid-semester. Do not move to 4.x during a live course.
#
# BUILD CONTEXT IS THE REPO ROOT, not docker/:
#   docker buildx build -f docker/Dockerfile .
# ----------------------------------------------------------------------------
FROM python:3.11-slim-bookworm

ARG DEBIAN_FRONTEND=noninteractive

# procps/htop: students need to SEE memory being eaten in S1.
# curl: seed download. git: project work.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps htop curl ca-certificates git tini \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME is arch-dependent on Debian (amd64 vs arm64 for Apple Silicon).
# Resolving it dynamically is the difference between "works on my Mac" and "works".
RUN JH="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" \
    && echo "JAVA_HOME=$JH" > /etc/environment.java \
    && ln -sfn "$JH" /opt/java-home
ENV JAVA_HOME=/opt/java-home
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY docker/requirements.txt /tmp/requirements.raw

# Sanitize before pip sees it. A UTF-8 BOM or CRLF line endings — which is what you
# get if the file was ever opened in Notepad or downloaded through a browser — make
# pip exit with code 2 (parse error) and a message that does not mention either.
# Stripping them here costs nothing and removes a whole class of build failure.
RUN sed -e '1s/^\xEF\xBB\xBF//' -e 's/\r$//' /tmp/requirements.raw > /tmp/requirements.txt \
    && echo "--- requirements pip will actually read ---" \
    && cat -A /tmp/requirements.txt | head -20 \
    && echo "-------------------------------------------"

# PySpark has NO wheel on PyPI — it is a 303 MB source tarball, and pip cannot
# resume a broken download. One dropped connection and the whole build fails on a
# hash mismatch. So: if you pre-downloaded it with scripts/fetch_pyspark.sh, install
# from the local copy and touch the network zero times for that package.
COPY docker/vendor/ /tmp/vendor/
RUN pip install --no-cache-dir --upgrade pip \
    && if ls /tmp/vendor/pyspark-*.tar.gz >/dev/null 2>&1; then \
         echo ">>> installing PySpark from local vendor copy (no download)"; \
         pip install --no-cache-dir --no-build-isolation /tmp/vendor/pyspark-*.tar.gz; \
       else \
         echo ">>> no vendored PySpark found."; \
         echo ">>> On a slow or unstable connection this next step WILL fail."; \
         echo ">>> Stop and run:  bash scripts/fetch_pyspark.sh"; \
       fi

RUN pip install --no-cache-dir \
         --retries 20 --timeout 180 \
         -r /tmp/requirements.txt

# Non-root, UID 1000. On Linux this is what stops every file the student creates
# in a bind mount from being owned by root.
ARG USER_UID=1000
ARG USER_GID=1000
RUN groupadd -g "${USER_GID}" student || true \
    && useradd -m -u "${USER_UID}" -g "${USER_GID}" -s /bin/bash student

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
# Spark inside a container guesses its own hostname and then cannot bind to it.
# Must be 0.0.0.0, NOT 127.0.0.1: Spark's WebUI.bind() uses this env var directly as
# the Jetty bind host (spark.driver.bindAddress does not apply to the UI). At
# 127.0.0.1 the UI listens on container loopback only and the published 4040 port
# reaches nothing — which kills S7.
ENV SPARK_LOCAL_IP=0.0.0.0
# Arrow makes toPandas() usable on the sizes we hand them.
ENV PYARROW_IGNORE_TIMEZONE=1

COPY docker/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY docker/jupyter_server_config.py /etc/jupyter/jupyter_server_config.py
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY scripts/prepare_tp_data.py scripts/diagnostic.py /opt/course/
COPY notebooks/ /opt/course/notebooks/
RUN chmod +x /usr/local/bin/entrypoint.sh \
    && mkdir -p /data /work /home/student/.ivy2 /tmp/spark-events \
    && chown -R student:student /data /work /home/student /tmp/spark-events /opt/course

USER student
WORKDIR /work

EXPOSE 8888 4040 4041

# Fails the build if Spark cannot actually start a session. Catching this at build
# time instead of at 09:00 on the day of S1 is the entire point.
RUN python -c "\
from pyspark.sql import SparkSession; \
s = SparkSession.builder.master('local[1]').appName('build-check').getOrCreate(); \
assert s.range(10).count() == 10; \
print('BUILD CHECK OK — Spark', s.version); \
s.stop()"

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
CMD ["lab"]

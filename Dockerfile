# Dockerfile
# this time including: multi-stage builds!

####### first stage (temporary)
FROM python:3.11.2-slim-bullseye AS builder

# preserve disk space by putting two commands in one layer
RUN apt-get update && \
    apt-get upgrade --yes

# keep docker from giving the user root-level access to ur machine
RUN useradd --create-home eso_analysis
USER eso_analysis
WORKDIR /home/eso_analysis

# make a virtual environment to run in to avoid version conflicts
ENV VIRTUALENV=/home/eso_analysis/venv
RUN python3 -m venv $VIRTUALENV
ENV PATH="$VIRTUALENV/bin:$PATH"

# copy over project dependencies
COPY --chown=eso_analysis pyproject.toml constraints.txt ./
# note: only copy individual files you need at the moment in the Dockerfile
# otherwise will need to rebuild for unrelated file changes
# can also use a .dockerignore file

# upgrade pip and setuptools to avoid version issues and vulnerabilities
RUN python -m pip install --upgrade pip setuptools && \
    python -m pip install --no-cache-dir -c constraints.txt ".[dev]"
# no need to cache bc we won't need the packages outside the venv

# copy over the source code
COPY --chown=eso_analysis src/ src/
COPY --chown=eso_analysis test/ test/

# run tests, linters, security checkers
# note: only unit tests run here because integration/e2e need Redis container
RUN python -m pip install . -c constraints.txt && \
    python -m pytest test/unit/ && \
    python -m isort src/ --check && \
    python -m black src/ --check --quiet && \
    python -m bandit -r src/ --quiet && \
    python -m pip wheel --wheel-dir dist/ . -c constraints.txt ".[dev]"

##### second stage
FROM python:3.11.2-slim-bullseye

RUN apt-get update && \
    apt-get upgrade --yes

RUN useradd --create-home eso_analysis
USER eso_analysis
WORKDIR /home/eso_analysis

ENV VIRTUALENV=/home/eso_analysis/venv
RUN python3 -m venv $VIRTUALENV
ENV PATH="$VIRTUALENV/bin:$PATH"

# copy over wheel from first stage
COPY --from=builder /home/eso_analysis/dist/eso_sentiment_analysis*.whl /home/eso_analysis
# upgrade pip n install wheel
RUN python -m pip install --upgrade pip setuptools && \
    python -m pip install --no-cache-dir eso_sentiment_analysis*.whl

CMD /bin/bash

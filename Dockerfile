FROM continuumio/miniconda3

RUN apt-get update --yes && apt-get install --yes tmux vim less

COPY environment.yml /opt/build/environment.yml
RUN conda env create -f /opt/build/environment.yml -q

COPY pyproject.toml /opt/build/pyproject.toml
COPY src /opt/build/src
WORKDIR /opt/build
RUN conda run -n aqm-eval bash -c "pip install --no-deps . && pip list"
RUN conda run -n aqm-eval bash -c "aqm-data-sync --help"
RUN conda run -n aqm-eval bash -c "aqm-mm-eval --help"

WORKDIR /opt
RUN rm -rf /opt/build

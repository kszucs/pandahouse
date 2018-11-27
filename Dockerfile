FROM continuumio/miniconda3

ARG PYTHON_VERSION=3.6
RUN conda install -c conda-forge \
        python=$PYTHON_VERSION \
        libgcc \
        pandas \
        numpy \
        requests && \
    conda clean --all

ADD . /pandahouse
WORKDIR /pandahouse

CMD python setup.py test

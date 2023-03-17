FROM python:3.11
ENV PYTHONUNBUFFERED=1

RUN mkdir /pipelines
WORKDIR /pipelines
RUN mkdir /pipelines/db
RUN mkdir /pipelines/pipelines
RUN mkdir /pipelines/example_pipeline

COPY /db /pipelines/db
COPY /pipelines /pipelines/pipelines
COPY /example_pipeline /pipelines/example_pipeline
COPY .gitignore /pipelines
COPY norm.csv /pipelines
COPY original.csv /pipelines
COPY poetry.lock /pipelines
COPY pyproject.toml /pipelines
COPY README.md /pipelines
COPY setup.py /pipelines
COPY TODO.md /pipelines

RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install

ENTRYPOINT ["python", "./example_pipeline/pipeline.py"]
#CMD [""]
FROM python:3.11-slim
ENV PYTHONIOENCODING utf-8

RUN apt-get update && apt-get install -y build-essential curl

COPY requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
COPY flake8.cfg /code/flake8.cfg
RUN pip install flake8

COPY ./src /code/src

WORKDIR /code/

CMD ["python", "-u", "/code/src/run.py"]

FROM python:3.6-slim

RUN pip3 install pipenv

RUN groupadd --gid 1000 pubsub && useradd --create-home --system --uid 1000 --gid pubsub pubsub
WORKDIR /home/pubsub
CMD ["python3", "run.py"]

COPY Pipfile* /home/pubsub/
RUN pipenv install --deploy --system
USER pubsub

COPY --chown=pubsub . /home/pubsub
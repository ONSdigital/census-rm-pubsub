# To test locally

```bash
gcloud components install pubsub-emulator
gcloud components update

# In a separate terminal
gcloud beta emulators pubsub start

gcloud beta emulators pubsub env-init
# Outputs value to use for PUBSUB_EMULATOR_HOST

docker run -d -p 5672:5672 --name rabbitmq rabbitmq:3.6.10-management

cat > .env << EOS
RABBIT_AMQP=amqp://guest:guest@localhost:5672
GCP_PROJECT_ID=project
PUBSUB_EMULATOR_HOST=localhost:8085
EOS

pipenv install --dev
pipenv shell

python test/create_topic.py project eq-submission-topic
python run.py

# In a separate terminal
pipenv run python test/publish_message.py project eq-submission-topic
...
```
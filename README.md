## Suggested Skeleton
- app.py
- config.py
- database.py
- Dockerfile
- requirements.txt

#### app.py
For application entrypoint, this is where all application logics stay

#### config.py
For all environment variable configuration

#### database.py
For database configuration and definition

#### Dockerfile
You know, for define docker image that will run on production

#### requirements.txt
All use packages need to define here

### START PROXYSQL
sudo docker run -v ~/.ssh:/.ssh -p 25432:5432 gcr.io/cloudsql-docker/gce-proxy:1.16 /cloud_sql_proxy --instances="leo-dataplatform:us-central1:leo-web-application-development"=tcp:0.0.0.0:5432 --credential_file=/.ssh/leo-dataplatform-dev.json

### TEST CHART
http://localhost:5000/chart?start_date=2020-12-01&end_date=2020-12-28&case_study_id=51&code_name=VOC_1_3_1

#### Docker Compose
Make sure all gcp credential files are placed in local folder
docker-compose up

### UNIT TEST
export GOOGLE_APPLICATION_CREDENTIALS=creds.json
pytest tests/test_app.py::test_sync_request

# Bigeye Training - Sample Repository

This is a [Bigeye](https://bigeye.com) project for demonstration and training purposes. This project utilizes airflow to orchestrate dbt for transformations and Bigeye for data observability/reliability.


### Running Airflow locally

This project uses a Makefile to consolidate common commands and make it easy for anyone to get started. To run Airflow locally, simply:

        make start-airflow

This command will build your local Airflow image and start Airflow automatically!

Navigate to http://localhost:8080/ and start writing & testing your DAGs! Login with the user-password combo: `admin:admin` (you can change this in `docker-compose.yaml`).

You'll notice in `docker-compose.yaml` that both DAGs and plugins are mounted as volumes. This means once Airflow is started, any changes to your code will be quickly synced to the webserver and scheduler. You shouldn't have to restart the Airflow instance during a period of development! 

When you're done, simply:

        make stop-airflow


### Cleaning up your local environment

If at any point you simply want to clean up or reset your local environment, you can run the following commands:

Reset your local docker-compose:

        make reset-airflow

Rebuild the local Airflow image for docker-compose (useful if you make changes to the Dockerfile):
        
        make rebuild-airflow

Reset your virtual environment:

        make clean-venv

Start completely from scratch:

        make clean-all
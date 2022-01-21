# Dagster Canvas LMS API to Ed-Fi AMT
This repository contains code that extracts Canvas assignment submission data from the Canvas API to create the Assignment and Assignment Submission facts found in Ed-Fi's Analytics Middle Tier. Between the Canvas data models and AMT, this repo also creates the Ed-Fi LMS Assignment and Assignment Submission data models to be future ready for the upcoming Ed-Fi LMS API.

> :warning: **Caution:** This repo is fully built out to extract Canvas data to Google Cloud Storage and create staging tables in BigQuery. The Canvas to AMT SQL is not complete. If you use an Ed-Fi certified SIS and Canvas, please reach out and I would be happy to complete and deploy the codebase for you.


![Canvas ETL](/assets/canvas_etl.png)

This repository is designed to be opened on a machine with [Docker](https://www.docker.com/) installed. When opened in [Visual Studio Code](https://code.visualstudio.com/) with the [Remote Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension installed, Visual Studio Code can open the repository in its own container using all Python requirements specified in this repository's `requirements.txt` file.

At the root of this repo is a `.env-sample` file. Copy the file to create a `.env` file. Complete the following missing values:

* CANVAS_BASE_URL
* CANVAS_ACCESS_TOKEN
* SCHOOL_YEAR_START_DATE

You will complete the other missing values in the steps below.

## Google Cloud Configuration
Create a Google Cloud Platform (GCP) project and set the `GCP_PROJECT` variable to the Google Cloud project ID.

### Service Account
Authentication with the GCP project happens through a service account. In GCP, head to _IAM & Admin --> Service Accounts_ to create your service account.

* Click **Create Service Account**
* Choose a name (ie. dagster) and click **Create**
* Grant the service account the following roles
    * BigQuery Admin
    * Storage Admin
* Click **Done** 
* Select the actions menu and click **Create key**. Create a JSON key, rename to _service.json_ and store in the root of the repository.

### Google Cloud Storage
Create a Google Cloud Storage bucket that will be used to house the CSV files retrieved from the NWEA MAP API. In GCP, head to _Cloud Storage_ and click **Create Bucket**. Once created, set the `GCS_BUCKET_DEV` variable to the newly created bucket's name (ie. dagster-dev-123).


## Launching Dev Job
In Visual Studio Code, with the repo opened in a container, run the command below:


```bash

dagit -w workspace.yaml;

```

The command above launches dagit, Dagster's web UI. The menu top-left will allow you to access the NWEA MAP related job.

* Click **Launchpad**
* Click **Launch Run**

## creating local pipeine for ingestion 05/30/2023

1. Airflow operator that interacts directly with postgres
require a db connection made from the airflow GUI under admin>connections.

2. in order to connect to a local dockerized postgres instance that is not
part of the airflow's set of containers:
  a. the db user had to be created, permissions determined, pg_hba.conf edited
  b. the local pg instance had to be included in the airflows docker stack's network, otherwise airflow will not connect to the external db. 

## pipeline flow 

1. drop packet into network folder 
- [ ] volume to drop-in raw data
- airflow continuously checks the folder to progress 
- [ ] airflow task to cross-check volume with current data
2. packet found, projects excel file for that specific job is retrieved
- there may be projects that share a projectkey but are available to ingest on separate occasions.for example, I may ingest NDOW, but 2 days later another NDOW datase surfaces. what then
- if project file is a repeat, do not process project excel file 
3. assess the available tables
4. create aggregate tables, store in local volume 
5. PASS PATH TO NEXT DAG THROUGH XCOM
6. retreive aggregate table to perform transforms
7. send transformed data to postgres
- update a ingest table

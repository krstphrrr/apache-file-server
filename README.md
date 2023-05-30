## creating local pipeine for ingestion 05/30/2023

1. Airflow operator that interact directly with postgres
require a db connection made from the airflow GUI under admin>connections.

2. in order to connect to a local dockerized postgres instance that is not
part of the airflow's set of containers:
  a. the db user had to be created, permissions determined, pg_hba.conf edited
  b. the local pg instance had to be included in the airflows docker stack's network, otherwise airflow will not connect to the external db. 

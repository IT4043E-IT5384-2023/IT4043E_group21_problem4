# group21_problem4
In order to profile the user wallet, we divide the task into 2 main tasks:

- Cluster user wallet based on the type of projects they join in then label them as an entity.
- Identify these entity behavior (pattern) among these projects.

More specifically, we consider to take  the transaction database, project database and smart contract database from Centic.

Step query:
-

 - First we try to collect list of project:
    - First of you need to downloads the knowledge_graph.projects.json file 
    from Mongodb KLG and add it to folder data.
    - Then runfile project.py
- After that we try to collect the users address that join the project:
    - Run participant.py
- Next we get all transaction/event lending/ transfer on each user address:
    - Run user_lending.py/user_transaction.py/user_transfer.py


## Lending events
### Dependencies
Prepare file `participants.json` at directory `data`

### Crawling process
```bash
python ./src/lending_events/crawler.py -o ./data/lending_events/raw/repay.py -t repay -j 4
```

### ETL process
```bash
python ./src/lending_events/etl.py
```
import json
import copy
import logging
from tqdm import tqdm
from collections import defaultdict

import pandas as pd
import psycopg2

from utils.logger import setup_logging


logger = logging.getLogger("project_filter")


def main():
    with open("./data/knowledge_graph.projects.json", encoding="utf-8") as f:
        kg = json.load(f)
    df = pd.DataFrame(kg)

    conn = psycopg2.connect(
        database="postgres",
        host="34.126.75.56",
        port="5432",
        user="student_token_transfer",
        password="svbk_2023"
    )

    cur = conn.cursor()
    chain_ids = ["0x1", "0x38", "0x89", "0x2b6653dc", "0xa", "0xa4b1", "0xa86a", "0xfa"]

    postgres = []
    deployed_chains = defaultdict(list)
    for cid in chain_ids:
        cur.execute(f"SELECT DISTINCT(project) FROM chain_{cid}.smart_contract")
        result = cur.fetchall()
        postgres += result
        for r in result:
            deployed_chains[r[0]].append(cid)

    postgres = [p[0].lower() for p in postgres]
    logger.info("Number of project collected from postgres is %d", len(postgres))

    char = set("".join(postgres))
    logger.info(sorted(char))


    mongo = copy.deepcopy(df["id"].values.tolist())
    mongo = [m.lower().replace("-", "_") for m in mongo]
    logger.info("Number of project collected from knowledge_graph on mongodb is %d", len(mongo))

    char = set("".join(mongo))
    logger.info(sorted(char))


    set_postgres = set(postgres)
    contains = []
    not_found = []
    for m in mongo:
        if m in set_postgres:
            contains.append(m)
        else:
            not_found.append(m)
    
    logger.info(len(contains))

    logger.info("Collect contract addresses ...")
    contains = set(contains)
    projects = []
    required_chain_id = "0x1"
    for k in tqdm(kg):
        project_id = k["id"]
        if project_id in contains and required_chain_id in deployed_chains[project_id]:
            cur.execute(f"SELECT contract_address FROM chain_{required_chain_id}.smart_contract WHERE project = '{project_id}'")
            k["contract_addresses"] = [a[0] for a in cur.fetchall()]
            projects.append(k)

    logger.info("Number of collected project: %d", len(projects))

    with open("./data/projects.json", "w", encoding="utf-8") as f:
        json.dump(projects, f, indent=4)


if __name__ == "__main__":
    setup_logging(include_time=True)
    main()
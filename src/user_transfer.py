import json
import psycopg2


def get_transaction(numm):
    db_params = {
        'host': '34.126.75.56',
        'database': 'postgres',
        'user': 'student_token_transfer',
        'password': 'svbk_2023',
        'port': '5432'
    }

    # Specify the table and fields you want to query
    table_name = 'chain_0x1.token_transfer'

    # Construct the SQL query with a WHERE clause
    class_values = ', '.join([f"'{cls}'" for cls in numm])
    query = f"SELECT * FROM {table_name} WHERE from_address IN ({class_values})"
    fields_to_select=['contract_address','transaction_hash','log_index','block_number','from_address','to_address','value']

    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        output_data = []
        for row in results:
            output_data.append(dict(zip(fields_to_select, row)))
        output_file_path ='./data/transfer.json'
        with open(output_file_path, 'w') as json_file:
            json.dump(output_data, json_file, indent=2)

        print(f"Results saved to {output_file_path}")

    except Exception as ex:
        print(ex)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    f = open('./data/participant/participants.json')
    data = json.load(f)
    lst =['ens', 'cryptokitties', 'sandbox', 'otherdeed', 'clonex']
    usr_lst = []
    for i in range(len(lst)):
        usr_lst.extend(data[lst[i]])
    num_usr = list(set(usr_lst))
    get_transaction(num_usr)

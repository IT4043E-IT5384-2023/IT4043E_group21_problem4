import pandas as pd
import json
f = open('../data/query_data/participants.json')
data = json.load(f)
fil_contrac = pd.read_csv("../data/filter_data/filter.csv"
)
fil_contrac = fil_contrac[fil_contrac['IsContract'] == False]
print(fil_contrac)
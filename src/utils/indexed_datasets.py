import pickle
import os
from copy import deepcopy

import numpy as np


class IndexedDataset:
    def __init__(self, path: str, num_cache=1):
        super().__init__()
        self.path           = path
        self.data_file      = None
        if path.endswith(".data") or path.endswith(".idx"):
            path = os.path.splitext(path)[0]
        self.data_offsets   = np.load(f"{path}.idx", allow_pickle=True).item()['offsets']
        self.data_file      = open(f"{path}.data", 'rb', buffering=-1)
        self.cache          = []
        self.num_cache      = num_cache

    def check_index(self, i):
        if i < 0 or i >= len(self.data_offsets) - 1:
            raise IndexError('index out of range')

    def __del__(self):
        if self.data_file:
            self.data_file.close()

    def __getitem__(self, i):
        self.check_index(i)
        if self.num_cache > 0:
            for c in self.cache:
                if c[0] == i:
                    return c[1]
        self.data_file.seek(self.data_offsets[i])
        b = self.data_file.read(self.data_offsets[i + 1] - self.data_offsets[i])
        item = pickle.loads(b)
        if self.num_cache > 0:
            self.cache = [(i, deepcopy(item))] + self.cache[:-1]
        return item

    def __len__(self):
        return len(self.data_offsets) - 1

    @classmethod
    def exist_dataset(cls, data_dir: str, prefix: str):
        if os.path.exists(data_dir) is False or os.path.isdir(data_dir) is False:
            return False
        fns = [
            os.path.join(data_dir, prefix + ".idx"),
            os.path.join(data_dir, prefix + ".data")
        ]
        for fn in fns:
            if os.path.exists(fn) is False or os.path.isfile(fn) is False:
                return False
        return True


class IndexedDatasetBuilder:
    def __init__(self, path):
        if path.endswith(".data") or path.endswith(".idx"):
            path = os.path.splitext(path)[0]
        self.path = path
        self.out_file = open(f"{path}.data", 'wb')
        self.byte_offsets = [0]

    def add_item(self, item):
        s = pickle.dumps(item)
        bytes = self.out_file.write(s)
        self.byte_offsets.append(self.byte_offsets[-1] + bytes)

    def finalize(self):
        self.out_file.close()
        np.save(open(f"{self.path}.idx", 'wb'), {'offsets': self.byte_offsets})


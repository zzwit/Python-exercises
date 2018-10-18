
from random import randint
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

x = [randint(0,350) for i in range(100)]
y = [randint(0,600) for i in range(100)]
chunkSize = 20000;
reader = pd.read_csv('../ceshi.txt', iterator=True)
chunk = reader.get_chunk(chunkSize)

height = chunk['cy']
weight = chunk['cx']

plt.scatter(weight,height)
plt.xlim(0, 360)
plt.ylim(650, 0)

plt.xlabel('x')
plt.ylabel('y')
# ax = plt.gca()
# ax.invert_yaxis()
plt.show()
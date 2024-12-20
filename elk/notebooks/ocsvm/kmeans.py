# Train the KMeans Model

# Load packages
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn import preprocessing
from tqdm import tqdm
import pickle
import joblib


# %matplotlib inline
kscaler = joblib.load('notebooks/ocsvm/transformers/kscaler.pickle')
# Importing the dataset
data = pd.read_csv('Attack.csv', delimiter=',')
data = data[data['Normal/Attack']=='Attack']
data = data.drop(['Timestamp','Normal/Attack'],axis=1)
data = kscaler.transform(data)
print(data)
# Fit

kmeans = KMeans(n_clusters = 4).fit(np.asarray(data))

# Save model: It is important to use binary access
with open('notebooks\ocsvm\models\ocsvm_kmeans.pickle', 'wb') as f:
    pickle.dump(kmeans, f)
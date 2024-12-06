from sklearn.preprocessing import OneHotEncoder, StandardScaler
import pickle
import pandas as pd
import numpy as np
import pandas as pd
import numpy as np
import pickle

def load_data(path):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        print(f"File not found at {path}")
    except pd.errors.EmptyDataError:
        print(f"No data found in {path}")
        return None

def preprocessing(df):
    constant_columns = ['P202', 'P401', 'P404', 'P502', 'P601', 'P603','Normal/Attack']
    if set(constant_columns).issubset(df.columns):
        df = df.drop(constant_columns, axis=1)
    
    # Convert Timestamp to datetime
    if 'Timestamp' in df.columns:
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], dayfirst=True, errors='coerce')
    
    # Identify missing values
    if df.isnull().sum().any():
        print("Missing values detected.")
        df.dropna(inplace=True)
    
    # Categorize columns
    dis_col, num_col = [], []
    for f in df.columns:
        if df[f].nunique() > 10:
            num_col.append(f)

        else:
            dis_col.append(f)
    num_col = num_col[1:] 
    return df, num_col, dis_col





def scale_and_encode(df, num_col, dis_col):
    # Scaling numerical columns
    if num_col:
        scaler = StandardScaler()
        df[num_col] = scaler.fit_transform(df[num_col])
        with open('notebooks/transformers/scaler.pickle', 'wb') as f:
            pickle.dump(scaler, f)
    
    # Encoding discrete columns
    if dis_col:
        encoder = OneHotEncoder(sparse=False, drop='first')
        encoded_data = encoder.fit_transform(df[dis_col])
        
        # Check if .get_feature_names_out() is available
        if hasattr(encoder, 'get_feature_names_out'):
            encoded_columns = encoder.get_feature_names_out(dis_col)
        else:
            # For older versions of sklearn
            encoded_columns = encoder.get_feature_names(dis_col)
        
        # Create a DataFrame for the encoded data with proper column names
        encoded_df = pd.DataFrame(encoded_data, columns=encoded_columns, index=df.index)
        
        # Drop original discrete columns and join the new encoded columns
        df = df.drop(dis_col, axis=1).join(encoded_df)
        
        # Save the encoder to a file
        with open('notebooks/transformers/encoder.pickle', 'wb') as f:
            pickle.dump(encoder, f)
    
    return df

# Load data
path = "Normal.csv"
df = load_data(path)

if df is not None:
    df, num_col, dis_col = preprocessing(df)
    
    print(f"Numerical Columns: {num_col}")
    print(f"Discrete Columns: {dis_col}")
    
    # Scale and encode the data
    df = scale_and_encode(df, num_col, dis_col)
    print("Data after scaling and encoding:")
    print(df.head())
    df.to_csv(f"./data/SWaT_train.csv", sep = ',', encoding = 'utf-8', index = False)
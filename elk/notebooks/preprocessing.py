import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import pickle
def dataEngineering(filepath, actuators_NAMES):
    # Load data
    normal_data = pd.read_csv(filepath)
    normal_data.set_index('Timestamp', inplace=True)

    # Remove the last column
    normal_data = normal_data.iloc[:, :-1]

    to_drop = ['P402' ,'P203','FIT501','FIT504','P501' ,'FIT502','AIT502' ,'FIT201' ,'MV101','PIT501','PIT503' ,'AIT504' ,'MV201' ,'MV302' ,'FIT503','P302' ,'FIT301' ,'UV401'] 

            
    remained_cols= [i for i in normal_data.columns if i not in to_drop]
    
    # Drop highly correlated features
    normal_data.drop(columns=to_drop, inplace=True)

    # Filter actuator names that are still in the dataset
    actuators_NAMES = [col for col in actuators_NAMES if col in normal_data.columns]

    # Separate sensors and actuators
    sensors = normal_data.drop(columns=actuators_NAMES)
    
    sens_cols = sensors.columns
    print(sens_cols)
    actuators = normal_data[actuators_NAMES]
    scaler = MinMaxScaler()
    # Fit and transform the sensors data using the scaler
    scaler.fit(sensors)
    sensors = scaler.transform(sensors)
    with open('notebooks/transformers/scaler.pickle', 'wb') as f:
        pickle.dump(scaler, f)
    # Convert normalized data back to a DataFrame
    sensors = pd.DataFrame(sensors, columns=sens_cols)

    # Create one-hot encoded dummies for actuators
    actuators_dummies = actuators.copy()
    for actuator in actuators_NAMES:
        actuators_dummies[actuator] = pd.Categorical(actuators_dummies[actuator], categories=[0, 1, 2])
        actuators_dummies = pd.get_dummies(actuators_dummies, columns=[actuator], dtype=int)

    # Ensure index consistency
    sensors.index = actuators_dummies.index

    # Concatenate sensors and actuators
    allData = pd.concat([sensors, actuators_dummies], axis=1)

    return allData , remained_cols


path = 'Normal.csv'
actuators_NAMES=['P101', 'P102', 'P201', 'P202', 'P204', 'P205', 'P206', 'MV301', 
                           'MV303', 'MV304', 'P301', 'P401', 'P403', 'P404', 'P502', 'P601', 'P602', 'P603']
allData , remained_cols= dataEngineering(path,actuators_NAMES)
print(allData)
allData.to_csv('data/SWaT_Train.csv')

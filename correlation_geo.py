
import numpy as np 
import xarray as xr
import matplotlib.pyplot as plt
from scipy.fft import fft, fftfreq
from scipy import stats 
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
import pandas as pd
import datetime as dt

#velocity, temperature, salinity, mixed layer depth
mld = xr.open_dataset('copernicus-data/mld_day.nc')
flow = xr.open_dataset('copernicus-data/vel_day.nc')
temp = xr.open_dataset('copernicus-data/temp_day.nc')
salt = xr.open_dataset('copernicus-data/salinity_day.nc')


#chlorophyll, nitrate, oxygen
chlor = xr.open_dataset('copernicus-data/chlor_day.nc')
no3 = xr.open_dataset('copernicus-data/no3_day.nc')
o2 = xr.open_dataset('copernicus-data/o2_day.nc')


lat  = chlor.latitude
lon = chlor.longitude
time = chlor.time

mld_ = mld.mlotst
flow_ = flow.uo
temp_ = temp.thetao
salt_ = salt.so

chlor_ = chlor.chl
no3_ = no3.no3
o2_ = o2.o2


def lin_reg(var1, var2, d1):
    lat = d1.latitude
    lon = d1.longitude
    time = d1.time
    df = xr.Dataset({"Var 1": var1, "Var 2": var2}).to_dataframe().dropna()
    df = df.reset_index()

    df['time'] = pd.to_datetime(df['time']).map(dt.datetime.toordinal)  #convert to ordinal for linear regression

    X = df[['latitude', 'longitude', 'time', "Var 1" ]]
    y = df["Var 2"]

    model = LinearRegression()
    model.fit(X,y)

    df['predicted var1'] = model.predict(X)


    from sklearn.metrics import r2_score

    r2 = r2_score(y, df['predicted var1'])
    print(f"R-squared Score: {r2:.4f}")



#I don't think that this works well
def pearson_flat(var1, var2):
    var1_flat = var1.values.flatten()
    var2_flat = var2.values.flatten()
    valid_mask = ~np.isnan(var1_flat) & ~np.isnan(var2_flat)
    var1_flat = var1_flat[valid_mask]
    var2_flat = var2_flat[valid_mask]
    corr, _ = pearsonr(var1_flat, var2_flat)
    return corr


import pandas as pd
import numpy as np
import yaml
import datetime
import sys, os, re, csv
from swmm_api import read_out_file

if len(sys.argv) > 3:
    experiment = sys.argv[1] # Mangwon or Bellinge
    ex_no = sys.argv[2] # 3
    rain_event = sys.argv[3]
ex_no = '%3.3d'%int(ex_no)

event_lst={'event1':{'analysis_date':'201206290000', 'end_date':'201207020000'},
           'event1-1':{'analysis_date':'201206290400', 'end_date':'201206290900'},
           'event1-2':{'analysis_date':'201206291600', 'end_date':'201206291900'},
           'event1-3':{'analysis_date':'201206300830', 'end_date':'201206301100'},
           'event1-4':{'analysis_date':'201207010330', 'end_date':'201207010530'},
           'event1-4':{'analysis_date':'201207010330', 'end_date':'201207010530'},
           'event2':{'analysis_date':'201508310000', 'end_date':'201509020000'},
           'event2-1':{'analysis_date':'201508310730', 'end_date':'201508310930'},
           'event2-2':{'analysis_date':'201508312000', 'end_date':'201508312200'},
           'event3':{'analysis_date':'201808110000', 'end_date':'201808120000'},
           'event3-1':{'analysis_date':'201808111200', 'end_date':'201808311400'}}

analysis_date = event_lst[rain_event]['analysis_date']
end_date = event_lst[rain_event]['end_date']

# Control 
control_file = open('./exp/'+experiment+ex_no+'/control.yaml', 'r')
p = yaml.safe_load(control_file)
control_file.close()
root_dir = p['meta']['root_dir']
start_date = analysis_date
cycle = p['da']['cycle']
interval = p['da']['interval']
nmember = p['da']['nmember']

# Directories
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast/'+analysis_date
obs_dir = root_dir+'/exp/'+experiment+ex_no+'/observation'
obs_file = root_dir+'/exp/'+experiment+ex_no+'/observation/obs'+analysis_date

# Water Gage
wg_dic = {}
obssite_file = const_dir+'/obssites.txt'
text_file = open(obssite_file,'r')
line = text_file.readlines()
text_file.close()
for i in range(1,len(line)):
   tmp = re.split('\t',line[i].strip())
   nchar = tmp.count('')
   for j in range(nchar): tmp.remove('')
   if len(tmp) == 0: continue
   wg_dic[tmp[0].strip()] = tmp[1].strip() 
print(wg_dic)

# Datetime
date = datetime.datetime(int(start_date[:4]),int(start_date[4:6]),int(start_date[6:8]),int(start_date[8:10]),int(start_date[10:12]))
end_date = datetime.datetime(int(end_date[:4]),int(end_date[4:6]),int(end_date[6:8]),int(end_date[8:10]),int(end_date[10:12]))
analysis_date = date.strftime('%Y%m%d%H%M')

# Observation
while date < end_date:
    obs_file = root_dir+'/exp/'+experiment+ex_no+'/observation/obs'+analysis_date
    obs_data = pd.read_csv(obs_file, sep = '\s+', engine='python')
    obs_data['Datetime'] = pd.to_datetime(obs_data['Datetime'], format='%Y%m%d%H%M')
    obs_data = obs_data.set_index('Datetime')
    output_file = root_dir+'/exp/'+experiment+ex_no+'/forecast/'+analysis_date+'/output00000000'
    opt = read_out_file(output_file)
    simulation_data = pd.DataFrame()
    sim_value = pd.Series()
    for wg,pp in wg_dic.items():
        obs_data[wg][(obs_data[wg] == -9999)] = np.NaN
        obs_data[wg] = obs_data[wg] * 0.3048
        sim_value = opt.get_part('node',pp).depth
        simulation_data[wg] = sim_value
    if analysis_date == start_date: 
        obsflow = obs_data#/0.3048
        simflow = simulation_data
    else:
        obsflow = pd.concat([obsflow, obs_data])
        simflow = pd.concat([simflow, simulation_data])
    date += datetime.timedelta(minutes=cycle)
    analysis_date = date.strftime('%Y%m%d%H%M')

rmse = []
for wg,pp in wg_dic.items():
    error_df = pd.DataFrame()
    obsflow[wg].name = 'actual'
    simflow[wg].name = 'predict'
    error_df = pd.concat([obsflow[wg],simflow[wg]], axis = 1)
    error_df['error'] = error_df['actual']-error_df['predict']
    error_df['squared error'] = error_df['error'] ** 2
    mse = error_df['squared error'].mean()
    rmse.append(np.sqrt(mse))
print(rmse)
rmse_file = root_dir + '/exp/' + experiment + ex_no + '/rmse_{}.csv'.format(rain_event)
wg_lst = []
with open(rmse_file, 'w', newline='') as file:
    writer = csv.writer(file)
    for wg,pp in wg_dic.items():
        wg_lst.append(wg)
    writer.writerows([wg_lst])
    writer.writerows([rmse])
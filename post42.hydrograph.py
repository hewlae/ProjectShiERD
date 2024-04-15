import pandas as pd
from numpy import *
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import seaborn as sns
import json, yaml
import datetime
import sys, os, re
from swmm_api import read_out_file

def plot_calibrated(graph_color, graph_width):
    # Datetime
    date = datetime.datetime(int(start_date[:4]),int(start_date[4:6]),int(start_date[6:8]),int(start_date[8:10]),int(start_date[10:12]))
    analysis_date = date.strftime('%Y%m%d%H%M')
    # Calibrated ver.(Pedersen et al.(2021)) is Bellinge 300
    ex_no_cal = '110' # Gaussian : '100', Pareto : '200' or '300'
    control_file_cal = open('./exp/'+experiment+ex_no_cal+'/control.yaml', 'r')
    p = yaml.safe_load(control_file_cal)
    control_file_cal.close()
    forecast_dir = root_dir+'/exp/'+experiment+ex_no_cal+'/forecast/'+analysis_date
    cycle_cal = p['da']['cycle']
    while date < end_date:
        output_file = forecast_dir+'/output00000000'
        opt = read_out_file(output_file)
        simulation_data = pd.DataFrame()
        sim_value = pd.Series()
        sim_value = opt.get_part('node',pp).depth
        simulation_data[wg] = sim_value
        if analysis_date == start_date: 
            simflow = simulation_data
        else:
            simflow = pd.concat([simflow, simulation_data])
        date += datetime.timedelta(minutes=cycle_cal)
        analysis_date = date.strftime('%Y%m%d%H%M')
        forecast_dir = root_dir+'/exp/'+experiment+ex_no_cal+'/forecast/'+analysis_date
    print(wg, pp, 'calbirated')
    sns.lineplot(x = simflow.index, y = wg, data = simflow, color=graph_color, ax=ax1, linewidth=graph_width, linestyle='dashdot')

def plot_modeled(graph_color, graph_width):
    # Datetime
    date = datetime.datetime(int(start_date[:4]),int(start_date[4:6]),int(start_date[6:8]),int(start_date[8:10]),int(start_date[10:12]))
    analysis_date = date.strftime('%Y%m%d%H%M')
    # Calibrated ver.(Pedersen et al.(2021)) is Bellinge 300
    ex_no_mod = '111'
    control_file_mod = open('./exp/'+experiment+ex_no_mod+'/control.yaml', 'r')
    p = yaml.safe_load(control_file_mod)
    control_file_mod.close()
    forecast_dir = root_dir+'/exp/'+experiment+ex_no_mod+'/forecast/'+analysis_date
    cycle_mod = p['da']['cycle']
    while date < end_date:
        output_file = forecast_dir+'/output00000000'
        opt = read_out_file(output_file)
        simulation_data = pd.DataFrame()
        sim_value = pd.Series()
        sim_value = opt.get_part('node',pp).depth
        simulation_data[wg] = sim_value
        if analysis_date == start_date: 
            simflow = simulation_data
        else:
            simflow = pd.concat([simflow, simulation_data])
        date += datetime.timedelta(minutes=cycle_mod)
        analysis_date = date.strftime('%Y%m%d%H%M')
        forecast_dir = root_dir+'/exp/'+experiment+ex_no_mod+'/forecast/'+analysis_date
    print(wg, pp, 'modeled')
    sns.lineplot(x = simflow.index, y = wg, data = simflow, color=graph_color, ax=ax1, linewidth=graph_width, linestyle='solid')

def plot_simgraph(imember, graph_color, graph_width):
    # Datetime
    date = datetime.datetime(int(start_date[:4]),int(start_date[4:6]),int(start_date[6:8]),int(start_date[8:10]),int(start_date[10:12]))
    analysis_date = date.strftime('%Y%m%d%H%M')
    forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast/'+analysis_date
    while date < end_date:
        output_file = forecast_dir+'/output'+member[imember]
        opt = read_out_file(output_file)
        simulation_data = pd.DataFrame()
        sim_value = pd.Series()
        sim_value = opt.get_part('node',pp).depth
        simulation_data[wg] = sim_value
        if analysis_date == start_date: 
            simflow = simulation_data
        else:
            simflow = pd.concat([simflow, simulation_data])
        date += datetime.timedelta(minutes=cycle)
        analysis_date = date.strftime('%Y%m%d%H%M')
        forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast/'+analysis_date
    print(wg, pp, imember)
    sns.lineplot(x = simflow.index, y = wg, data = simflow, color=graph_color, ax=ax1, linewidth=graph_width)

if len(sys.argv) > 4:
    analysis_date = sys.argv[1] # 201206290300
    end_date = sys.argv[2] # 201206291300
    experiment = sys.argv[3] # Mangwon or Bellinge
    ex_no = sys.argv[4] # 3
ex_no = '%3.3d'%int(ex_no)

# Control 
control_file = open('./exp/'+experiment+ex_no+'/control.yaml', 'r')
p = yaml.safe_load(control_file)
control_file.close()
root_dir = p['meta']['root_dir']
#start_date = p['meta']['start_date']
start_date = analysis_date
cycle = p['da']['cycle']
interval = p['da']['interval']
nmember = p['da']['nmember']

# Directories
bin_dir = root_dir+'/build'
bin_file = bin_dir+'/runswmm'
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
rainfall_dir = root_dir+'/exp/'+experiment+ex_no+'/rainfall'
parameter_dir = root_dir+'/exp/'+experiment+ex_no+'/parameter/'+analysis_date
analysis_dir = root_dir+'/exp/'+experiment+ex_no+'/analysis/'+analysis_date
forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast/'+analysis_date
obs_dir = root_dir+'/exp/'+experiment+ex_no+'/observation'
obs_file = root_dir+'/exp/'+experiment+ex_no+'/observation/obs'+analysis_date
graph_dir =  root_dir+'/exp/'+experiment+ex_no+'/hydrograph/'+start_date+'_'+end_date
if not os.path.isdir(graph_dir): os.system('mkdir -p '+graph_dir)

# Member
member = list(range(nmember))
for imember in range(nmember): member[imember] = '%8.8d'%imember

# json
# wg_dic = {'G71F04R':'G71F090',
#         'G71F05R':'G72K020',
#         'G71F06R':'G71F06R'}
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

# Rainfall
rain_gage = ['rg5425', 'rg5427']
rain_file = rainfall_dir + '/rg_bellinge_Jun2010_Aug2021.dat'
text_file = open(rain_file,'r')
line = text_file.readlines()
text_file.close()
rain_df = pd.DataFrame()
for rg in rain_gage:
    df = pd.DataFrame()
    rain_series = []
    rain_obs = []
    for i in range(0,len(line)): # len(line)
        tmp = re.split(' ',line[i].strip())
        # print(datetime.datetime.strptime(tmp[1]+tmp[2]+tmp[3], '%Y%m%d'))
        time = datetime.datetime.strptime(tmp[1]+'%2.2d'%int(tmp[2])+'%2.2d'%int(tmp[3])+'%2.2d'%int(tmp[4])+'%2.2d'%int(tmp[5]), '%Y%m%d%H%M')
        if tmp[0] == rg and end_date > time >= date: #and time < end_date:
            rain_series.append(time)
            rain_obs.append(float(tmp[6]))
    # df = pd.DataFrame({'time_series':rain_series, rg:rain_obs})
    series = pd.Series(rain_obs, index=rain_series)
    df[rg] = series
    rain_df = pd.concat([rain_df, df], axis=1)
    # if rain_df.empty:
    #     rain_df = rain_df.append(df)
    # else:
    #     rain_df = pd.concat([rain_df, df], axis=1)
rain_df = rain_df.resample('15min').sum() 
print(rain_df.index)

# Observation
while date < end_date:
    obs_file = root_dir+'/exp/'+experiment+ex_no+'/observation/obs'+analysis_date
    obs_data = pd.read_csv(obs_file, sep = '\s+', engine='python')
    obs_data['Datetime'] = pd.to_datetime(obs_data['Datetime'], format='%Y%m%d%H%M')
    obs_data = obs_data.set_index('Datetime')
    for wg,pp in wg_dic.items():
        obs_data[wg] = obs_data[wg] * 0.3048
    if analysis_date == start_date: 
        obsflow = obs_data#/0.3048
    else:
        obsflow = pd.concat([obsflow, obs_data])
    date += datetime.timedelta(minutes=cycle)
    analysis_date = date.strftime('%Y%m%d%H%M')
print(obsflow.index)
# Datetime
date = datetime.datetime(int(start_date[:4]),int(start_date[4:6]),int(start_date[6:8]),int(start_date[8:10]),int(start_date[10:12]))
# end_date = datetime.datetime(int(end_date[:4]),int(end_date[4:6]),int(end_date[6:8]),int(end_date[8:10]),int(end_date[10:12]))
analysis_date = date.strftime('%Y%m%d%H%M')

plt.rcParams.update({'figure.max_open_warning': 0})
# for i,wg in enumerate(wg_dic):
for wg,pp in wg_dic.items():
    sns.set(font_scale=1.5)
    sns.set_style("ticks", rc={"font.family": 'serif'})
    fig, ax1 = plt.subplots(figsize=(24,6))
    ax1.set(ylim=(0, obsflow[wg].max()+0.7))
    ax1.set_ylabel("W.L : {} [m]".format(wg), fontsize = '15')
    # for imember in range(nmember):
    #     plot_simgraph(imember, 'grey', 1)
    sns.lineplot(x = obsflow.index, y= wg, data = obsflow, color="green", linestyle='dashed', ax=ax1, linewidth=2)
    plot_calibrated('magenta', 2) # calibrated benchmark
    plot_modeled('blue', 2)
    plot_simgraph(0, 'red', 2) # ensemble mean
    plt.savefig(graph_dir+'/hydro_{}_cycle.png'.format(wg))
    plt.clf()

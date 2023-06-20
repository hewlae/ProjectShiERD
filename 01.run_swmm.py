#! /usr/bin/env python3
import sys, os, shutil, time, datetime, re, yaml, json
from numpy import *
from mpi4py import MPI
from swmm_api import read_inp_file
from swmm_api import read_hst_file
from swmm_api.input_file.section_labels import OPTIONS, FILES, SUBCATCHMENTS, SUBAREAS, INFILTRATION, CONDUITS, TIMESERIES

class Parameter:
   def __init__(self, name, used, minval, meanval, maxval, data):
      self.name = name
      self.used = used
      self.minval = minval
      self.meanval = meanval
      self.maxval = maxval
      self.data = data

# def read_rainfall(line):
#    tmp = re.split(',', line.strip())
#    # nchar = tmp.count('')
#    # for j in range(nchar): tmp.remove('')
#    # date = re.split(r'\.',tmp[0].strip()) 
#    line_date = datetime.datetime(int(tmp[0][:4]),int(tmp[0][5:7]),int(tmp[0][8:10]),int(tmp[0][11:13]),int(tmp[0][14:16]))
#    if date 
#    return (date, float(tmp[5]))

if len(sys.argv) > 3:
   analysis_date = sys.argv[1]
   experiment = sys.argv[2]
   ex_no = sys.argv[3]
ex_no = '%3.3d'%int(ex_no)

# Control
control_file = open('./exp/'+experiment+ex_no+'/control.yaml', 'r')
p = yaml.safe_load(control_file)
control_file.close()
root_dir = p['meta']['root_dir']
cold_start = p['meta']['cold_start']
start_date = p['meta']['start_date']
control = p['da']['para_control']
used_parameters = p['da']['parameters']
rain_control = p['da']['rain_control']
used_raingrids = p['da']['raingrids']
cycle = p['da']['cycle']
interval = p['da']['interval']
nmember = p['da']['nmember']
nparameter = p['model']['nparameter']
nsubcatchment = p['model']['nsubcatchment']
nconduit = p['model']['nconduit']
nstreet = p['model']['nstreet']
ninfiltration = nsubarea = nsubcatchment

# Directories
bin_dir = root_dir+'/build'
bin_file = bin_dir+'/runswmm'
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
rainfall_dir = root_dir+'/exp/'+experiment+ex_no+'/rainfall'
parameter_dir = root_dir+'/exp/'+experiment+ex_no+'/parameter/'+analysis_date
analysis_dir = root_dir+'/exp/'+experiment+ex_no+'/analysis/'+analysis_date
forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast/'+analysis_date
# Workdir
comm = MPI.COMM_WORLD
nprocessor = comm.Get_size()
myid = comm.Get_rank()
work_dir = root_dir+'/work/work'+'%2.2d'%myid
os.system('mkdir -p '+work_dir)
current_dir = os.getcwd()
os.chdir(work_dir)
if myid == 0 and not os.path.isdir(forecast_dir): 
   os.system('mkdir -p '+forecast_dir)
   # os.system('ln -s '+rainfall_dir+'/*.dat '+forecast_dir)
comm.Barrier()

# Member
member = list(range(nmember))
for imember in range(nmember): member[imember] = '%8.8d'%imember

# Datetime
date = datetime.datetime(int(analysis_date[:4]),int(analysis_date[4:6]),int(analysis_date[6:8]),int(analysis_date[8:10]),int(analysis_date[10:12]))
date += datetime.timedelta(minutes=cycle)
forecast_date = date.strftime('%Y%m%d%H%M')
date += datetime.timedelta(minutes=1)
end_date = date.strftime('%Y%m%d%H%M')

# Rainfall 
rg_file = const_dir+'/raingtog.json'
json_file = open(rg_file, 'r')
dic = json.load(json_file)
json_file.close()
rg_timeseries = {}
ncycle = int(cycle/interval) # cycle/interval = 10/2 = 5
cycledate = list(range(ncycle)) 
nrain = int(cycle)
raindate = list(range(nrain))
date = datetime.datetime(int(analysis_date[:4]),int(analysis_date[4:6]),int(analysis_date[6:8]),int(analysis_date[8:10]),int(analysis_date[10:12]))
for rg in dic.keys():
   # -1. Create rainfall timeseries (every minutes)
   for itime in range(nrain):
      raindate[itime] = [date + datetime.timedelta(minutes=itime), float(0.0)]
   # -2. Insert rainfall data if applicable
   raw_files = []
   for itime in range(ncycle):
      cycledate[itime] = date + datetime.timedelta(minutes=itime*interval) 
      raw_file = rg + '_' + cycledate[itime].strftime('%Y%m%d') + '.csv'
      if raw_file not in raw_files: raw_files.append(raw_file)
   for raw_file in raw_files:
      if not os.path.isfile(rainfall_dir+'/'+raw_file): continue
   text_file = open(rainfall_dir+'/'+raw_file,'r', encoding='utf-8')
   line = text_file.readlines()
   text_file.close()
   for i in range(1,len(line)):
      tmp = re.split(',', line[i].strip()) #.strip()
      linedate = datetime.datetime(int(tmp[0][:4]),int(tmp[0][5:7]),int(tmp[0][8:10]),int(tmp[0][11:13]),int(tmp[0][14:16]))
      if linedate > raindate[-1][0] or linedate < raindate[0][0] : continue
      for i, t in enumerate(raindate):
         if t[0] != linedate : continue
         raindate[i][1] = float(tmp[1])
   rg_timeseries[rg] = raindate[:]      

# Parameter
parameter = []
parameter_file = const_dir+'/parameters.txt'
text_file = open(parameter_file,'r')
line = text_file.readlines()
text_file.close()
for i in range(1,nparameter+rain_control+1):
   tmp = re.split('\t',line[i].strip())
   nchar = tmp.count('')
   for j in range(nchar): tmp.remove('')
   name = tmp[0].strip()
   if name in used_parameters: used = True
   elif name in ('SVRI'): used = True
   else: used = False
   minval = float(tmp[1])
   meanval = float(tmp[2])
   maxval = float(tmp[3])
   if name in ('Roughness',): data = zeros((nconduit))
   elif name in ('S_Roughness'): data = zeros((nstreet))
   elif name in ('SVRI'): data = zeros((len(used_raingrids)))
   else: data = zeros((nsubcatchment))
   if not used: data[:] = meanval
   #print(name, used, minval, meanval, maxval)
   parameter.append(Parameter(name, used, minval, meanval, maxval, data))
#sys.exit(0)

# Run
template_file = const_dir+'/input.txt'
for imember in range(nmember):
   if imember%nprocessor != myid: continue
   output_file = forecast_dir+'/output'+member[imember]
   #if os.path.isfile(output_file): continue
   # os.system('rm -rf *')
   # Read parameters
   parameter_file = parameter_dir+'/para'+member[imember]
   text_file = open(parameter_file, 'r')
   for i in range(nparameter+rain_control):
      if not parameter[i].used: continue
      line = text_file.readline()
      tmp = re.split('\t',line.strip())
      nchar = tmp.count('')
      for k in range(nchar): tmp.remove('')
      n = int(tmp[1])
      for j in range(n):
         line = text_file.readline()
         tmp = re.split('\t',line.strip())
         nchar = tmp.count('')
         for k in range(nchar): tmp.remove('')
         parameter[i].data[j] = float(tmp[1])
      #if imember == 1: print(parameter[i].name, parameter[i].data[0])
      if control == 1: parameter[i].data[1:] = parameter[i].data[0]
   text_file.close()
   
   # Write input
   input_file = forecast_dir+'/input'+member[imember]
   inp = read_inp_file(template_file)
   # Datetime
   inp[OPTIONS]['START_DATE'] = analysis_date[4:6]+'/'+analysis_date[6:8]+'/'+analysis_date[:4]
   inp[OPTIONS]['START_TIME'] = analysis_date[8:10]+':'+analysis_date[10:12]+':00'
   inp[OPTIONS]['REPORT_START_DATE'] = analysis_date[4:6]+'/'+analysis_date[6:8]+'/'+analysis_date[:4]
   inp[OPTIONS]['REPORT_START_TIME'] = analysis_date[8:10]+':'+analysis_date[10:12]+':00'
   #inp[OPTIONS]['END_DATE'] = forecast_date[4:6]+'/'+forecast_date[6:8]+'/'+forecast_date[:4]
   #inp[OPTIONS]['END_TIME'] = forecast_date[8:10]+':'+forecast_date[10:12]+':00'
   inp[OPTIONS]['END_DATE'] = end_date[4:6]+'/'+end_date[6:8]+'/'+end_date[:4] # enddate need be slightly greater than the true forecast date
   inp[OPTIONS]['END_TIME'] = end_date[8:10]+':'+end_date[10:12]+':00'
   inp[OPTIONS]['REPORT_STEP'] = '00:'+'%2.2d'%interval+':00'
   # Hotstart
   if cold_start == 1 and analysis_date == start_date: pass
   else: inp[FILES]['USE HOTSTART'] = '"'+analysis_dir+'/state'+member[imember]+'"'
   inp[FILES]['SAVE HOTSTART'] = '"'+forecast_dir+'/state'+member[imember]+'"'
   # Rainfall
   for rg, grids in dic.items():
      for grid in grids:
         if rain_control == 1 and grid in used_raingrids : 
            grid_timeseries = rg_timeseries[rg]
            for i in range(len(grid_timeseries)):
               grid_timeseries[i][1] *= parameter[-1].data[used_raingrids.index(grid)]               
            inp[TIMESERIES]['svri_{}'.format(grid)].data = grid_timeseries
         else : inp[TIMESERIES]['svri_{}'.format(grid)].data = rg_timeseries[rg]
   # Subcatchment
   subcatchments = inp[SUBCATCHMENTS].keys()
   for j,subcatchment in enumerate(subcatchments):
      # print(inp[SUBCATCHMENTS][subcatchment])
      # print(inp[SUBAREAS][subcatchment])
      # print(inp[INFILTRATION][subcatchment])
      for i in range(nparameter):
         if parameter[i].name == 'Roughness': continue
         elif parameter[i].name == 'Imperv': inp[SUBCATCHMENTS][subcatchment].imperviousness = parameter[i].data[j]
         elif parameter[i].name == 'Slope': inp[SUBCATCHMENTS][subcatchment].slope = parameter[i].data[j]
         elif parameter[i].name == 'N_Imperv': inp[SUBAREAS][subcatchment].n_imperv = parameter[i].data[j]
         elif parameter[i].name == 'N_Perv': inp[SUBAREAS][subcatchment].n_perv = parameter[i].data[j]
         elif parameter[i].name == 'S_Imperv': inp[SUBAREAS][subcatchment].storage_imperv = parameter[i].data[j]
         elif parameter[i].name == 'S_Perv': inp[SUBAREAS][subcatchment].storage_perv = parameter[i].data[j]
         elif parameter[i].name == 'PctZero': inp[SUBAREAS][subcatchment].pct_zero = parameter[i].data[j]
         elif parameter[i].name == 'Ksat': inp[INFILTRATION][subcatchment].Ksat = parameter[i].data[j]
         elif parameter[i].name == 'DryTime': inp[INFILTRATION][subcatchment].time_dry = parameter[i].data[j]
         elif parameter[i].name == 'MaxRate': inp[INFILTRATION][subcatchment].rate_max = parameter[i].data[j]
         elif parameter[i].name == 'MinRate': inp[INFILTRATION][subcatchment].rate_min = parameter[i].data[j]
         elif parameter[i].name == 'Decay': inp[INFILTRATION][subcatchment].decay = parameter[i].data[j]
   conduits = inp[CONDUITS].keys()
   for j,conduit in enumerate(conduits):
      #print(inp[CONDUITS][conduit])
      for i in range(nparameter):
         if j < nconduit and parameter[i].name == 'Roughness': inp[CONDUITS][conduit].roughness = parameter[i].data[j]
         elif j >= nconduit and parameter[i].name == 'S_Roughness': inp[CONDUITS][conduit].roughness = parameter[i].data[j-nconduit]
   inp.write_file(input_file)
   # to check
   # if not analysis_date == start_date:
   #    input_file = forecast_dir+'/input'+member[imember]
   #    state_file = analysis_dir+'/state'+member[imember]
   #    inp = read_inp_file(input_file)
   #    hsf = read_hst_file(state_file, inp)
   #    print(hsf.storages_frame.depth)
   # else: pass

   # Write output
  # print('running SWMM at {}'.format(input_file))
   os.system(bin_file+' '+input_file+' output.txt '+output_file+' > output.log 2>&1')
for i in range(nprocessor):
   if i%nprocessor != myid: continue
   os.chdir(current_dir)
   #os.system('rm -rf '+work_dir)
comm.Barrier()

#! /usr/bin/env python3
import sys, os, shutil, time, datetime, struct, re, yaml
from numpy import *
from scipy.stats import norm
from mpi4py import MPI
from swmm_api import read_inp_file
from swmm_api import read_hst_file

class Parameter:
   def __init__(self, name, used, minval, meanval, maxval, data):
      self.name = name
      self.used = used
      self.minval = minval
      self.meanval = meanval
      self.maxval = maxval
      self.data = data

class State:
   def __init__(self, name, kind, used, ana, fcst):
      self.name = name
      self.kind = kind
      self.used = used
      self.ana = ana
      self.fcst = fcst

def write_ana(filename, imember, state):
   bin_file = open(filename, 'wb')
   data = struct.pack('15s', b'SWMM5-HOTSTART4')
   bin_file.write(data)
   data = struct.pack('6i', nsubcatchment, nlanduse, nnode+noutfall+nstorage, nconduit+nstreet+npump+norifice+nweir+noutlet, npollutant, flowunit)
   bin_file.write(data)
   for j in range(nsubcatchment):
      for i in range(nstate):
         if state[i].kind != 'subcatchment': continue
         datum = struct.pack('1d', state[i].ana[j,imember])
         bin_file.write(datum)
   for j in range(nnode):
      for i in range(nstate):
         if state[i].kind != 'node': continue
         datum = struct.pack('1f', state[i].ana[j,imember])
         bin_file.write(datum)
   for j in range(noutfall):
      for i in range(nstate):
         if state[i].kind != 'outfall': continue
         datum = struct.pack('1f', state[i].ana[j,imember])
         bin_file.write(datum)
   for j in range(nstorage):
      for i in range(nstate):
         if state[i].kind != 'storage': continue
         datum = struct.pack('1f', state[i].ana[j,imember])
         bin_file.write(datum)
   for j in range(nconduit):
      for i in range(nstate):
         if state[i].kind != 'conduit': continue
         datum = struct.pack('1f', state[i].ana[j,imember])
         bin_file.write(datum)
   for j in range(nstreet):
      for i in range(nstate):
         if state[i].kind != 'street': continue
         datum = struct.pack('1f', state[i].ana[j,imember])
         bin_file.write(datum)
   for j in range(npump):
      for i in range(nstate):
         if state[i].kind != 'pump': continue
         datum = struct.pack('1f', state[i].ana[j,imember])
         bin_file.write(datum)
   for j in range(norifice):
      for i in range(nstate):
         if state[i].kind != 'orifice': continue
         datum = struct.pack('1f', state[i].ana[j,imember])
         bin_file.write(datum)
   for j in range(nweir):
      for i in range(nstate):
         if state[i].kind != 'weir': continue
         datum = struct.pack('1f', state[i].ana[j,imember])
         bin_file.write(datum)
   for j in range(noutlet):
      for i in range(nstate):
         if state[i].kind != 'outlet': continue
         datum = struct.pack('1f', state[i].ana[j,imember])
         bin_file.write(datum)
   bin_file.close()

def write_fcst(filename, imember, state):
   bin_file = open(filename, 'wb')
   data = struct.pack('15s', b'SWMM5-HOTSTART4')
   bin_file.write(data)
   data = struct.pack('6i', nsubcatchment, nlanduse, nnode+noutfall+nstorage, nconduit+nstreet+npump+norifice+nweir+noutlet, npollutant, flowunit)
   bin_file.write(data)
   for j in range(nsubcatchment):
      for i in range(nstate):
         if state[i].kind != 'subcatchment': continue
         datum = struct.pack('1d', state[i].fcst[j,imember])
         bin_file.write(datum)
   for j in range(nnode):
      for i in range(nstate):
         if state[i].kind != 'node': continue
         datum = struct.pack('1f', state[i].fcst[j,imember])
         bin_file.write(datum)
   for j in range(noutfall):
      for i in range(nstate):
         if state[i].kind != 'outfall': continue
         datum = struct.pack('1f', state[i].fcst[j,imember])
         bin_file.write(datum)
   for j in range(nstorage):
      for i in range(nstate):
         if state[i].kind != 'storage': continue
         datum = struct.pack('1f', state[i].fcst[j,imember])
         bin_file.write(datum)
   for j in range(nconduit):
      for i in range(nstate):
         if state[i].kind != 'conduit': continue
         datum = struct.pack('1f', state[i].fcst[j,imember])
         bin_file.write(datum)
   for j in range(nstreet):
      for i in range(nstate):
         if state[i].kind != 'street': continue
         datum = struct.pack('1f', state[i].fcst[j,imember])
         bin_file.write(datum)
   for j in range(npump):
      for i in range(nstate):
         if state[i].kind != 'pump': continue
         datum = struct.pack('1f', state[i].fcst[j,imember])
         bin_file.write(datum)
   for j in range(norifice):
      for i in range(nstate):
         if state[i].kind != 'orifice': continue
         datum = struct.pack('1f', state[i].fcst[j,imember])
         bin_file.write(datum)
   for j in range(nweir):
      for i in range(nstate):
         if state[i].kind != 'weir': continue
         datum = struct.pack('1f', state[i].fcst[j,imember])
         bin_file.write(datum)
   for j in range(noutlet):
      for i in range(nstate):
         if state[i].kind != 'outlet': continue
         datum = struct.pack('1f', state[i].fcst[j,imember])
         bin_file.write(datum)
   bin_file.close()

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
para_control = p['da']['para_control']
state_control = p['da']['state_control']
rain_control = p['da']['rain_control']
used_parameters = p['da']['parameters']
used_variables = p['da']['statevars']
used_raingrids = p['da']['raingrids']
cycle = p['da']['cycle']
interval = p['da']['interval']
nmember0 = p['da']['nmember']
herror = p['da']['herror']
nparameter = p['model']['nparameter']
nstate = p['model']['nstate']
nsubcatchment = p['model']['nsubcatchment']
nnode = p['model']['nnode']
noutfall = p['model']['noutfall']
nstorage = p['model']['nstorage']
nconduit = p['model']['nconduit']
nstreet = p['model']['nstreet']
npump = p['model']['npump']
norifice = p['model']['norifice']
nweir = p['model']['nweir']
noutlet = p['model']['noutlet']
npollutant = 0; nlanduse = 0; flowunit = 3
qcthreshold = 6.

# Directories
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
rainfall_dir = root_dir+'/exp/'+experiment+ex_no+'/rainfall'
obs_dir = root_dir+'/exp/'+experiment+ex_no+'/observation'
newanalysis_dir = root_dir+'/exp/'+experiment+ex_no+'/analysis'
newparameter_dir = root_dir+'/exp/'+experiment+ex_no+'/parameter'
parameter_dir = root_dir+'/exp/'+experiment+ex_no+'/parameter/'+analysis_date
analysis_dir = root_dir+'/exp/'+experiment+ex_no+'/analysis/'+analysis_date
reanalysis_dir = root_dir+'/exp/'+experiment+ex_no+'/reanalysis/'+analysis_date
forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast/'+analysis_date
obs_file = root_dir+'/exp/'+experiment+ex_no+'/observation/obs'+analysis_date
# Workdir
comm = MPI.COMM_WORLD
nprocessor = comm.Get_size()
myid = comm.Get_rank()
work_dir = root_dir+'/work/work'+'%2.2d'%myid
os.system('mkdir -p '+work_dir)
current_dir = os.getcwd()
os.chdir(work_dir)
if myid == 0 and not os.path.isdir(reanalysis_dir): 
   os.system('mkdir -p '+reanalysis_dir)

# Member
nmember = 0
for imember in range(1,nmember0):
   if (imember-1)%nprocessor != myid: continue
   nmember += 1
member = list(range(nmember))
kmember = 0
for imember in range(1,nmember0):
   if (imember-1)%nprocessor != myid: continue
   member[kmember] = '%8.8d'%imember
   kmember += 1
pass

# Datetime
date = datetime.datetime(int(analysis_date[:4]),int(analysis_date[4:6]),int(analysis_date[6:8]),int(analysis_date[8:10]),int(analysis_date[10:12]))
date += datetime.timedelta(minutes=cycle)
forecast_date = date.strftime('%Y%m%d%H%M')
newanalysis_dir += '/'+forecast_date
newparameter_dir += '/'+forecast_date
if myid == 0 and not os.path.isdir(newanalysis_dir): os.system('mkdir -p '+newanalysis_dir)
if myid == 0 and not os.path.isdir(newparameter_dir): os.system('mkdir -p '+newparameter_dir)
comm.Barrier()



# Part 1: Parameters
# Parameter characteristics
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
   if not used:
      data = zeros((1,nmember+1))
      data[:,:] = meanval
   elif para_control == 1:
      data = zeros((1,nmember+1))
   elif para_control == 2:
      if name in ('Roughness',): data = zeros((nconduit,nmember+1)) # last one for ensemble mean
      elif name in ('S_Roughness'): data = zeros((nstreet,nmember+1))
      elif name in ('SVRI'): data = zeros((len(used_raingrids),nmember+1))
      else: data = zeros((nsubcatchment,nmember+1))
   # print(name, used, minval, meanval, maxval)
   parameter.append(Parameter(name, used, minval, meanval, maxval, data))
pass

# Read parameters
for imember in range(nmember):
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
         parameter[i].data[j,imember] = float(tmp[0])
      # if imember == 1: print(parameter[i].name, parameter[i].data[0])
   text_file.close()
pass

# Parameter perturbations
n = max([nconduit,nstreet,nnode,nstorage,nsubcatchment])
src = zeros((n)); bufr = zeros((n))
for i in range(nparameter+rain_control):
   if not parameter[i].used: continue
   parameter[i].data[:,-1] = parameter[i].data[:,:-1].sum(axis=1)
   n,mtmp = parameter[i].data.shape
   src[:n] = parameter[i].data[:,-1]
   comm.Allreduce(src[:n], bufr[:n], MPI.SUM)
   parameter[i].data[:,-1] = bufr[:n]/(nmember0-1)
   for imember in range(nmember):
      parameter[i].data[:,imember] -= parameter[i].data[:,-1]
      parameter[i].data[:,imember] /= sqrt(nmember0-1)
pass



#Part 2: State
# State characteristics
state = []
state_file = const_dir+'/states.txt'
text_file = open(state_file,'r')
line = text_file.readlines()
text_file.close()
for i in range(1,nstate+1):
   tmp = re.split('\t',line[i].strip())
   nchar = tmp.count('')
   for j in range(nchar): tmp.remove('')
   name = tmp[0].strip()
   kind = tmp[1].strip()
   if kind in used_variables: used = True
   else: used = False
   if used:
      if state_control == 0:
         used = False
      elif state_control == 1:
         if name[:12] == 'Infiltration': used = False
         elif name[:7] == 'setting': used = False
         elif name[:9] == 'hydraulic': used = False        
      elif state_control == 2:
         used = True
   if kind == 'subcatchment':
      ana = zeros((nsubcatchment,nmember+1)) # last one for ensemble mean
      fcst = zeros((nsubcatchment,nmember+1))
   elif kind == 'node':
      ana = zeros((nnode,nmember+1))
      fcst = zeros((nnode,nmember+1))
   elif kind == 'outfall':
      ana = zeros((noutfall,nmember+1))
      fcst = zeros((noutfall,nmember+1))
   elif kind == 'storage':
      ana = zeros((nstorage,nmember+1))
      fcst = zeros((nstorage,nmember+1))
   elif kind == 'conduit':
      ana = zeros((nconduit,nmember+1))
      fcst = zeros((nconduit,nmember+1))
   elif kind == 'street':
      ana = zeros((nstreet,nmember+1))
      fcst = zeros((nstreet,nmember+1))
   elif kind == 'pump':
      ana = zeros((npump,nmember+1))
      fcst = zeros((npump,nmember+1))
   elif kind == 'orifice':
      ana = zeros((norifice,nmember+1))
      fcst = zeros((norifice,nmember+1))
   elif kind == 'weir':
      ana = zeros((nweir,nmember+1))
      fcst = zeros((nweir,nmember+1))
   elif kind == 'outlet':
      ana = zeros((noutlet,nmember+1))
      fcst = zeros((noutlet,nmember+1))      
   # print(name, kind, used)
   state.append(State(name, kind, used, ana, fcst))
pass

# Read old analysis and forecast states
for imember in range(nmember):
   input_file = forecast_dir+'/input'+member[imember]
   state_file = forecast_dir+'/state'+member[imember]
   inp = read_inp_file(input_file)
   hsf = read_hst_file(state_file, inp)
   # print(hsf.storages_frame.depth)
   # print(hsf.subcatchments_frame)
   # print(hsf.nodes_frame)
   # print(hsf.links_frame)
   for i in range(nstate):
      if state[i].kind == 'subcatchment':
         state[i].fcst[:,imember] = hsf.subcatchments_frame.loc[:,state[i].name].values
      elif state[i].kind == 'node':
         state[i].fcst[:,imember] = hsf.nodes_frame.loc[:nnode-1,state[i].name].values
      elif state[i].kind == 'outfall':
         state[i].fcst[:,imember] = hsf.nodes_frame.loc[nnode:,state[i].name].values
      elif state[i].kind == 'storage':
         state[i].fcst[:,imember] = hsf.storages_frame.loc[:,state[i].name].values
      # print('forecast states @ {} + 30 min'.format(analysis_date))
      # print(state[i].name, state[i].ana[0])
      elif state[i].kind == 'conduit':
         state[i].fcst[:,imember] = hsf.links_frame.loc[:nconduit-1,state[i].name].values
      elif state[i].kind == 'street':
         state[i].fcst[:,imember] = hsf.links_frame.loc[nconduit:nconduit+nstreet-1,state[i].name].values
      elif state[i].kind == 'pump':
         state[i].fcst[:,imember] = hsf.links_frame.loc[nconduit+nstreet:nconduit+nstreet+npump-1,state[i].name].values
      elif state[i].kind == 'orifice':
         state[i].fcst[:,imember] = hsf.links_frame.loc[nconduit+nstreet+npump:nconduit+nstreet+npump+norifice-1,state[i].name].values
      elif state[i].kind == 'weir':
         state[i].fcst[:,imember] = hsf.links_frame.loc[nconduit+nstreet+npump+norifice:nconduit+nstreet+npump+norifice+nweir-1,state[i].name].values
      elif state[i].kind == 'outlet':
         state[i].fcst[:,imember] = hsf.links_frame.loc[nconduit+nstreet+npump+norifice+nweir:nconduit+nstreet+npump+norifice+nweir+noutlet-1,state[i].name].values
   if cold_start == 1 and analysis_date == start_date: continue
   state_file = analysis_dir+'/state'+member[imember]
   hsf = read_hst_file(state_file, inp)
   for i in range(nstate):
      if state[i].kind == 'subcatchment':
         state[i].ana[:,imember] = hsf.subcatchments_frame.loc[:,state[i].name].values
      elif state[i].kind == 'node':
         state[i].ana[:,imember] = hsf.nodes_frame.loc[:nnode-1,state[i].name].values
      elif state[i].kind == 'outfall':
         state[i].ana[:,imember] = hsf.nodes_frame.loc[nnode:,state[i].name].values
      elif state[i].kind == 'storage':
         state[i].ana[:,imember] = hsf.storages_frame.loc[:,state[i].name].values
      # print('analysis states @ {} + 30 min'.format(analysis_date))
      # print(state[i].name, state[i].ana[0])
      elif state[i].kind == 'conduit':
         state[i].ana[:,imember] = hsf.links_frame.loc[:nconduit-1,state[i].name].values
      elif state[i].kind == 'street':
         state[i].ana[:,imember] = hsf.links_frame.loc[nconduit:nconduit+nstreet-1,state[i].name].values
      elif state[i].kind == 'pump':
         state[i].ana[:,imember] = hsf.links_frame.loc[nconduit+nstreet:nconduit+nstreet+npump-1,state[i].name].values
      elif state[i].kind == 'orifice':
         state[i].ana[:,imember] = hsf.links_frame.loc[nconduit+nstreet+npump:nconduit+nstreet+npump+norifice-1,state[i].name].values
      elif state[i].kind == 'weir':
         state[i].ana[:,imember] = hsf.links_frame.loc[nconduit+nstreet+npump+norifice:nconduit+nstreet+npump+norifice+nweir-1,state[i].name].values
      elif state[i].kind == 'outlet':
         state[i].ana[:,imember] = hsf.links_frame.loc[nconduit+nstreet+npump+norifice+nweir:nconduit+nstreet+npump+norifice+nweir+noutlet-1,state[i].name].values
pass

# State perturbations
n = max([nconduit,nstreet,nnode,nstorage,nsubcatchment])
src = zeros((n)); bufr = zeros((n))
for i in range(nstate):
   state[i].fcst[:,-1] = state[i].fcst[:,:-1].sum(axis=1)
   n,mtmp = state[i].fcst.shape
   src[:n] = state[i].fcst[:,-1]
   comm.Allreduce(src[:n], bufr[:n], MPI.SUM)
   state[i].fcst[:,-1] = bufr[:n]/(nmember0-1)
   for imember in range(nmember):
      state[i].fcst[:,imember] -= state[i].fcst[:,-1]
      if state[i].used: state[i].fcst[:,imember] /= sqrt(nmember0-1)
   if cold_start == 1 and analysis_date == start_date: continue
   state[i].ana[:,-1] = state[i].ana[:,:-1].sum(axis=1)
   n,mtmp = state[i].ana.shape
   src[:n] = state[i].ana[:,-1]
   comm.Allreduce(src[:n], bufr[:n], MPI.SUM)
   state[i].ana[:,-1] = bufr[:n]/(nmember0-1)
   for imember in range(nmember):
      state[i].ana[:,imember] -= state[i].ana[:,-1]
      if state[i].used: state[i].ana[:,imember] /= sqrt(nmember0-1)
pass

# Replace the ensemble mean by deterministic one for reanalysis
if myid == 0 and not (cold_start == 1 and analysis_date == start_date):
   input_file = forecast_dir+'/input'+'%8.8d'%0
   state_file = analysis_dir+'/state'+'%8.8d'%0
   inp = read_inp_file(input_file)
   hsf = read_hst_file(state_file, inp)
   # print('at 04.run_etkf at id = 0')   
   # print(hsf.storages_frame.depth)
   for i in range(nstate):
      if state[i].kind == 'subcatchment':
         state[i].ana[:,imember] = hsf.subcatchments_frame.loc[:,state[i].name].values
      elif state[i].kind == 'node':
         state[i].ana[:,imember] = hsf.nodes_frame.loc[:nnode-1,state[i].name].values
      elif state[i].kind == 'outfall':
         state[i].ana[:,imember] = hsf.nodes_frame.loc[nnode:,state[i].name].values
      elif state[i].kind == 'storage':
         state[i].ana[:,imember] = hsf.storages_frame.loc[:,state[i].name].values
         # print('forecast states @ {} + 30 min'.format(analysis_date))
         # print(state[i].name, state[i].ana)
      elif state[i].kind == 'conduit':
         state[i].ana[:,imember] = hsf.links_frame.loc[:nconduit-1,state[i].name].values
      elif state[i].kind == 'street':
         state[i].ana[:,imember] = hsf.links_frame.loc[nconduit:nconduit+nstreet-1,state[i].name].values
      elif state[i].kind == 'pump':
         state[i].ana[:,imember] = hsf.links_frame.loc[nconduit+nstreet:nconduit+nstreet+npump-1,state[i].name].values
      elif state[i].kind == 'orifice':
         state[i].ana[:,imember] = hsf.links_frame.loc[nconduit+nstreet+npump:nconduit+nstreet+npump+norifice-1,state[i].name].values
      elif state[i].kind == 'weir':
         state[i].ana[:,imember] = hsf.links_frame.loc[nconduit+nstreet+npump+norifice:nconduit+nstreet+npump+norifice+nweir-1,state[i].name].values
      elif state[i].kind == 'outlet':
         state[i].ana[:,imember] = hsf.links_frame.loc[nconduit+nstreet+npump+norifice+nweir:nconduit+nstreet+npump+norifice+nweir+noutlet-1,state[i].name].values
         # print(state[i].ana[:,imember])
for i in range(nstate):
   if cold_start == 1 and analysis_date == start_date: continue
   n,mtmp = state[i].ana.shape
   if myid == 0: bufr[:n] = state[i].ana[:,-1]
   comm.Bcast(bufr[:n], 0)
   if myid != 0: state[i].ana[:,-1] = bufr[:n]
pass



# Part 3: Observations
# Read observations
text_file = open(obs_file)
line = text_file.readlines()
text_file.close()
tmp = re.split('\t',line[0].strip())
nchar = tmp.count('')
for j in range(nchar): tmp.remove('')
nsite = len(tmp) - 1
ntime = int(cycle/interval)
y0 = zeros((ntime,nsite))
for itime in range(ntime):
   tmp = re.split('\t',line[itime+1].strip())
   nchar = tmp.count('')
   for j in range(nchar): tmp.remove('')
   for isite in range(nsite): y0[itime,isite] = float(tmp[isite+1])
pass

# Read Y (simulated obs)
Y = zeros((ntime,nsite,nmember+1))
for imember in range(nmember):
   obs_file = forecast_dir+'/obs'+member[imember]
   text_file = open(obs_file)
   line = text_file.readlines()
   text_file.close()
   for itime in range(ntime):
      tmp = re.split('\t',line[itime+1].strip())
      nchar = tmp.count('')
      for j in range(nchar): tmp.remove('')
      for isite in range(nsite): Y[itime,isite,imember] = float(tmp[isite+1])
# Ensemble mean
src = zeros((ntime,nsite)); bufr = zeros((ntime,nsite))
Y[:,:,-1] = Y[:,:,:-1].sum(axis=2)
src[:,:] = Y[:,:,-1]
comm.Allreduce(src, bufr, MPI.SUM)
Y[:,:,-1] = bufr/(nmember0-1)
# Quality control
src[:,:] = sum(Y[:,:,:-1]**2,axis=2)
comm.Allreduce(src, bufr, MPI.SUM)
bufr[:,:] = bufr/(nmember0-1)-Y[:,:,-1]**2 # forecast error variance
invalid = abs(y0-Y[:,:,-1]) > qcthreshold*sqrt(bufr+herror**2)
y0[invalid] = -9999.
# Perturbations
for imember in range(nmember):
   Y[:,:,imember] -= Y[:,:,-1]
   Y[:,:,imember] /= sqrt(nmember0-1)
pass



# Part 4: ETKF
# QC
valid = y0 > -9999.+1
y0 = y0[valid]
Y = Y[valid,:]
m = sum(valid)
sigmao = zeros((m))
YYT = zeros((m,m))
E = zeros((m))
U = zeros((m,m))
V = zeros((nmember,m))
G = zeros((m))
FG = zeros((m))
L = zeros((m))
FL = zeros((m))
d = zeros((m))
VTX = zeros((m))
dX = zeros((nmember))
#print(y0)
#print(Y[:,0])
# Observation errors
sigmao[:] = herror
for imember in range(nmember): Y[:,imember] /= sigmao
y0[:] = (y0-Y[:,-1])/sigmao

# Eigen-decomposition
YYT[:,:] = dot(Y[:,:-1],Y[:,:-1].T)
comm.Allreduce(YYT, U, MPI.SUM)
YYT[:,:] = U
E[:], U[:,:] = linalg.eigh(YYT)
E[:] = E[::-1]; U[:,:] = U[:,::-1]
invalid = E < 0.; E[invalid] = 0.
E[-1] = 0.
# Find rank
for k in range(m):
   if E[k] < 1.E-12: break
nrank = k
# Gamma and Lambda
G[:nrank] = sqrt(E[:nrank])
L[:nrank] = 1./sqrt(1.+G[:nrank]**2)
# Coordinate of the innovation vector in U
for k in range(nrank): d[k] = dot(y0,U[:,k])
# V
for k in range(nrank): V[:,k] = dot(Y[:,:-1].T,U[:,k])/G[k]

# Adaptive inflation
for k in range(nrank):
   FG[k] = G[k]*L[k]**2*abs(d[k])
   FG[k] = max([FG[k],G[k]*L[k]])
for k in range(1,nrank):
   if FG[k] > FG[k-1]: FG[k] = FG[k-1]
FL[:nrank] = FG[:nrank]/G[:nrank]
L[:nrank] = G[:nrank]*L[:nrank]**2*d[:nrank]
FL[:nrank] = 1. - FL[:nrank]
#if infl == 0: # pre-multiplicative
#   FL[:] = 1./sqrt(1./rho+G**2) # lambda
#elif infl == 1: # post-multiplicative
#   FL[:] = rho*L[:]
#print(G[:nrank])
#print(FL[:nrank])



# Part 5: Parameter analyses
# Parameter analyses
for i in range(nparameter+rain_control):
   if not parameter[i].used: continue
   n,mtmp = parameter[i].data.shape
   for j in range(n):
      for k in range(nrank): VTX[k] = sum(V[:,k]*parameter[i].data[j,:-1]) # X in the basis V
      comm.Allreduce(VTX[:nrank], E[:nrank], MPI.SUM)
      VTX[:nrank] = E[:nrank]
      parameter[i].data[j,-1] += sum(VTX[:nrank]*L[:nrank]) # analysis
      VTX[:nrank] *= FL[:nrank]
      dX[:] = 0.
      for k in range(nrank): dX[:] += VTX[k]*V[:,k]
      parameter[i].data[j,:-1] -= dX
      for k in range(nmember): parameter[i].data[j,k] = parameter[i].data[j,-1] + sqrt(nmember0-1)*parameter[i].data[j,k]
# Write out
for imember in range(nmember):
   parameter_file = newparameter_dir+'/para'+member[imember]
   text_file = open(parameter_file, 'w')
   for i in range(nparameter+rain_control):
      if not parameter[i].used: continue
      n, mtmp = parameter[i].data.shape
      text_file.write(parameter[i].name+'\t'+str(n)+'\n')
      # Convert back to physical quantities
      for j in range(n):
         physical = (parameter[i].maxval-parameter[i].minval)*norm.cdf(parameter[i].data[j,imember]) + parameter[i].minval
         text_file.write('%10.6f'%parameter[i].data[j,imember]+'\t'+'%10.6f'%physical+'\n')
   text_file.close()
if myid == 0:
   parameter_file = newparameter_dir+'/para'+'%8.8d'%0
   text_file = open(parameter_file, 'w')
   for i in range(nparameter+rain_control):
      if not parameter[i].used: continue
      n, mtmp = parameter[i].data.shape
      text_file.write(parameter[i].name+'\t'+str(n)+'\n')
      # Convert back to physical quantities
      for j in range(n):
         physical = (parameter[i].maxval-parameter[i].minval)*norm.cdf(parameter[i].data[j,-1]) + parameter[i].minval
         text_file.write('%10.6f'%parameter[i].data[j,-1]+'\t'+'%10.6f'%physical+'\n')
   text_file.close()
   reanalysis_file = reanalysis_dir+'/para'+'%8.8d'%0
   shutil.copy(parameter_file, reanalysis_file)
pass



# Part 6: State analyses
for i in range(nstate):
   if not state[i].used: continue
   # State perturbation analyses
   n,mtmp = state[i].fcst.shape
   for j in range(n):
      for k in range(nrank): VTX[k] = sum(V[:,k]*state[i].fcst[j,:-1]) # X in the basis V
      comm.Allreduce(VTX[:nrank], E[:nrank], MPI.SUM)
      VTX[:nrank] = E[:nrank]
      #state[i].fcst[j,-1] += sum(VTX[:nrank]*L[:nrank]) # we do not use analysis at the end of window
      VTX[:nrank] *= FL[:nrank]
      dX[:] = 0.
      for k in range(nrank): dX[:] += VTX[k]*V[:,k]
      state[i].fcst[j,:-1] -= dX
      state[i].fcst[j,:-1] *= sqrt(nmember0-1)
   if cold_start == 1 and analysis_date == start_date: continue
   # State reanalysis
   n,mtmp = state[i].ana.shape
   for j in range(n):
      for k in range(nrank): VTX[k] = sum(V[:,k]*state[i].ana[j,:-1]) # X in the basis V
      comm.Allreduce(VTX[:nrank], E[:nrank], MPI.SUM)
      VTX[:nrank] = E[:nrank]
      state[i].ana[j,-1] += sum(VTX[:nrank]*L[:nrank])
# Writeout
for imember in range(nmember):
   pert_file = newanalysis_dir+'/pert'+member[imember]
   write_fcst(pert_file, imember, state)
if myid == 0 and not (cold_start == 1 and analysis_date == start_date):
   state_file = reanalysis_dir+'/state'+'%8.8d'%0
   write_ana(state_file, -1, state)
comm.Barrier()
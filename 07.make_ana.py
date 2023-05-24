#! /usr/bin/env python3
import sys, os, shutil, datetime, struct, re, yaml
from numpy import *
from mpi4py import MPI
from swmm_api import read_inp_file
from swmm_api import read_hst_file

class State:
   def __init__(self, name, kind, used, ana):
      self.name = name
      self.kind = kind
      self.used = used
      self.ana = ana

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
cycle = p['da']['cycle']
nmember0 = p['da']['nmember']
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

# Directories
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast'
analysis_dir = root_dir+'/exp/'+experiment+ex_no+'/analysis/'+analysis_date
# Workdir
comm = MPI.COMM_WORLD
nprocessor = comm.Get_size()
myid = comm.Get_rank()
work_dir = root_dir+'/work/work'+'%2.2d'%myid
os.system('mkdir -p '+work_dir)
current_dir = os.getcwd()
os.chdir(work_dir)

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
date -= datetime.timedelta(minutes=cycle)
firstguess_date = date.strftime('%Y%m%d%H%M')
forecast_dir += '/'+firstguess_date

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
   #print(name, kind, used)
   state.append(State(name, kind, used, ana))
pass

# Read analysis and perturbations
for imember in range(nmember):
   input_file = forecast_dir+'/input'+member[imember]
   state_file = analysis_dir+'/pert'+member[imember]
   inp = read_inp_file(input_file)
   hsf = read_hst_file(state_file, inp)
   #print(hsf.storages_frame.depth)
   #print(hsf.subcatchments_frame)
   #print(hsf.nodes_frame)
   #print(hsf.links_frame)
   for i in range(nstate):
      if state[i].kind == 'subcatchment':
         state[i].ana[:,imember] = hsf.subcatchments_frame.loc[:,state[i].name].values
      elif state[i].kind == 'node':
         state[i].ana[:,imember] = hsf.nodes_frame.loc[:nnode-1,state[i].name].values
      elif state[i].kind == 'outfall':
         state[i].ana[:,imember] = hsf.nodes_frame.loc[nnode:,state[i].name].values
      elif state[i].kind == 'storage':
         state[i].ana[:,imember] = hsf.storages_frame.loc[:,state[i].name].values
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

# Read the deterministic one
n = max(nconduit,nstreet,nnode,nstorage,nsubcatchment)
bufr = zeros((n))
if myid == 0:
   input_file = forecast_dir+'/input'+'%8.8d'%0
   state_file = analysis_dir+'/state'+'%8.8d'%0
   inp = read_inp_file(input_file)
   hsf = read_hst_file(state_file, inp)
   # print('reanalysis results @ {}'.format(analysis_date))
   # print(hsf.storages_frame.depth)
   for i in range(nstate):
      if state[i].kind == 'subcatchment':
         state[i].ana[:,-1] = hsf.subcatchments_frame.loc[:,state[i].name].values
      elif state[i].kind == 'node':
         state[i].ana[:,-1] = hsf.nodes_frame.loc[:nnode-1,state[i].name].values
      elif state[i].kind == 'outfall':
         state[i].ana[:,-1] = hsf.nodes_frame.loc[nnode:,state[i].name].values
      elif state[i].kind == 'storage':
         state[i].ana[:,-1] = hsf.storages_frame.loc[:,state[i].name].values
      elif state[i].kind == 'conduit':
         state[i].ana[:,-1] = hsf.links_frame.loc[:nconduit-1,state[i].name].values
      elif state[i].kind == 'street':
         state[i].ana[:,-1] = hsf.links_frame.loc[nconduit:nconduit+nstreet-1,state[i].name].values
      elif state[i].kind == 'pump':
         state[i].ana[:,-1] = hsf.links_frame.loc[nconduit+nstreet:nconduit+nstreet+npump-1,state[i].name].values
      elif state[i].kind == 'orifice':
         state[i].ana[:,-1] = hsf.links_frame.loc[nconduit+nstreet+npump:nconduit+nstreet+npump+norifice-1,state[i].name].values
      elif state[i].kind == 'weir':
         state[i].ana[:,-1] = hsf.links_frame.loc[nconduit+nstreet+npump+norifice:nconduit+nstreet+npump+norifice+nweir-1,state[i].name].values
      elif state[i].kind == 'outlet':
         state[i].ana[:,-1] = hsf.links_frame.loc[nconduit+nstreet+npump+norifice+nweir:nconduit+nstreet+npump+norifice+nweir+noutlet-1,state[i].name].values
for i in range(nstate):
   n,mtmp = state[i].ana.shape
   if myid == 0: bufr[:n] = state[i].ana[:,-1]
   comm.Bcast(bufr[:n], 0)
   if myid != 0: state[i].ana[:,-1] = bufr[:n]
pass

# Add perturbations and writeout
for imember in range(nmember):
   for i in range(nstate): 
      # if state[i].name == 'setting' and state[i].kind == 'outlet': print(state[i].name, state[i].kind, state[i].ana)
      state[i].ana[:,imember] += state[i].ana[:,-1]
      # if state[i].name == 'setting' and state[i].kind == 'outlet': 
      #    print('after update ...')
      #    print(state[i].name, state[i].kind, state[i].ana)
   state_file = analysis_dir+'/state'+member[imember]
   write_ana(state_file, imember, state)
   # to check
   # input_file = forecast_dir+'/input'+'%8.8d'%0
   # state_file = analysis_dir+'/state'+member[imember]
   # inp = read_inp_file(input_file)
   # hsf = read_hst_file(state_file, inp)
   # print(hsf.nodes_frame[-20:])
   # print(hsf.links_frame[-30:])
   # print(hsf.storages_frame.depth)
comm.Barrier()

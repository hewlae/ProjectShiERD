#! /usr/bin/env python3
import sys, os, shutil, datetime, re, yaml
from numpy import *
from mpi4py import MPI
from swmm_api import read_out_file
from swmm_api.output_file import OBJECTS, VARIABLES

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
interval = p['da']['interval']
nmember = p['da']['nmember']

# Directories
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast/'+analysis_date
comm = MPI.COMM_WORLD
nprocessor = comm.Get_size()
myid = comm.Get_rank()

# Member
member = list(range(nmember))
for imember in range(nmember): member[imember] = '%8.8d'%imember

# Datetime
ntime = int(cycle/interval)
obsdate = list(range(ntime))
date = datetime.datetime(int(analysis_date[:4]),int(analysis_date[4:6]),int(analysis_date[6:8]),int(analysis_date[8:10]),int(analysis_date[10:12]))
for itime in range(ntime): obsdate[itime] = date + datetime.timedelta(minutes=(itime+1)*interval)

# Obssite
obssite = []
obssite_file = const_dir+'/obssites.txt'
text_file = open(obssite_file,'r')
line = text_file.readlines()
text_file.close()
for i in range(1,len(line)):
   tmp = re.split('\t',line[i].strip())
   nchar = tmp.count('')
   for j in range(nchar): tmp.remove('')
   if len(tmp) == 0: continue
   obssite.append(tmp[1].strip())
nsite = len(obssite)
if myid == 0: print(obssite)
#sys.exit(0)

# Run
obs = zeros((ntime,nsite))
for imember in range(nmember):
   if imember%nprocessor != myid: continue
   obs[:,:] = -9999.
   # Read
   output_file = forecast_dir+'/output'+member[imember]
   out = read_out_file(output_file)
   for isite in range(nsite):
      #print(out.get_part(OBJECTS.NODE, obssite[isite], VARIABLES.NODE.DEPTH).values[1:])
      obs[:,isite] = 3.28 * out.get_part(OBJECTS.NODE, obssite[isite], VARIABLES.NODE.DEPTH).values[0:]
   # Write
   obs_file = forecast_dir+'/obs'+member[imember]
   text_file = open(obs_file, 'w')
   text = 'Datetime'
   for isite in range(nsite): text += '\t'+obssite[isite]
   text_file.write(text+'\n')
   for itime in range(ntime):
      text = obsdate[itime].strftime('%Y%m%d%H%M')
      for isite in range(nsite): text += '\t'+'%10.4f'%obs[itime,isite]
      text_file.write(text+'\n')
   text_file.close()
comm.Barrier()

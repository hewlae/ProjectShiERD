#! /usr/bin/env python3
import sys, os, shutil, datetime, re, yaml
from numpy import *
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

# Directories
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
reanalysis_dir = root_dir+'/exp/'+experiment+ex_no+'/reanalysis/'+analysis_date

# Member
nmember = 1
member = ['%8.8d'%0]

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
#sys.exit(0)

# Run
obs = zeros((ntime,nsite))
for imember in range(nmember):
   obs[:,:] = -9999.
   # Read
   output_file = reanalysis_dir+'/output'+member[imember]
   out = read_out_file(output_file)
   for isite in range(nsite): obs[:,isite] = out.get_part(OBJECTS.NODE, obssite[isite], VARIABLES.NODE.DEPTH).values[0:]
   # Write
   obs_file = reanalysis_dir+'/obs'+member[imember]
   text_file = open(obs_file, 'w')
   text = 'Datetime'
   for isite in range(nsite): text += '\t'+obssite[isite]
   text_file.write(text+'\n')
   for itime in range(ntime):
      text = obsdate[itime].strftime('%Y%m%d%H%M')
      for isite in range(nsite): text += '\t'+'%10.4f'%obs[itime,isite]
      text_file.write(text+'\n')
   text_file.close()
pass

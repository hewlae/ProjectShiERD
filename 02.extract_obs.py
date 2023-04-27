#! /usr/bin/env python3
import sys, os, shutil, datetime, re, yaml
from numpy import *

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
bin_dir = root_dir+'/build'
bin_file = bin_dir+'/runswmm'
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
obs_dir = root_dir+'/exp/'+experiment+ex_no+'/observation'
obs_file = root_dir+'/exp/'+experiment+ex_no+'/observation/obs'+analysis_date

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
   obssite.append(tmp[0].strip())
nsite = len(obssite)
print(obssite)
#sys.exit(0)

# Read
obs = zeros((ntime,nsite))
obs[:,:] = -9999.
for isite in range(nsite):
   raw_file = obs_dir+'/'+obssite[isite]+'.csv'
   text_file = open(raw_file,'r')
   text_file.readline()
   while True:
      line = text_file.readline()
      tmp = re.split(',',line.strip())
      nchar = tmp.count('')
      for j in range(nchar): tmp.remove('')
      if len(tmp) == 0: break
      date = datetime.datetime(int(tmp[1][:4]),int(tmp[1][5:7]),int(tmp[1][8:10]),int(tmp[1][11:13]),int(tmp[1][14:16]))
      if date < obsdate[0]: continue
      if date > obsdate[-1]: break
      delta = date - obsdate[0]
      delta = delta.days*24*60 + int(delta.seconds/60)
      itime = int(delta/interval)
      if tmp[-2] == 'False': tmp[-2] = None
      else: obs[itime,isite] = 3.28 * float(tmp[-2])
#      print(obssite[isite], date, obs[itime,isite])
   text_file.close()
pass

# Write
text_file = open(obs_file, 'w')
text = 'Datetime'
for isite in range(nsite): text += '\t'+obssite[isite]
text_file.write(text+'\n')
for itime in range(ntime):
   text = obsdate[itime].strftime('%Y%m%d%H%M')
   for isite in range(nsite): text += '\t'+'%10.4f'%obs[itime,isite]
   text_file.write(text+'\n')
text_file.close()

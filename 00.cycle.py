#! /usr/bin/env python3
import sys, os, shutil, time, datetime, re, yaml

if len(sys.argv) > 2:
   experiment = sys.argv[1]
   ex_no = sys.argv[2]
ex_no = '%3.3d'%int(ex_no)

# Control
control_file = open('./exp/'+experiment+ex_no+'/control.yaml', 'r')
p = yaml.safe_load(control_file)
control_file.close()
root_dir = p['meta']['root_dir']
start_date = p['meta']['start_date']
end_date = p['meta']['end_date']
npe = p['meta']['npe']
cycle = p['da']['cycle']
nmember = p['da']['nmember']

# Directories
bin_dir = root_dir
const_dir = root_dir+'/exp/'+experiment+ex_no+'/const'
parameter_dir = root_dir+'/exp/'+experiment+ex_no+'/parameter'
analysis_dir = root_dir+'/exp/'+experiment+ex_no+'/analysis'
forecast_dir = root_dir+'/exp/'+experiment+ex_no+'/forecast'
reanalysis_dir = root_dir+'/exp/'+experiment+ex_no+'/reanalysis'

# Member
member = list(range(nmember))
for imember in range(nmember): member[imember] = '%8.8d'%imember

# Datetime
date = datetime.datetime(int(start_date[:4]),int(start_date[4:6]),int(start_date[6:8]),int(start_date[8:10]),int(start_date[10:12]))
end_date = datetime.datetime(int(end_date[:4]),int(end_date[4:6]),int(end_date[6:8]),int(end_date[8:10]),int(end_date[10:12]))
start = time.time()
while date < end_date:
   analysis_date = date.strftime('%Y%m%d%H%M')
   forecast_date = date + datetime.timedelta(minutes=cycle)
   forecast_date = forecast_date.strftime('%Y%m%d%H%M')
   ntry = 5
   existed = True
   for k in range(ntry):
      os.system('mpirun -n '+str(npe)+' '+bin_dir+'/01.run_swmm.py '+analysis_date+' '+experiment+' '+ex_no)
      existed = True
      for imember in range(nmember):
         output_file = forecast_dir+'/'+analysis_date+'/output'+member[imember]
         if not os.path.isfile(output_file): existed = False; break
      if existed: break
      else: print('Try rerun Ensemble SWMM at '+analysis_date)
   if not existed:
      print('Ensemble SWMM run has a severe problem at '+analysis_date)
      sys.exit(0)
   pass

   os.system(bin_dir+'/02.extract_obs.py '+analysis_date+' '+experiment+' '+ex_no)
   os.system('mpirun -n '+str(npe)+' '+bin_dir+'/03.apply_H.py '+analysis_date+' '+experiment+' '+ex_no)
   existed = True
   for imember in range(1,nmember):
      output_file = forecast_dir+'/'+analysis_date+'/obs'+member[imember]
      if not os.path.isfile(output_file): existed = False; break
   if not existed:
      print('H run has a severe problem at '+analysis_date)
      sys.exit(0)
   pass

   os.system('mpirun -n '+str(npe)+' '+bin_dir+'/04.run_etkf.py '+analysis_date+' '+experiment+' '+ex_no)
   existed = True
   for imember in range(1,nmember):
      output_file = analysis_dir+'/'+forecast_date+'/pert'+member[imember]
      if not os.path.isfile(output_file): existed = False; break
      output_file = parameter_dir+'/'+forecast_date+'/para'+member[imember]
      if not os.path.isfile(output_file): existed = False; break
   if not existed:
      print('EnKF run has a severe problem at '+analysis_date)
      sys.exit(0)
   output_file = reanalysis_dir+'/'+analysis_date+'/para'+member[0]
   if not os.path.isfile(output_file):
      print('EnKF run has a severe problem at '+analysis_date)
      sys.exit(0)
   pass

   os.system(bin_dir+'/05.rerun_swmm0.py '+analysis_date+' '+experiment+' '+ex_no)
   output_file = analysis_dir+'/'+forecast_date+'/state'+member[0]
   if not os.path.isfile(output_file):
      print('Deterministic SWMM run has a severe problem at '+analysis_date)
      sys.exit(0)
   os.system(bin_dir+'/06.apply_H0.py '+analysis_date+' '+experiment+' '+ex_no)

   os.system('mpirun -n '+str(npe)+' '+bin_dir+'/07.make_ana.py '+forecast_date+' '+experiment+' '+ex_no)
   existed = True
   for imember in range(1,nmember):
      output_file = analysis_dir+'/'+forecast_date+'/state'+member[imember]
      if not os.path.isfile(output_file): existed = False; break
   if not existed:
      print('Making analyses has a severe problem at '+forecast_date)
      sys.exit(0)
   pass
   end = time.time()
   result = str(datetime.timedelta(seconds=(end-start))).split(".")
   print('running a ETKF cycle @ {} takes time of '.format(analysis_date) + result[0])
   start = end
   date += datetime.timedelta(minutes=cycle)
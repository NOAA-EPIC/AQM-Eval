#!/bin/bash --login
#SBATCH --clusters=c6
#SBATCH --partition=batch
#SBATCH --account=bil-fire3
#SBATCH --job-name=monet
#SBATCH --nodes=2
#SBATCH --tasks-per-node=100
#SBATCH --time=9:30:00
#SBATCH --output=./run_monet.log
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=Patrick.C.Campbell@noaa.gov

# 1.  Chem species: O3, PM2.5, CO, NO2, SO2, NH3. Run time = 6:00:00 is recommended.
# 2.  Met variables: tmp2m, td2m, ws10m, wd10m. Run time = 4:20:00 is recommended.
# For Both Chem and Met and AQS PM, Run time = 11:00:00 is recommended.
# 3.  AQS PM variables, PM25 SO4, NO3, NH4, EC, and OC. Run time = 12:00:00 is recommended for 10 days

#cd into working directory
cd /gpfs/f6/bil-fire3/proj-shared/Patrick.C.Campbell/MELODIES-MONET/working/MM_scripts/aqmv8p1_metemis_Aug2023

#Load M-M modules on C6
module use /gpfs/f6/bil-fire3/world-shared/modulefiles
module load melodies-monet


#1.  CHEM Evaluations -------------------------------->

#Copy modified namelist for chem evaluation case study
#cp namelist.chem.yaml namelist.yaml

#To generate all yaml configs for tasks
#python monet_yaml_generator.py

#Ensure monet generated bash scripts are executable needed for chem below
#chmod +x ./link_dyns_to_Alldays_base.sh
#chmod +x ./link_dyns_to_Alldays_eval.sh
#chmod +x ./pg8_csi_rename_hit_rate
#chmod +x ./pg8_csi_rename_false_alarm_rate
#chmod +x ./pg8_csi_rename_critical_success_index

#For chem evaluation linking of chem variables to airnow files
#./link_dyns_to_Alldays_base.sh && ./link_dyns_to_Alldays_eval.sh

#Available M-M selected tasks for chem
#python monet_AirNow_driver.py save_paired
#python monet_AirNow_driver.py timeseries
#python monet_AirNow_driver.py taylor
#python monet_AirNow_driver.py spatial_bias
#python monet_AirNow_driver.py spatial_overlay
#python monet_AirNow_driver.py boxplot
#python monet_AirNow_driver.py multi_boxplot
#python monet_AirNow_driver.py scorecard_rmse
#python monet_AirNow_driver.py scorecard_ioa
#python monet_AirNow_driver.py scorecard_nmb
#python monet_AirNow_driver.py scorecard_nme
#python monet_AirNow_driver.py csi
#This is option dependent on what "csi_score" is chosen in namelist.chem.yaml
#./pg8_csi_rename_hit_rate
#./pg8_csi_rename_false_alarm_rate
#./pg8_csi_rename_critical_success_index
#python monet_AirNow_driver.py stats



#2.  MET Evaluations ------------------------------->

#Copy modified namelist for met evaluation case study
#cp namelist.ish.yaml namelist.yaml

#To generate all yaml configs for tasks
#python monet_yaml_generator.py

#Ensure monet generated bash scripts are executable needed for met below
#chmod +x ish_conv_base.sh
#chmod +x ish_conv_eval.sh

#For met evaluation conversion of met variables to ish
#./ish_conv_base.sh && ./ish_conv_eval.sh

#Available M-M selected tasks for met
#python monet_AirNow_driver.py save_paired
#python monet_AirNow_driver.py timeseries
#python monet_AirNow_driver.py taylor
#python monet_AirNow_driver.py spatial_bias
#python monet_AirNow_driver.py spatial_overlay
#python monet_AirNow_driver.py boxplot
#python monet_AirNow_driver.py stats


#3 AQS PM Composition Evaluations ---------------------------------->

#Copy modified namelist for met evaluation case study
cp namelist.aqs.pm.yaml namelist.yaml

#To generate all yaml configs for tasks
python monet_yaml_generator.py

#Ensure monet generated bash scripts are executable needed for AQS below
chmod +x aqs_pm_conv_base.sh
chmod +x aqs_pm_conv_eval.sh
chmod +x ./pg8_csi_rename_hit_rate
chmod +x ./pg8_csi_rename_false_alarm_rate
chmod +x ./pg8_csi_rename_critical_success_index

#For AQS evaluation conversion of chem variables to AQS
./aqs_pm_conv_base.sh && ./aqs_pm_conv_eval.sh

#Available M-M selected tasks for chem
python monet_AirNow_driver.py save_paired
python monet_AirNow_driver.py timeseries
python monet_AirNow_driver.py taylor
python monet_AirNow_driver.py spatial_bias
python monet_AirNow_driver.py spatial_overlay
python monet_AirNow_driver.py boxplot
python monet_AirNow_driver.py multi_boxplot
python monet_AirNow_driver.py scorecard_rmse
python monet_AirNow_driver.py scorecard_ioa
python monet_AirNow_driver.py scorecard_nmb
python monet_AirNow_driver.py scorecard_nme
python monet_AirNow_driver.py csi
#This is option dependent on what "csi_score" is chosen in namelist.aqs.pm.yaml (same as above)
./pg8_csi_rename_hit_rate
#./pg8_csi_rename_false_alarm_rate
#./pg8_csi_rename_critical_success_index
python monet_AirNow_driver.py stats

#4 AQS VOC Species Evaluations ---------------------------------->
#TODO

#!/bin/bash --login
#SBATCH --clusters=c6
#SBATCH --partition=batch
#SBATCH --account=bil-fire8
#SBATCH --job-name=monet
#SBATCH --nodes=2
#SBATCH --tasks-per-node=100
#SBATCH --time=11:00:00
#SBATCH --output=./out.run_monet.sh
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=benjamin.koziol@noaa.gov

# 1.  Chem species: O3, PM2.5, CO, NO2. Run time = 6:00:00 is recommended.
# 2.  Met variables: tmp2m, td2m, ws10m, wd10m. Run time = 4:20:00 is recommended.
# For Both Chem and Met, Run time = 11:00:00 is recommended.

set -xue

#cd into working directory
_WD=/gpfs/f6/bil-fire8/scratch/Benjamin.Koziol/tmp/MM-run
cd ${_WD} || exit

#Load M-M modules on C6
module use /gpfs/f6/bil-fire3/world-shared/modulefiles #tdk: need conda environment for M-M
module load melodies-monet


#1.  CHEM Evaluations -------------------------------->

##Copy modified namelist for chem evaluation case study
#cp namelist.chem.yaml namelist.yaml
#
##To generate all yaml configs for tasks
#python monet_yaml_generator.py
#
#Ensure monet generated bash scripts are executable needed for chem below
#chmod +x ./link_dyns_to_Alldays_base.sh #tdk: don't need base for obs only
#chmod +x ./link_dyns_to_Alldays_eval.sh
#
##This is option dependent on what "csi_score" is chosen in namelist.chem.yaml
##chmod +x ./pg8_csi_rename_hit_rate
##chmod +x ./pg8_csi_rename_false_alarm_rate
#chmod +x ./pg8_csi_rename_critical_success_index
#
##For chem evaluation linking of chem variables to airnow files
##./link_dyns_to_Alldays_base.sh #tdk: don't need base for obs only
#./link_dyns_to_Alldays_eval.sh
#
##Available M-M selected tasks for chem
#python monet_AirNow_driver.py save_paired
#python monet_AirNow_driver.py timeseries
#python monet_AirNow_driver.py taylor
#python monet_AirNow_driver.py spatial_bias
#python monet_AirNow_driver.py spatial_overlay
#python monet_AirNow_driver.py boxplot
#python monet_AirNow_driver.py multi_boxplot
##python monet_AirNow_driver.py scorecard_rmse #tdk: scorecard are for comparing two model output
##python monet_AirNow_driver.py scorecard_ioa
##python monet_AirNow_driver.py scorecard_nmb
##python monet_AirNow_driver.py scorecard_nme
#python monet_AirNow_driver.py csi
#
##This is option dependent on what "csi_score" is chosen in namelist.chem.yaml (same as above)
##./pg8_csi_rename_hit_rate
##./pg8_csi_rename_false_alarm_rate
##./pg8_csi_rename_critical_success_index
#python monet_AirNow_driver.py stats



#2.  MET Evaluations ------------------------------->

#Copy modified namelist for met evaluation case study
cp namelist.ish.yaml namelist.yaml

#To generate all yaml configs for tasks
python monet_yaml_generator.py

#Ensure monet generated bash scripts are executable needed for met below
#chmod +x ish_conv_base.sh
chmod +x ish_conv_eval.sh

#For met evaluation conversion of met variables to ish files
#./ish_conv_base.sh &&
./ish_conv_eval.sh

#Available M-M selected tasks for met
python monet_AirNow_driver.py save_paired
python monet_AirNow_driver.py timeseries
python monet_AirNow_driver.py taylor
python monet_AirNow_driver.py spatial_bias
python monet_AirNow_driver.py spatial_overlay
python monet_AirNow_driver.py boxplot
python monet_AirNow_driver.py stats

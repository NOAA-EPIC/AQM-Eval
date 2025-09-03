# This code uses MELODIES-MONET to read in a .yaml file
# and produces plots. For an interactive script see
# jupyter notebooks in main directory.
# This is needed to tell matplotlib to use a non-interactive backend and avoid display errors.

import matplotlib

matplotlib.use("Agg")
from melodies_monet import driver
import os, sys
import dask
import cartopy

# cartopy.config["data_dir"] = os.environ["CARTOPY_DATA_DIR"]
cartopy.config["data_dir"] = "/gpfs/f6/bil-fire8/world-shared/UFS_SRW_data/develop/NaturalEarth"
print(f"{cartopy.config['data_dir']=}")

yaml_option = sys.argv[1]
print(f"{yaml_option=}")
an = driver.analysis()

# -- Read the yaml file
an.control = "./control_" + yaml_option + ".yaml"
print(f"{an.control=}")
an.read_control()

# -- Make output/plot directory for M-M analysis from yaml file
print("---- Creating melodies-monet plot output directory:")
cmd = "mkdir -p " + " " + an.control_dict["analysis"]["output_dir"]
print(f"{cmd=}")
os.system(cmd)

# -- Make a copy of the namelist in the plot directory for reference later
print("---- Melodies-monet control file:", an.control)
cmd = "cp " + an.control + " " + an.control_dict["analysis"]["output_dir"]
print(f"{cmd=}")
os.system(cmd)


# -- Create plots or stats based on yaml settings
if yaml_option == "save_paired":
    an.open_models()
    an.open_obs()
    an.pair_data()
    an.save_analysis()
elif (yaml_option == "spatial_overlay") or (yaml_option == "spatial_bias"):
    an.read_analysis()
    an.open_models()
    an.plotting()
elif yaml_option == "stats":
    an.read_analysis()
    an.stats()
else:
    an.read_analysis()
    an.plotting()

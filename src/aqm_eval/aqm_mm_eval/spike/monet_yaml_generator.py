import yaml
from jinja2 import Environment, FileSystemLoader

# Read namelist.yaml
with open("./namelist.yaml", "r") as f:
    namelist = yaml.safe_load(f)
tasks = namelist["mm_tasks"]

print("----------------------")
print("Selected MM tasks:")
print(tasks)
print("----------------------")

# Set up jinja2 environment
env = Environment(loader=FileSystemLoader(searchpath="."))

# Read template, render configs from namelist, and create control yaml file
for task in tasks:
    template = env.get_template("./yaml_template/template_" + task)

    if task == "scorecard_rmse":
        namelist["scorecard_eval_method"] = '"RMSE"'
    elif task == "scorecard_ioa":
        namelist["scorecard_eval_method"] = '"IOA"'
    elif task == "scorecard_nmb":
        namelist["scorecard_eval_method"] = '"NMB"'
    elif task == "scorecard_nme":
        namelist["scorecard_eval_method"] = '"NME"'

    with open("./control_" + task + ".yaml", "w") as f:
        f.write(template.render(**namelist))

# For chem evaluation
# template = env.get_template("./yaml_template/template_link_base")
# with open("./link_dyns_to_Alldays_base.sh", "w") as f:
#     f.write(template.render(**namelist))
template = env.get_template("./yaml_template/template_link_eval")
with open("./link_dyns_to_Alldays_eval.sh", "w") as f:
    f.write(template.render(**namelist))
template = env.get_template("./yaml_template/template_csi_rename_hit_rate")
with open("./pg8_csi_rename_hit_rate", "w") as f:
    f.write(template.render(**namelist))
template = env.get_template("./yaml_template/template_csi_rename_false_alarm_rate")
with open("./pg8_csi_rename_false_alarm_rate", "w") as f:
    f.write(template.render(**namelist))
template = env.get_template("./yaml_template/template_csi_rename_critical_success_index")
with open("./pg8_csi_rename_critical_success_index", "w") as f:
    f.write(template.render(**namelist))

# For met evaluation
# template = env.get_template("./yaml_template/template_ish_base")
# with open("./ish_conv_base.sh", "w") as f:
#     f.write(template.render(**namelist))
template = env.get_template("./yaml_template/template_ish_eval")
with open("./ish_conv_eval.sh", "w") as f:
    f.write(template.render(**namelist))

# For aqs pm evaluation
template = env.get_template("./yaml_template/template_aqs_pm_base")
with open("./aqs_pm_conv_base.sh", "w") as f:
    f.write(template.render(**namelist))
template = env.get_template("./yaml_template/template_aqs_pm_eval")
with open("./aqs_pm_conv_eval.sh", "w") as f:
    f.write(template.render(**namelist))


print("MM control yaml files generated!")

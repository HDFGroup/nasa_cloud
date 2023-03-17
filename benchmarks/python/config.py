##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
import os
import sys
import yaml
from pkg_resources import resource_filename

cfg = {}


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def debug(*args, **kwargs):
    # can't use log.debug since that calls back to cfg
    if "LOG_LEVEL" in os.environ and os.environ["LOG_LEVEL"] == "DEBUG":
        print("DEBUG>", *args, **kwargs)



def getCmdLineArg(x):
    # return value of command-line option
    # use "--x=val" to set option 'x' to 'val'
    # use "--x" for boolean flags
    option = "--" + x + "="
    for i in range(1, len(sys.argv)):
        arg = sys.argv[i]
        if arg == "--" + x:
            # boolean flag
            debug(f"got cmd line flag for {x}")
            return True
        elif arg.startswith(option):
            # found an override
            nlen = len(option)
            override = arg[nlen:]  # return text after option string
            debug(f"got cmd line override for {x}")
            return override
    return None


def _load_cfg():
    # load config yaml
    yml_file = None
    config_dirs = []
    # check if there is a command line option for config directory
    config_dir = getCmdLineArg("config-dir")
    # check cmdLineArg with underline
    if not config_dir:
        config_dir = getCmdLineArg("config_dir")

    if config_dir:
        config_dirs.append(config_dir)
    if not config_dirs and "CONFIG_DIR" in os.environ:
        config_dirs.append(os.environ["CONFIG_DIR"])
        debug(f"got environment override for config-dir: {config_dirs[0]}")
    if not config_dirs:
        debug("set default location for config dirs")
        config_dirs = ["../config",]  # default locations
    for config_dir in config_dirs:
        file_name = os.path.join(config_dir, "config.yml")
        debug("checking config path:", file_name)
        if os.path.isfile(file_name):
            yml_file = file_name
            break
        # Check for alt extension
        file_name = os.path.join(config_dir, "config.yaml")
        debug("checking config path:", file_name)
        if os.path.isfile(file_name):
            yml_file = file_name
            break

    if not yml_file:
        raise FileNotFoundError("unable to load config.yml")
    debug(f"_load_cfg with '{yml_file}'")
    try:
        with open(yml_file, "r") as f:
            yml_config = yaml.safe_load(f)
    except yaml.scanner.ScannerError as se:
        msg = f"Error parsing config.yml: {se}"
        eprint(msg)
        raise KeyError(msg) 

    # apply overrides for each key and store in cfg global
    for x in yml_config:
        cfgval = yml_config[x]
        # see if there is a command-line override
        override = getCmdLineArg(x)

        # see if there are an environment variable override
        if override is None and x.upper() in os.environ:
            override = os.environ[x.upper()]
            debug(f"got env value override for {x} ")

        if override is not None:
            if cfgval is not None:
                try:
                    # convert to same type as yaml
                    override = type(cfgval)(override)
                except ValueError as ve:
                    msg = "Error applying command line override value for "
                    msg += f"key: {x}: {ve}"
                    eprint(msg)
                    # raise KeyError(msg)
            cfgval = override  # replace the yml value

        cfg[x] = cfgval


def get(x, default=None):
    """get x if found in config
    otherwise return default
    """
    if not cfg:
        _load_cfg()
    if x not in cfg:
        if default is not None:
            cfg[x] = default
        else:
            return None
    return cfg[x]

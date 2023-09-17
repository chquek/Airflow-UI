#!/usr/bin/python3

import yaml
import random
import sys
import os
import configparser

# Add in cwd to PATH
os.environ['PATH'] = ".:" + os.environ['PATH'] 

# Guide : https://stackabuse.com/reading-and-writing-yaml-to-a-file-in-python/
version = "1.0.0"

class Yaml :

    def __init__(self, yaml_config ):

        with open(yaml_config) as file:
            self.config = yaml.load(file,Loader=yaml.FullLoader)

    def ds(self):
        return ( self.config )

    def get(self,fields):

        ptr = self.config
        for i in fields:
            ptr = ptr[i]

        return ptr

def show_menu ( menu ):

    '''
    # replace all ${X} with os.environ[X]
    for item in menu :
        print ( "menuitem" , item , menu[item] ) 
    '''
    # prepend with option to go back previous menu , and append with option to exit menu
    main = {"..": ".."}
    main.update(menu)
    main.update({"quit": "Quit"})

    # check if DIR exists , if yes , change to that folder
    if ( 'DIR' in main ):
        os.chdir ( dollar_replace(main['DIR']) )
        del main['DIR']

    # if NOTE exist, save the string - and display it as part of the menu.  NOTE is not an opton
    note = None
    if ( 'NOTE' in main ):
        note = dollar_replace(main['NOTE'])
        del main['NOTE']

    print( os.environ['PATH'] )
    os.system("clear")
    print("\n")
    print ( "YAML [%s] : %s , Current working directory = %s" %  (version,yfile,os.getcwd())  )
    print ( f"Note : {note}" ) if ( note ) else None
    print ( f"Last Command : {lastcmd}" ) if ( lastcmd ) else None
    print ()

    # convert to list and display as X : entry
    as_list = list(main)
    for  i in range ( 0,len(as_list) ) :
        key = as_list[i] 
        folder = ""
        if isinstance( main[key] , dict ) :
            folder = "*"
        print( "  [ %d ] : %s %s" % ( i, dollar_replace(key) , folder ) )
    print("\n")
    val = input("Enter choice : ")

    # catch for bad input
    try :
        idx = int(val)
    except ValueError:
        return "error"

    return "error" if ( idx > len(main)-1 or idx < 0 ) else val,as_list[int(val)]

import re
def run( cmd ) :

    # ------------------------------------------------------------------------------
    # substitute environment variables if it exists , replace ${X} with $env->{X}
    # ------------------------------------------------------------------------------
    # for key in env:
    #     meta = "${" + key + "}"
    #     cmd = re.sub( re.escape(meta) , env[key], cmd )

    '''
    look for [X]-[Y] - if exists , prompt of X, and if no input , set it to Y , otherwise set to input
    '''
    found = re.findall("(\[.*?\]-\[.*?\])", cmd )
    for match in found:
        (qn, ans) = re.findall("\[(.*)\]-\[(.*)\]", match)[0]
        val = input(f"{qn} [{ans}] : ")
        ans = val if (val) else ans
        cmd = re.sub(re.escape(match), ans, cmd )

    # global lastcmd
    # lastcmd = cmd
    os.system(cmd)

    input ("\nHit <ENTER> to continue :" )

def cfgparser (file ) :

    # Read config.ini file
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(file)
    rec = {}
    # do this to suppore interpolation
    for key in config.sections() :
        for inner_key in dict(config.items(key) ) :
            rec[inner_key] = config.get(key,inner_key)
            os.environ[inner_key] = config.get(key,inner_key)
    return rec

def shellconfig ( file ) :
    f = open( file , "r" )
    for line in f.readlines() :

        line = line.rstrip()
        if line.startswith("#") or re.match("^$",line)  :
            continue

        line = line.replace("export ","")

        for match in re.findall( "(.*?)=(.*)",line):
            os.environ[match[0]] = dollar_replace ( match[1] ) 

    f.close()

# -----------------------------------------------------------------------------------------------------------
# this replaces ${host} with the value of os.environ['host'].  avoid using str cos it is a built in function
# -----------------------------------------------------------------------------------------------------------
def dollar_replace ( tstr ) :

    found = re.findall("\$\{(.*?)\}", tstr )
    for match in found:
        meta = "${" + match + "}"
        if match not in os.environ :
            print ( f"Value %s is used in YAML but not defined in ENV , SHELLCONFIG , nor CONFIGPARSER" % ( match ) )
            sys.exit(0)
        tstr = re.sub(re.escape(meta), os.environ[match] , tstr )

    return tstr

# ------------------------------------------------------------------------------
# substitute environment variables if it exists , replace ${X} with $env->{X}
# ------------------------------------------------------------------------------
def envreplace ( obj ):

    for item in obj:
        value = obj[item]
        if ( type(value) == str ):
            for key in env:
                if key.startswith ("BASH_FUNC" ):	# ignore if the environment variable has this key. no use for it here.
                    continue
                meta = "${" + key + "}"
                # print ("META",meta)
                value = re.sub(re.escape(meta), env[key], value)
                obj[item] = value
        if ( type(value) == dict ):
            envreplace ( value )


if __name__ == '__main__':

    lastcmd = None
    # Priority - argument , local , environment variable
    if ( len(sys.argv) > 1 ):
        yfile = sys.argv[1]
    elif os.path.isfile('menu.yaml') :
        yfile = "menu.yaml"
    elif os.environ.get('YAMLFILE') :
        yfile = os.environ.get('YAMLFILE')
    else :
        print ("Cannot find YAML file")

    y = Yaml(yfile)
    top = y.get([])

    # read in environment variables
    if ( 'CONFIGPARSER' in top ):
        cfgparser( top['CONFIGPARSER'] )
        del top['CONFIGPARSER']

    # read in environment variables
    if ( 'SHELLCONFIG' in top ):
        shellconfig ( top['SHELLCONFIG'] )
        del top['SHELLCONFIG']

    # merge the environment variable and this YAML ENV variables
    if ( 'ENV' in top ):
        # assign the pointer to the ENV entry env2 and then delete ENV from menu.yaml
        env2 = y.get(['ENV'])
        for v in env2:
            os.environ[v] = str(env2[v])
        del top['ENV']

    env = os.environ

    # subsitute all options with environment variables , is this still needed since everything goes into os.environ now ?
    # envreplace ( top )

    menu = top      # set menu from the top level
    stack = []      # stack to track menu navigation
    while True:

        val , choice = show_menu ( menu )

        if ( choice == "error" ):
            print("Not a valid number")
            continue

        sys.exit(0) if ( choice == 'quit'  ) else None

        if ( choice == ".." ) :
            menu = stack.pop() if ( stack ) else menu
            continue

        if ( type(menu[choice]) == dict ):
            stack.append ( menu )             # save the parent and show the submenu
            menu = menu[choice]
            continue


        if (type(menu[choice]) == str ):    # no submenu.
            torun = dollar_replace ( menu[choice] )
            print("parsing : %s " % torun )
            lastcmd = f"[{val}] {choice} => {menu[choice]}"
            run ( torun )
            continue


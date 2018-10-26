#!/usr/bin/env python

import os
import subprocess


def main():
    print("Generating graphouse properties")
    with open('/etc/graphouse/graphouse.properties', 'w') as graphouse_config:
        for env_key, value in os.environ.items():
            if not env_key.startswith("GH__"):
                continue
            java_key = env_key.replace("GH", "graphouse", 1).lower().replace("__", ".").replace("_", "-")
            print ("Property '%s' value: %s" % (java_key, value))
            graphouse_config.write("%s=%s\n" % (java_key, value))

    print("Generating graphouse vm options")
    with open('/etc/graphouse/graphouse.vmoptions', 'w') as graphouse_vm_config:
        vm_xmx = os.environ.get("GH_XMX", "4g")
        vm_xms = os.environ.get("GH_XMS", "256m")
        vm_xss = os.environ.get("GH_XSS", "2m")
        print ("Xmx %s, xms %s, xss %s" % (vm_xmx, vm_xms, vm_xss))

        graphouse_vm_config.write('-Xmx%s\n' % vm_xmx)
        graphouse_vm_config.write('-Xms%s\n' % vm_xms)
        graphouse_vm_config.write('-Xss%s\n' % vm_xss)
        graphouse_vm_config.write('-XX:StringTableSize=10000000\n')
        graphouse_vm_config.write('-XX:+UseG1GC\n')
        graphouse_vm_config.write('-XX:MaxGCPauseMillis=1000\n')

    print ("Starting graphouse")
    subprocess.call("/opt/graphouse/bin/graphouse")


if __name__ == "__main__":
    main()

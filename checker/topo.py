#!/usr/bin/env python

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import Node
from mininet.log import setLogLevel, info
from mininet.cli import CLI
from mininet.link import TCLink
import sys
import time

class LinuxRouter( Node ):
    "A Node with IP forwarding enabled."

    # pylint: disable=arguments-differ
    def config( self, **params ):
        super( LinuxRouter, self).config( **params )
        # Enable forwarding on the router
        self.cmd( 'sysctl net.ipv4.ip_forward=1' )

    def terminate( self ):
        self.cmd( 'sysctl net.ipv4.ip_forward=0' )
        super( LinuxRouter, self ).terminate()


class NetworkTopo( Topo ):
    "A LinuxRouter connecting three IP subnets"


    def build( self, **_opts ):

        defaultIP = '192.168.1.1/24'  # IP address for r0-eth1
        router = self.addNode( 'r0', cls=LinuxRouter, ip=defaultIP) 


        h1 = self.addHost( 'h1', ip='192.168.1.100/24',
                           defaultRoute='via 192.168.1.1' )

        h2 = self.addHost( 'h2', ip='172.16.0.100/12',
                           defaultRoute='via 172.16.0.1' )

        # 8 Mb/s, 1ms delay, 5% packet loss
        self.l1_with_loss = self.addLink( h1, router, intfName1='h1-eth0', bw=8, delay='1ms', loss=5,
                     params1={ 'ip' : '192.168.1.1/24' })  # for clarity

        # 8 Mb/s, 1ms delay, 5% packet loss
        self.l2_with_loss = self.addLink(h2, router, intfName2='eth0', bw=8, delay='1ms', loss=5,
                      params2={ 'ip' : '172.16.0.1/12' } )


class NetworkManager(object):
    def __init__(self, net):
        self.h1 = net.get("h1")
        self.h2 = net.get("h2")
        self.net = net

    def copy_binaries(self, file_path):
        self.work_folder_h1 = self.h1.cmd("mktemp -d").strip()
        self.work_folder_h2 = self.h2.cmd("mktemp -d").strip()
        print("[INFO] Using {} and {} as tmpdirs".format(self.work_folder_h1, self.work_folder_h2))
        # Copy the test file
        for i in file_path:
            self.h1.cmd("cp checker/tests/{} {}".format(i, self.work_folder_h1))

        # Copy the binaries
        self.h1.cmd("cp -r client {}".format(self.work_folder_h1))
        self.h2.cmd("cp -r server {}".format(self.work_folder_h2))

    def set_link(self, loss = True, corruption = False):
        loss = 10
    
    def cleanup(self):
        # kill server client if already running
        self.h1.cmd("pkill -9 client")
        self.h2.cmd("pkill -9 server")
    
    def enabled_corruption(self):
        self.h1.cmd("tc qdisc replace dev h1-eth0 root netem corrupt 5%")
        self.h2.cmd("tc qdisc replace dev h2-eth0 root netem corrupt 5%")

    def disable_corruption(self):
        self.h1.cmd("tc qdisc delete dev h1-eth0 root")
        self.h2.cmd("tc qdisc delete dev h2-eth0 root")

    def run_test(self,  test_file, sleep_time, loss = True, corruption = False):
        self.cleanup()
        self.copy_binaries([test_file])
        self.set_link()
        #if (corruption):
        #    self.enabled_corruption()

        # We start the server
        server = self.h2.popen("cd {} && ./server > server.out 2> server.err ".format(self.work_folder_h2), shell=True)

        # Wait for the server to open
        time.sleep(1)

        # We start the client
        client = self.h1.popen("cd {} && ./client {} > client.out 2> client.err".format(self.work_folder_h1, test_file), shell=True)
        
        # Sleep based on the optimum time
        time.sleep(sleep_time)

        res = self.h2.cmd("diff -q {}/{} {}/file.out".format(self.work_folder_h1, test_file, self.work_folder_h2))
        print("[Info] Running diff -q {}/{} {}/file.out".format(self.work_folder_h1, 
                                    test_file, self.work_folder_h2))

        runtime = self.h2.cmd('cat {}/server.out  | grep "Total time"'.format(self.work_folder_h2))

        if (runtime != ""):
            print("[Info] {}".format(runtime))
        else:
            print("[Info] Server didn't finish in time")

        self.cleanup()

        #if (corruption):
        #    self.disable_corruption()

        if res == "":
            return True
        else:
            return False


    def run_test_multiple(self,  test_files, loss = True, corruption = False):


        self.cleanup()
        self.copy_binaries(test_files)
        self.set_link()
        if (corruption):
            self.enabled_corruption()

        num_clients = len(test_files)
        # We start the server
        server = self.h2.popen("cd {} && ./server {} > server.out 2> server.err ".format(self.work_folder_h2, num_clients), shell=True)

        # Wait for the server to open
        time.sleep(3)

        # We start the clients
        for i in range(num_clients):
            client = self.h1.popen("cd {} && ./client {} > client{}.out 2> client{}.err".format(self.work_folder_h1, test_files[i], i, i), shell=True)
         
        # Sleep based on the optimum time
        time.sleep(20)

        self.cleanup()

        ok = True

        for i in range(num_clients):
            # Connections may not be in order, check if the file is there
            for j in test_files:
                res = self.h2.cmd("diff -q {}/{} {}/file{}.out".format(self.work_folder_h1, j, self.work_folder_h2, i))
                if res == "":
                    break
        
            print("[Info] Running diff between {} with all the output files since connection may be out of order".format(test_files[i]))

            if res != "":
                ok = False
                break


        if (corruption):
            self.disable_corruption()

        if ok == True:
            return True
        else:
            return False

def test_runer_helper(nm, testname, points, testfile, loss, corruption, multiple, sleep_time):

    print("## Running Test - {} - cutoff transfer time {}s - {}  .........".format(testname, sleep_time, testfile))
    
    if multiple:
        res = nm.run_test_multiple(testfile, loss, corruption)
    else:
        res = nm.run_test(testfile, sleep_time, loss, corruption)

    if (res):
        print("Test Result: PASS [{}]".format(points))
        return points
    else:
        print("Test Result: FAIL [{}]".format(points))
        return 0

    sleep(1)

def run(tn, run_tests = False):
    "Test linux router"
    topo = NetworkTopo()
    net = Mininet( topo=topo, link=TCLink, controller=None,
                   waitConnected=True )  # controller is used by s1-s3
    net.start()

    if run_tests == False:
        net.startTerms()
        CLI( net )
    else:
        nm = NetworkManager(net)
        points = 0

        if tn == "" or tn == "corruption_text":
            # Testing with loss
            points += test_runer_helper(nm, "Corruption Text", 10, "lorem_1.txt", True, False, 0, 1.5)
            points += test_runer_helper(nm, "Corruption Text", 10, "lorem_100.txt", True, False, 0, 2)
            points += test_runer_helper(nm, "Corruption Text", 10, "gameofthrones.txt", True, False, 0, 40)

        if tn == "" or tn == "corruption_image":
            # Testing with loss and corruption
            test_name = "Corruption Binary"
            points += test_runer_helper(nm, test_name, 5, "router.jpg", True, True, 0, 3)
            points += test_runer_helper(nm, test_name, 10, "usa.jpg", True, True, 0, 5)
            points += test_runer_helper(nm, test_name, 10, "datacenter.jpg", True, True, 0, 20)
            points += test_runer_helper(nm, test_name, 10, "starlink.jpg", True, True, 0, 35)

        # Testing with multiple connections (includes points for three way handshake)
        if tn == "" or tn == "connection_and_multiconnection":
            test_name = "Multiple connections and connection"
            points += test_runer_helper(nm, test_name, 15, ["lorem_1.txt", "router.jpg"], True, True, 1, 15)
            time.sleep(3)
            points += test_runer_helper(nm, test_name, 20, ["lorem_100.txt", "router.jpg", "scan.txt", "lorem_1.txt"], True, True, 1, 15)

        if (tn == ""):
            print("Total: {}/100".format(points))

    net.stop()


if __name__ == '__main__':
    setLogLevel('critical')

    if len(sys.argv) > 1 and sys.argv[1] == "tests":
        if len(sys.argv) > 2:
            test_name = sys.argv[2]
            run(test_name, run_tests=True)
        else:
            run("", run_tests=True)
    else:
        run("", run_tests=False)

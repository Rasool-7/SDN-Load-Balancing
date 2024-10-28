#!/usr/bin/python

'This is the topology for ACN project'
from import_topology import *
from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import matplotlib.pyplot as plt

def measure_average_latency(host, target_ip, num_requests):
    """Measure the average latency over a number of ping requests."""
    latencies = []
    
    for i in range(num_requests):
        # Ping target and extract the latency value
        output = host.cmd(f'ping -c 1 {target_ip}')
        latency = None
        for line in output.split('\n'):
            if 'avg' in line:
                latency = line.split('=')[-1].split('/')[1]
                break
        
        if latency:
            latencies.append(float(latency))
        else:
            latencies.append(float('inf'))  # Handle timeouts as 'inf'

    # Calculate average latency
    if latencies:
        average_latency = sum(latencies) / len(latencies)
        return average_latency
    else:
        return float('inf')

def topology():
    'Create a network and controller'
    net = Mininet(controller=RemoteController, switch=OVSKernelSwitch)
    protocolName = "OpenFlow13"

    c0 = net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)
    c1 = net.addController('c1', controller=RemoteController, ip='127.0.0.2', port=6654)
    
    info("*** Creating the nodes\n")
     
    h1 = net.addHost('h1', ip='10.0.0.1/24', position='10,10,0')
    h2 = net.addHost('h2', ip='10.0.0.2/24', position='20,10,0')
    
    switch1 = net.addSwitch('switch1', protocols=protocolName, position='12,10,0')
    switch2 = net.addSwitch('switch2', protocols=protocolName, position='15,20,0')
    switch3 = net.addSwitch('switch3', protocols=protocolName, position='18,10,0')
    switch4 = net.addSwitch('switch4', protocols=protocolName, position='14,10,0')
    switch5 = net.addSwitch('switch5', protocols=protocolName, position='16,10,0')
    switch6 = net.addSwitch('switch6', protocols=protocolName, position='14,0,0')
    switch7 = net.addSwitch('switch7', protocols=protocolName, position='16,0,0')
    switch8 = net.addSwitch('switch8', protocols=protocolName, position='16,0,2')
    switch9 = net.addSwitch('switch9', protocols=protocolName, position='16,0,3')

    
    info("*** Adding the Link\n")
    net.addLink(h1, switch1)
    net.addLink(switch1, switch2)
    net.addLink(switch1, switch4)
    net.addLink(switch1, switch6)
    net.addLink(switch2, switch3)
    net.addLink(switch4, switch5)
    net.addLink(switch5, switch3)
    net.addLink(switch6, switch7)
    net.addLink(switch7, switch3)
    net.addLink(switch7, switch9)
    net.addLink(switch8, switch5)
    net.addLink(switch3, h2)

    info("*** Starting the network\n")
    net.build()
    c0.start()
    switch1.start([c0])
    switch2.start([c0])
    switch3.start([c0])
    switch4.start([c0])
    switch5.start([c0])
    switch6.start([c0])
    switch7.start([c0])
    switch8.start([c1])
    switch9.start([c1])
    
    # Run a ping test to ensure connectivity
    net.pingFull()

    # Measure average latencies for different request counts
    request_counts = [100, 500, 1000, 5000]
    average_latencies = []

    info("*** Measuring average latency between h1 and h2 for different request counts\n")
    
    for count in request_counts:
        avg_latency = measure_average_latency(h1, '10.0.0.2', count)
        average_latencies.append(avg_latency)
        print(f"Average latency for {count} requests: {avg_latency:.4f} ms")

    # Plot average latency data
    plt.plot(request_counts, average_latencies, marker='o')
    plt.title('Average Ping Latency')
    plt.xlabel('Number of Ping Requests')
    plt.ylabel('Average Latency (ms)')
    plt.grid(True)
    plt.show()

    info("*** Running the CLI\n")
    CLI(net)

    info("*** Stopping network\n")
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    topology()

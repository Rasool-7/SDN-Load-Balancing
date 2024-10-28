# Copyright 2013,2014 James McCauley
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
A very sloppy IP load balancer.

Run it with --ip=<Service IP> --servers=IP1,IP2,...

By default, it will do load balancing on the first switch that connects.  If
you want, you can add --dpid=<dpid> to specify a particular switch.

Please submit improvements. :)
"""

from pox.core import core
import pox
log = core.getLogger("iplb")

from pox.lib.packet.ethernet import ethernet, ETHER_BROADCAST
from pox.lib.packet.ipv4 import ipv4
from pox.lib.packet.arp import arp
from pox.lib.addresses import IPAddr, EthAddr
from pox.lib.util import str_to_bool, dpid_to_str, str_to_dpid

import pox.openflow.libopenflow_01 as of

import time
import random

FLOW_IDLE_TIMEOUT = 10
FLOW_MEMORY_TIMEOUT = 60 * 5



class MemoryEntry (object):
  """
  Record for flows we are balancing

  Table entries in the switch "remember" flows for a period of time, but
  rather than set their expirations to some long value (potentially leading
  to lots of rules for dead connections), we let them expire from the
  switch relatively quickly and remember them here in the controller for
  longer.

  Another tactic would be to increase the timeouts on the switch and use
  the Nicira extension which can match packets with FIN set to remove them
  when the connection closes.
  """
  def __init__ (self, server, first_packet, client_port):
    self.server = server
    self.first_packet = first_packet
    self.client_port = client_port
    self.refresh()

  def refresh (self):
    self.timeout = time.time() + FLOW_MEMORY_TIMEOUT

  @property
  def is_expired (self):
    return time.time() > self.timeout

  @property
  def key1 (self):
    ethp = self.first_packet
    ipp = ethp.find('ipv4')
    tcpp = ethp.find('tcp')

    return ipp.srcip,ipp.dstip,tcpp.srcport,tcpp.dstport

  @property
  def key2 (self):
    ethp = self.first_packet
    ipp = ethp.find('ipv4')
    tcpp = ethp.find('tcp')

    return self.server,ipp.srcip,tcpp.dstport,tcpp.srcport


class iplb (object):
    """
    A simple IP load balancer with weighted round-robin

    Give it a service_ip and a list of server IP addresses. 
    New TCP flows to service_ip will be redirected to one of the servers based on weighted round-robin.
    """

    def __init__ (self, connection, service_ip, servers = [], weights = []):
        self.service_ip = IPAddr(service_ip)
        self.servers = [IPAddr(a) for a in servers]
        self.weights = weights if weights else [1] * len(servers)
        self.server_weights = self._expand_weights(self.weights)
        self.current_server_index = 0
        self.con = connection
        self.mac = self.con.eth_addr
        self.live_servers = {} 
        self.outstanding_probes = {} 
        self.probe_cycle_time = 5
        self.arp_timeout = 3
        self.memory = {} 
        self._probe_wait_time = 2  # Set the probe wait time in seconds
        self._do_probe()
        try:
            self.log = log.getChild(dpid_to_str(self.con.dpid))
        except:
            # Be nice to Python 2.6 (ugh)
            self.log = log

        self.outstanding_probes = {}  # IP -> expire_time
        self.probe_cycle_time = 5
        self.arp_timeout = 3
        self.memory = {}  # (srcip,dstip,srcport,dstport) -> MemoryEntry
        self._do_probe()  # Kick off the probing
    def _do_expire(self):
            """
            Expire probes and memory entries that have timed out
            """
            # Expire ARP probes
            current_time = time.time()
            expired_probes = [ip for ip, expire_time in self.outstanding_probes.items() if expire_time <= current_time]
            for ip in expired_probes:
                del self.outstanding_probes[ip]
    
            # Expire memory entries
            expired_memory_keys = [key for key, entry in self.memory.items() if entry.is_expired]
            for key in expired_memory_keys:
                del self.memory[key]
    def _expand_weights(self, weights):
        """
        Expand the weights into a list of server indices based on their weights
        """
        expanded_weights = []
        for index, weight in enumerate(weights):
            expanded_weights.extend([index] * weight)
        return expanded_weights

    def _pick_server(self, key, inport):
        """
        Pick a server using weighted round-robin
        """
        if not self.live_servers:
            return None  # Handle case when there are no live servers

        # Select server index based on expanded weights
        server_index = self.server_weights[self.current_server_index]
        server = list(self.live_servers.keys())[server_index]

        # Move to the next server for the next connection
        self.current_server_index = (self.current_server_index + 1) % len(self.server_weights)

        return server

    def _do_probe(self):
        """
        Send an ARP to a server to see if it's still up
        """
        self._do_expire()

        server = self.servers.pop(0)
        self.servers.append(server)

        r = arp()
        r.hwtype = r.HW_TYPE_ETHERNET
        r.prototype = r.PROTO_TYPE_IP
        r.opcode = r.REQUEST
        r.hwdst = ETHER_BROADCAST
        r.protodst = server
        r.hwsrc = self.mac
        r.protosrc = self.service_ip
        e = ethernet(type=ethernet.ARP_TYPE, src=self.mac,
                     dst=ETHER_BROADCAST)
        e.set_payload(r)
        msg = of.ofp_packet_out()
        msg.data = e.pack()
        msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
        msg.in_port = of.OFPP_NONE
        self.con.send(msg)

        self.outstanding_probes[server] = time.time() + self.arp_timeout

        core.callDelayed(self._probe_wait_time, self._do_probe)

    def _handle_PacketIn (self, event):
        inport = event.port
        packet = event.parsed

        def drop():
            if event.ofp.buffer_id is not None:
                # Kill the buffer
                msg = of.ofp_packet_out(data=event.ofp)
                self.con.send(msg)
            return None

        tcpp = packet.find('tcp')
        if not tcpp:
            arpp = packet.find('arp')
            if arpp:
                # Handle replies to our server-liveness probes
                if arpp.opcode == arpp.REPLY:
                    if arpp.protosrc in self.outstanding_probes:
                        # A server is (still?) up; cool.
                        del self.outstanding_probes[arpp.protosrc]
                        if (self.live_servers.get(arpp.protosrc, (None,None))
                                == (arpp.hwsrc,inport)):
                            pass
                        else:
                            self.live_servers[arpp.protosrc] = arpp.hwsrc,inport
                            self.log.info("Server %s up", arpp.protosrc)
                return

            # Not TCP and not ARP. Drop it.
            return drop()

        ipp = packet.find('ipv4')

        if ipp.srcip in self.servers:
            key = ipp.srcip,ipp.dstip,tcpp.srcport,tcpp.dstport
            entry = self.memory.get(key)

            if entry is None:
                self.log.debug("No client for %s", key)
                return drop()

            entry.refresh()

            mac,port = self.live_servers[entry.server]
            actions = []
            actions.append(of.ofp_action_dl_addr.set_src(self.mac))
            actions.append(of.ofp_action_nw_addr.set_src(self.service_ip))
            actions.append(of.ofp_action_output(port=entry.client_port))
            match = of.ofp_match.from_packet(packet, inport)

            msg = of.ofp_flow_mod(command=of.OFPFC_ADD,
                                  idle_timeout=FLOW_IDLE_TIMEOUT,
                                  hard_timeout=of.OFP_FLOW_PERMANENT,
                                  data=event.ofp,
                                  actions=actions,
                                  match=match)
            self.con.send(msg)

        elif ipp.dstip == self.service_ip:
            key = ipp.srcip,ipp.dstip,tcpp.srcport,tcpp.dstport
            entry = self.memory.get(key)
            if entry is None or entry.server not in self.live_servers:
                if len(self.live_servers) == 0:
                    self.log.warn("No servers!")
                    return drop()

                server = self._pick_server(key, inport)
                self.log.debug("Directing traffic to %s", server)
                entry = MemoryEntry(server, packet, inport)
                self.memory[entry.key1] = entry
                self.memory[entry.key2] = entry

            entry.refresh()

            mac,port = self.live_servers[entry.server]
            actions = []
            actions.append(of.ofp_action_dl_addr.set_dst(mac))
            actions.append(of.ofp_action_nw_addr.set_dst(entry.server))
            actions.append(of.ofp_action_output(port=port))
            match = of.ofp_match.from_packet(packet, inport)

            msg = of.ofp_flow_mod(command=of.OFPFC_ADD,
                                  idle_timeout=FLOW_IDLE_TIMEOUT,
                                  hard_timeout=of.OFP_FLOW_PERMANENT,
                                  data=event.ofp,
                                  actions=actions,
                                  match=match)
            self.con.send(msg)

# Remember which DPID we're operating on (first one to connect)
_dpid = None

def launch (ip, servers, weights=None, dpid=None):
    global _dpid
    if dpid is not None:
        _dpid = str_to_dpid(dpid)

    servers = servers.replace(","," ").split()
    servers = [IPAddr(x) for x in servers]
    ip = IPAddr(ip)

    if weights:
        weights = [int(x) for x in weights.split(",")]
    else:
        weights = [1] * len(servers)  # Default weight is 1 for all servers

    from proto.arp_responder import ARPResponder
    old_pi = ARPResponder._handle_PacketIn
    def new_pi (self, event):
        if event.dpid == _dpid:
            return old_pi(self, event)
    ARPResponder._handle_PacketIn = new_pi

    from proto.arp_responder import launch as arp_launch
    arp_launch(eat_packets=False, **{str(ip):True})
    import logging
    logging.getLogger("proto.arp_responder").setLevel(logging.WARN)

    def _handle_ConnectionUp (event):
        global _dpid
        if _dpid is None:
            _dpid = event.dpid

        if _dpid != event.dpid:
            log.warn("Ignoring switch %s", event.connection)
        else:
            if not core.hasComponent('iplb'):
                core.registerNew(iplb, event.connection, IPAddr(ip), servers, weights)
                log.info("IP Load Balancer Ready.")
            log.info("Load Balancing on %s", event.connection)

            core.iplb.con = event.connection
            event.connection.addListeners(core.iplb)

    core.openflow.addListenerByName("ConnectionUp", _handle_ConnectionUp)
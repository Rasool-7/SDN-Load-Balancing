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
        return ipp.srcip, ipp.dstip, tcpp.srcport, tcpp.dstport

    @property
    def key2 (self):
        ethp = self.first_packet
        ipp = ethp.find('ipv4')
        tcpp = ethp.find('tcp')
        return self.server, ipp.srcip, tcpp.dstport, tcpp.srcport

class iplb (object):
    def __init__ (self, connection, service_ip, servers = []):
        self.service_ip = IPAddr(service_ip)
        self.servers = [IPAddr(a) for a in servers]
        self.con = connection
        self.mac = self.con.eth_addr
        self.live_servers = {} # IP -> MAC, port
        self.connection_counts = {server: 0 for server in self.servers} # Static connection counts

        try:
            self.log = log.getChild(dpid_to_str(self.con.dpid))
        except:
            self.log = log

        self.outstanding_probes = {} # IP -> expire_time
        self.probe_cycle_time = 5
        self.arp_timeout = 3
        self.memory = {} # (srcip, dstip, srcport, dstport) -> MemoryEntry

        self._do_probe()

    def _do_expire (self):
        t = time.time()
        for ip, expire_at in list(self.outstanding_probes.items()):
            if t > expire_at:
                self.outstanding_probes.pop(ip, None)
                if ip in self.live_servers:
                    self.log.warn("Server %s down", ip)
                    del self.live_servers[ip]

        c = len(self.memory)
        self.memory = {k: v for k, v in self.memory.items() if not v.is_expired}
        if len(self.memory) != c:
            self.log.debug("Expired %i flows", c - len(self.memory))

    def _do_probe (self):
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
        e = ethernet(type=ethernet.ARP_TYPE, src=self.mac, dst=ETHER_BROADCAST)
        e.set_payload(r)
        msg = of.ofp_packet_out()
        msg.data = e.pack()
        msg.actions.append(of.ofp_action_output(port=of.OFPP_FLOOD))
        msg.in_port = of.OFPP_NONE
        self.con.send(msg)
        self.outstanding_probes[server] = time.time() + self.arp_timeout
        core.callDelayed(self._probe_wait_time, self._do_probe)

    @property
    def _probe_wait_time (self):
        r = self.probe_cycle_time / float(len(self.servers))
        r = max(.25, r)
        return r

    def _pick_server (self, key, inport):
        """
        Pick a server for a (hopefully) new connection
        """
        if not self.live_servers:
            self.log.warn("No servers!")
            return None

        # Choose the server with the least number of connections
        min_connections = float('inf')
        selected_server = None

        for server in self.live_servers:
            count = self.connection_counts.get(server, 0)
            if count < min_connections:
                min_connections = count
                selected_server = server

        return selected_server

    def _handle_PacketIn (self, event):
        inport = event.port
        packet = event.parsed

        def drop ():
            if event.ofp.buffer_id is not None:
                msg = of.ofp_packet_out(data=event.ofp)
                self.con.send(msg)
            return None

        tcpp = packet.find('tcp')
        if not tcpp:
            arpp = packet.find('arp')
            if arpp:
                if arpp.opcode == arpp.REPLY:
                    if arpp.protosrc in self.outstanding_probes:
                        del self.outstanding_probes[arpp.protosrc]
                        if (self.live_servers.get(arpp.protosrc, (None, None))
                            == (arpp.hwsrc, inport)):
                            pass
                        else:
                            self.live_servers[arpp.protosrc] = arpp.hwsrc, inport
                            self.connection_counts[arpp.protosrc] = 0
                            self.log.info("Server %s up", arpp.protosrc)
                return

            return drop()

        ipp = packet.find('ipv4')

        if ipp.srcip in self.servers:
            key = ipp.srcip, ipp.dstip, tcpp.srcport, tcpp.dstport
            entry = self.memory.get(key)
            if entry is None:
                self.log.debug("No client for %s", key)
                return drop()
            entry.refresh()
            mac, port = self.live_servers[entry.server]
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
            key = ipp.srcip, ipp.dstip, tcpp.srcport, tcpp.dstport
            entry = self.memory.get(key)
            if entry is None or entry.server not in self.live_servers:
                if len(self.live_servers) == 0:
                    self.log.warn("No servers!")
                    return drop()

                server = self._pick_server(key, inport)
                if server is None:
                    self.log.warn("No available server!")
                    return drop()

                self.log.debug("Directing traffic to %s", server)
                entry = MemoryEntry(server, packet, inport)
                self.memory[entry.key1] = entry
                self.memory[entry.key2] = entry
                # Update connection count
                self.connection_counts[server] += 1

            entry.refresh()
            mac, port = self.live_servers[entry.server]
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

        else:
            drop()

        # Handle the end of the connection
        def handle_connection_close(entry):
            if entry.server in self.connection_counts:
                self.connection_counts[entry.server] -= 1

        # Implement logic to handle connection closure
        # (This part depends on how you handle connection closing in your environment)

# Remember which DPID we're operating on (first one to connect)
_dpid = None

def launch (ip, servers, dpid = None):
    global _dpid
    if dpid is not None:
        _dpid = str_to_dpid(dpid)

    servers = servers.replace(",", " ").split()
    servers = [IPAddr(x) for x in servers]
    ip = IPAddr(ip)

    from proto.arp_responder import ARPResponder
    old_pi = ARPResponder._handle_PacketIn
    def new_pi (self, event):
        if event.dpid == _dpid:
            return old_pi(self, event)
    ARPResponder._handle_PacketIn = new_pi

    from proto.arp_responder import launch as arp_launch
    arp_launch(eat_packets=False, **{str(ip): True})
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
                core.registerNew(iplb, event.connection, IPAddr(ip), servers)
                log.info("IP Load Balancer Ready.")
            log.info("Load Balancing on %s", event.connection)
            core.iplb.con = event.connection
            event.connection.addListeners(core.iplb)

    core.openflow.addListenerByName("ConnectionUp", _handle_ConnectionUp)

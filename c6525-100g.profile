"""Eight 100g nodes interconnected with a single switch."""

# Import the Portal object.
import geni.portal as portal
# Import the ProtoGENI library.
import geni.rspec.pg as pg
# Import the Emulab specific extensions.
import geni.rspec.emulab as emulab

# Create a portal object,
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

# Node node-0
node_0 = request.RawPC('node-0')
node_0.routable_control_ip = True
node_0.hardware_type = 'c6525-100g'
node_0.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+shenango-PG0//nu-100g'
iface0 = node_0.addInterface('interface-0')
iface1 = node_0.addInterface('interface-1')

# Node node-1
node_1 = request.RawPC('node-1')
node_1.routable_control_ip = True
node_1.hardware_type = 'c6525-100g'
node_1.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+shenango-PG0//nu-100g'
iface2 = node_1.addInterface('interface-2')
iface3 = node_1.addInterface('interface-3')

# Node node-2
node_2 = request.RawPC('node-2')
node_2.routable_control_ip = True
node_2.hardware_type = 'c6525-100g'
node_2.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+shenango-PG0//nu-100g'
iface4 = node_2.addInterface('interface-4')
iface5 = node_2.addInterface('interface-5')

# Node node-3
node_3 = request.RawPC('node-3')
node_3.routable_control_ip = True
node_3.hardware_type = 'c6525-100g'
node_3.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+shenango-PG0//nu-100g'
iface6 = node_3.addInterface('interface-6')
iface7 = node_3.addInterface('interface-7')

# Node node-4
node_4 = request.RawPC('node-4')
node_4.routable_control_ip = True
node_4.hardware_type = 'c6525-100g'
node_4.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+shenango-PG0//nu-100g'
iface8 = node_4.addInterface('interface-8')
iface9 = node_4.addInterface('interface-9')

# Node node-5
node_5 = request.RawPC('node-5')
node_5.routable_control_ip = True
node_5.hardware_type = 'c6525-100g'
node_5.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+shenango-PG0//nu-100g'
iface10 = node_5.addInterface('interface-10')
iface11 = node_5.addInterface('interface-11')

# Node node-6
node_6 = request.RawPC('node-6')
node_6.routable_control_ip = True
node_6.hardware_type = 'c6525-100g'
node_6.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+shenango-PG0//nu-100g'
iface12 = node_6.addInterface('interface-12')
iface13 = node_6.addInterface('interface-13')

# Node node-7
node_7 = request.RawPC('node-7')
node_7.routable_control_ip = True
node_7.hardware_type = 'c6525-100g'
node_7.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+shenango-PG0//nu-100g'
iface14 = node_7.addInterface('interface-14')
iface15 = node_7.addInterface('interface-15')

# Link link-0 for WAN connection
link_0 = request.Link('link-0')
link_0.Site('undefined')
link_0.best_effort = False
link_0.bandwidth = 25000000
#link_0.setNoInterSwitchLinks()
link_0.addInterface(iface0)
link_0.addInterface(iface2)
link_0.addInterface(iface4)
link_0.addInterface(iface6)
link_0.addInterface(iface8)
link_0.addInterface(iface10)
link_0.addInterface(iface12)
link_0.addInterface(iface14)

# Link link-1 for 100g LAN connection
link_1 = request.Link('link-1')
link_1.Site('undefined')
link_1.best_effort = False
link_1.bandwidth = 100000000
#link_1.setNoInterSwitchLinks()
link_1.addInterface(iface1)
link_1.addInterface(iface3)
link_1.addInterface(iface5)
link_1.addInterface(iface7)
link_1.addInterface(iface9)
link_1.addInterface(iface11)
link_1.addInterface(iface13)
link_1.addInterface(iface15)

# Print the generated rspec

"""100g servers for running Nu."""

# Import the Portal object.
import geni.portal as portal
# Import the ProtoGENI library.
import geni.rspec.pg as pg
# Import the Emulab specific extensions.
import geni.rspec.emulab as emulab

# Describe the parameter(s) this profile script can accept.
portal.context.defineParameter( "n", "Number of Machines", portal.ParameterType.INTEGER, 8 )

# Retrieve the values the user specifies during instantiation.
params = portal.context.bindParameters()

# Create a portal object,
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

nodes = []
ifaces_25 = []
ifaces_100 = []

for i in range(0, params.n):
    n = request.RawPC('node-%d' % i)
    n.routable_control_ip = True
    n.hardware_type = 'c6525-100g'
    n.disk_image = 'urn:publicid:IDN+utah.cloudlab.us+image+shenango-PG0//nu-100g'
    nodes.append(n)
    iface_25 = n.addInterface('interface-%d' % (2 * i), pg.IPv4Address('10.10.1.%d' % (i + 1),'255.255.255.0'))
    ifaces_25.append(iface_25)
    iface_100 = n.addInterface('interface-%d' % (2 * i + 1), pg.IPv4Address('10.10.2.%d' % (i + 1),'255.255.255.0'))
    ifaces_100.append(iface_100)

# Link link-25 for 25g WAN connection
link_25 = request.Link('link-25')
link_25.Site('undefined')
link_25.best_effort = False
link_25.bandwidth = 25000000
for iface in ifaces_25:
    link_25.addInterface(iface)

# Link link-100 for 100g LAN connection
link_100 = request.Link('link-100')
link_100.Site('undefined')
link_100.best_effort = False
link_100.bandwidth = 100000000
for iface in ifaces_100:
    link_100.addInterface(iface)

# Print the generated rspec
pc.printRequestRSpec(request)

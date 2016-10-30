# AuroraArc
This project is in extremely early phases.  Unless interested in contributing, I would keep looking before bringing this into your projects.

Primary goals:
* Small footprint
* High performance
* Few dependencies, but not necessairly dependency free

Initially this project will be a layer of connection management on top of the normal MySQL java jdbc connector.  Allowing aurora functionality, and robust failover, while depending on a battle tested driver.  In addition to handling aurora's robust and fast failover, it is also designed to handling reading from the secondary read replica servers.

Eventually this driver may end up replacing the mysql jdbc driver dependency with a ground up implementation in order to gain specific performance gains.

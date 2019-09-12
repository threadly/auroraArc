# AuroraArc [![Build status](https://badge.buildkite.com/eccfc05b1cb909831416e399fe015cc14aaae9631e19e5d101.svg)](https://buildkite.com/threadly/nightly-auroraarc)
This project is in extremely early phases (though may be usable).  Unless interested in contributing, I would keep looking before bringing this into your projects.

The goal is to provide a fault tolerant, and high performance, driver for AWS's Aurora servers.  The driver is designed to be small and light weight, with minimal external dependencies.

Include the auroraArc driver into your project from maven central...

For MySQL:

```script
<dependency>
	<groupId>org.threadly</groupId>
	<artifactId>auroraArc-mysql</artifactId>
	<version>0.11</version>
</dependency>
```

For Postgresql:

```script
<dependency>
  <groupId>org.threadly</groupId>
  <artifactId>auroraArc-psql</artifactId>
  <version>0.11</version>
</dependency>
```

## How stable is it?

Like most open source projects this project is use at your own risk.  At this point we have reached a usable level of stability.  A version 1.0 release will indicate that we feel it has reached API and functionality stability.  There are still many basic improvements that I think are important to accomplish before that release.

At this point the driver is functionally stable.  Stability improvements in the most recent months have been more edge cases with only incremental improvements.  This driver has had extensive production usage now.  At least one company has used this driver as their primary JDBC driver for most back end services for well over a year.  This has allowed us to work out some even some uncommon condition improvements.  This has been used and tested most extensively with JDBI + HikariCP or direct manual use.  Use in other tools are more likely to uncover unexpected behavior.  If there is a specific tooling configuration you would like tested or are curious about please open an issue.

## How it works
This project currently works by having each aurora "Connection" actually be a management layer which under the hood is selecting the connection to the actual aurora host to be used.  The primary reason for this is we can then depend on the normal, and battle tested, MySQL jdbc connector (in the future we may allow any delegated driver to be used).  Resulting in the primary concern of this driver to become when we can change the underline connection, and which connection to select to distribute load across the cluster evenly.

The driver aims to facilitate taking advantages of Aurora's low secondary read slave's latency by allowing the connection to be directed to a random (but healthy) secondary server when the connection is set to be `read only`.  In conditions where no healthy secondary server is available, the primary will be used for these read only requests.

One of the most significant advantages to Aurora is how fast failovers can be.  Supporting this in the best way possible was a primary motivation in building this driver.  Our drivers aurora connections are able to share a common view of the cluster.  Meaning that failover events are recognized quickly, maybe from the cluster monitoring itself, or as one connection has issues, it can initiate checks to try and minimize failures.  We also do best effort attempts, for example if the master server has gone down without a clear replacement yet we will attempt to leverage the secondary servers to and maintain what we can for read only actions.

Looking towards the future, it should be easy to allow other drivers to be used as well if there is a need.  In time this may become a great layer for any primary / secondary sql setup where one wants to leverage reading from the secondary replica.

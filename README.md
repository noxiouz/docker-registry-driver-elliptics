# Docker registry elliptics driver

This is a [docker-registry backend driver](https://github.com/dotcloud/docker-registry/tree/master/depends/docker-registry-core) based on the [Elliptics](http://reverbrain.com/elliptics/) key-value storage.

[![PyPI version][pypi-image]][pypi-url]
[![Build Status][travis-image]][travis-url]
[![Coverage Status](https://coveralls.io/repos/noxiouz/docker-registry-driver-elliptics/badge.png?branch=master)](https://coveralls.io/r/noxiouz/docker-registry-driver-elliptics?branch=master)
![Downloads](https://pypip.in/download/docker-registry-driver-elliptics/badge.svg)
## Usage

Assuming you have a working docker-registry and elliptics setup.

`pip install docker-registry-driver-elliptics`

Edit your configuration so that `storage` reads `elliptics`.


## Options

You may add any of the following to your main docker-registry configuration to further configure it.

1. `elliptics_nodes`: Elliptics remotes. Endpoint is `host:port:af`. `af` is address family as number. Use `2` for IPv4, `10` for IPv6
1. `elliptics_wait_timeout`: time to wait for the operation complete
1. `elliptics_check_timeout`: timeout for pinging node
1. `elliptics_io_thread_num`: number of IO threads in processing pool
1. `elliptics_net_thread_num`: number of threads in network processing pool
1. `elliptics_nonblocking_io_thread_num`: number of IO threads in processing pool dedicated to nonblocking ops
1. `elliptics_groups`: Elliptics groups registry should use
1. `elliptics_verbosity`: Elliptics logger verbosity `info|debug|notice|data|error`
1. `elliptics_logfile`: path to Elliptics logfile (default: `dev/stderr`)
1. `elliptics_node_flags`: names of flags for Node

Example:

```yaml
elliptics:
      <<: *common
      storage: elliptics
      elliptics_nodes: [
            "elliptics-host1:1025:2",
            "elliptics-host2:1025:10",
            ...
            "host:port:af" ] # or spaceseparated string
      elliptics_wait_timeout: 60
      elliptics_check_timeout: 60
      elliptics_io_thread_num: 2
      elliptics_net_thread_num: 2
      elliptics_nonblocking_io_thread_num: 2
      elliptics_groups: [1, 2, 3]
      elliptics_verbosity: "debug"
      elliptics_logfile: "/tmp/logfile.log"
      elliptics_node_flags: ["mix_stats", "no_csum"]
```

## Developer setup

Clone this.

Install `Elliptics`:

```
sudo apt-get install curl
curl http://repo.reverbrain.com/REVERBRAIN.GPG | sudo apt-key add -
sudo echo "deb http://repo.reverbrain.com/precise/ current/amd64/" | sudo tee -a /etc/apt/sources.list
sudo echo "deb http://repo.reverbrain.com/precise/ current/all/" | sudo tee -a /etc/apt/sources.list
sudo apt-get update
sudo apt-get install elliptics
```

Get your python ready:

```
sudo apt-get install python-pip
sudo pip install tox
```

Start the test `Elliptics`:

```
cd fixtures
sudo ./start.sh
```

You are ready to hack.
In order to verify what you did is ok, just run `tox`.

This will run the tests provided by [`docker-registry-core`](https://github.com/dotcloud/docker-registry/tree/master/depends/docker-registry-core)


## License

This is licensed under the Apache license.
Most of the code here comes from docker-registry, under an Apache license as well.

[pypi-url]: https://pypi.python.org/pypi/docker-registry-driver-elliptics
[pypi-image]: https://badge.fury.io/py/docker-registry-driver-elliptics.svg

[travis-url]: http://travis-ci.org/noxiouz/docker-registry-driver-elliptics
[travis-image]: https://secure.travis-ci.org/noxiouz/docker-registry-driver-elliptics.png?branch=master

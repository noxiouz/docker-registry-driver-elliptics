# FUSE module to mount Elliptics based Registry filesystem

## Installation

```(bash)
git clone https://github.com/noxiouz/docker-registry-driver-elliptics.git -b fuse
pip install .
pip install fusepy
```

Also you have to install `libfuse`.

## Usage 

Take a look into fuse_module directory. After that

```
sudo python registry-fs.py <path_to_conig_file> <mountpoint>
```

Options:
 + configuration file looks like registry configuration file.
 + mountpoint. Name says everything.


Configuration example:
```yaml
elliptics_nodes: [
        "host1:1025:2",
        "host2:1025:2"]
elliptics_io_thread_num: 4
elliptics_net_thread_num: 4
elliptics_wait_timeout: 30
elliptics_check_timeout: 60
elliptics_groups: [1, 2, 3]
elliptics_verbosity: 2
elliptics_logfile: "elliptics.log"
elliptics_namespace: "DOCKER"
loglevel: info
```

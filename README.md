# python_bloomd_sharded_driver
We abstract over a variable number of shards with https://github.com/posix4e/bloom-python-driver/blob/master/pybloomd.py


The bloomd_docker_scaff directory made it easy to get bloomd working in docker quickly. We assume that's running if we are runin main mode. In main mode we run some basic end to end tests against bloomd. Basically inserting rows and removing them.


To install
```
pip install docker-compose
```

To start and run tests

```
./start
```

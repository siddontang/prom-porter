# Prometheus Porter

Dump a time range data from Prometheus and load to another Prometheus for query. 

## Usage

### Write

1. Add the following configurtion to the Prometheus server

```yaml
remote_write:
  - url: "http://localhost:1234/write"
```

2. Start write server.
3. To dump data, run `curl http://localhost:1234/dump`


### Read

1. Add the following configuration to another Prometheus server

```yaml
remote_read:
  - url: "http://localhost:1235/read"
```

2. Start read server with the dump data
3. Access the Prometheus web for query

## Reference 

+ [remote-endpoints-and-storage](https://prometheus.io/docs/operating/integrations/#remote-endpoints-and-storage)
+ [remote-storage-integrations](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations)
+ [remote_storage_adapter](https://github.com/prometheus/prometheus/tree/master/documentation/examples/remote_storage/remote_storage_adapter)
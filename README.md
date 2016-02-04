# bss

Files watcher for the Illumina sequencers that streams reads through Kafka.

## Commands

To run the application and watch the `tmp` folder:
```
sbt "run tmp"
```

To run the watcher with some configuration changed (see `src/main/resources/application.conf` for options):
```
BSS_DB_BASE_PATH=dbx sbt "run tmp"
```

To prepare the docker image:
```
sbt docker:publishLocal
```

To run the image:
```
docker run -ti --name bss --rm -v $(pwd)/virtual_fc:/mnt bss:0.1 /mnt
```

To create an snapshot of the directory structure and timestamps/sizes:
```
sbt "run-main bss.tools.Snapshot tmp"
```

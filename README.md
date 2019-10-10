# Conclave

Infrastructure for defining and running large data workflows against multiple backends.

## Purpose

This framework allows users to define data analysis workflows in familiar frontend languages and then execute them on multiple data storage and processing backends (including privacy-preserving backend services that support secure multi-party computation).

## Dependencies

Conclave requires a Python 3.5 environment and was tested on Ubuntu (14.04+). See `requirements.txt` for other dependencies.

Consider using pyenv (https://github.com/pyenv/pyenv) to avoid changing `python` to `python3` in a bunch of places.

## Setup

Run `pip install -r requirements.txt`.

## Testing

The library comes with a number of tests::

    nosetests --with-doctest

## Network Setup

Note that the benchmarks under `benchmarks/` assume that party 1 is reachable at `ca-spark-node-0`, party 2 at `cb-spark-node-0`, and party 3 at `cc-spark-node-0`. You can modify your `/etc/hosts` file to map IP addresses to host addresses. To map the above to 127.0.0.1 (for a local run) include the following entry in your `/etc/hosts` file:

```bash
127.0.0.1	ca-spark-node-0 cb-spark-node-0 cc-spark-node-0
```

Most likely you already have a mapping for localhost, for example:

```bash
127.0.0.1	localhost
```

In that case, just append the node addresses after `localhost`.

You can also modify the party addresses inside `CodeGenConfig` by updating the `network_config` dict.

## Disclaimer

This is experimental software and does not guarantee security or correctness.

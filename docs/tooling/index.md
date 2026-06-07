---
title: "Tooling"
sidebar:
  label: "Overview"
description: "Command-line tools that ship with Kronotop: kronotop-cli and kronotop-ctl."
---

Kronotop ships with two command-line tools. Both are published as standalone jars on the
[Releases page](https://github.com/kronotop/kronotop/releases), bundled in the Docker image
`ghcr.io/kronotop/kronotop`, and can also be built as native binaries from source.

| Tool                            | Purpose                                                                   |
|---------------------------------|---------------------------------------------------------------------------|
| [kronotop-cli](kronotop-cli.md) | Interactive client for running commands against a Kronotop server.        |
| [kronotop-ctl](kronotop-ctl.md) | Control tool for cluster operations, such as bootstrapping a new cluster. |

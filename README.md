# 🚀 MQ Benchmarker

A simple, universal, multi-protocol benchmark tool for MQ Brokers *(Version 0.3.1: Apache (ActiveMQ) Artemis only. Others will follow)*. Designed to stress-test your messaging infrastructure and find its bottlenecks via an intuitive web dashboard. It is recommended to setup Prometheus combined with Grafana or similar to investigate metrics.

Update 7 september 2026: *Work in Progress!*
Backlog:
- Adding the possibility to save connection details
- Adding the possibility to add TLS (Both 1-way and 2 way, including a key/trusttore)
- Adding the possibility to add a custom message header to test messages

## ✨ Features

* **Multi-Protocol:** Supports both native Artemis CORE and the open AMQP 1.0 standard out-of-the-box.
* **Web Dashboard:** No more command-line hassle. Manage, monitor, and troubleshoot your benchmark live from your browser with a built-in terminal.
* **Enterprise Architecture:** Test with `SENDER`, `RECEIVER`, `BOTH` or simulate heavy backend traffic using `REQUESTER`/`RESPONDER` patterns *(Currently in development)*.
* **Master/Worker Ready:** Scale horizontally to thousands of threads via distributed worker nodes *(Currently in development)*.

## 🐳 Quick Start (Docker)

Start the benchmarker locally with the following command. Adjust if you like:

```bash
docker run -d \
  --name mq-benchmarker \
  -p 8080:8080 \
  --cpus="2.0" \
  -m 2g \
  -e JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0" \
  marcelbil/mq-benchmarker:latest
```

Next, open your browser and navigate to [http://localhost:8080](http://localhost:8080).
*(Default credentials: `admin` / `benchmark`)*

## ⚙️ Advanced Tuning (Java JVM)

When generating extreme loads, it is highly recommended to limit the container's resources to prevent OOM (Out Of Memory) crashes on the host. Use the `JAVA_TOOL_OPTIONS` and Docker resource limits as shown in the Quick Start guide. This ensures the JVM respects the container boundaries.

## 🏢 About the Author
This open-source project is developed and maintained by **[Prospectum-ICT](https://prospectum-ict.nl)**. 
We specialize in Enterprise Integration, Middleware, and robust Message Queue architectures. Need help with your messaging infrastructure? Feel free to reach out!

import cluster from "cluster";
import os from "os";
import { dirname } from "path";
import { fileURLToPath } from "url";
import { ClusterMemoryStorePrimary } from '@express-rate-limit/cluster-memory-store';

if(cluster.isPrimary) {
  const rateLimiterStore = new ClusterMemoryStorePrimary();
  rateLimiterStore.init();

  const __dirname = dirname(fileURLToPath(import.meta.url));

  const cpuCount = os.cpus().length;

  console.log(`The total number of CPUs is ${cpuCount}`);
  console.log(`Primary pid=${process.pid}`);
  cluster.setupPrimary({
    exec: __dirname + "/server.js",
  });

  for (let i = 0; i < cpuCount; i++) {
    cluster.fork();
  }
  cluster.on("exit", (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} has been killed: Code: ${code}, Signal: ${signal}`);
    console.log("Starting another worker");
    cluster.fork();
  });
}
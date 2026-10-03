import { mkdir, copyFile } from "node:fs/promises";
import { resolve } from "node:path";
// The SDK ships minified ESM and worker chunks. Serve these unchanged to avoid
// rebundling its large geographic engine; all files are still hosted locally.
await mkdir("public/vendor", { recursive: true });
for (const file of [
  "maplibre-gl.mjs",
  "maplibre-gl-shared.mjs",
  "maplibre-gl-worker.mjs",
]) {
  await copyFile(
    resolve("node_modules/maplibre-gl/dist", file),
    resolve("public/vendor", file),
  );
}

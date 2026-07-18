import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    exclude: [".repos/**"],
  },
  fmt: {
    ignorePatterns: [".repos/**"],
  },
  lint: {
    jsPlugins: [{ name: "vite-plus", specifier: "vite-plus/oxlint-plugin" }],
    rules: { "vite-plus/prefer-vite-plus-imports": "error" },
    options: { typeAware: true, typeCheck: true },
    ignorePatterns: [".repos/**"],
  },
  run: {
    cache: true,
  },
});

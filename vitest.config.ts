import { defineConfig } from "vitest/config"

export default defineConfig({
    test: {
        include: ["src/tests/**/*.test.ts"],
        testTimeout: 15_000,
        setupFiles: ["src/tests/setup.ts"],
        reporters: ["verbose"],
    },
})

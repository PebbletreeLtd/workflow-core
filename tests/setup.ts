
/**
 * Vitest global setup — silences noisy console output unless VERBOSE=1.
 *
 * console.error and console.warn are buffered during each test. On success
 * the buffer is discarded; on failure it is flushed to the real console so
 * the output appears next to the failure report.
 *
 *   npx vitest run              → quiet on green, errors shown on red
 *   VERBOSE=1 npx vitest run    → everything
 */
import { beforeAll, afterEach } from "vitest"

const buffer: { level: "warn" | "error"; args: any[] }[] = []

const originalError = console.error
const originalWarn = console.warn

function flush() {
    for (const entry of buffer) {
        if (entry.level === "error") originalError(...entry.args)
        else originalWarn(...entry.args)
    }
    buffer.length = 0
}

beforeAll(() => {
    if (!process.env["VERBOSE"]) {
        const noop = () => { }
        console.log = noop
        console.debug = noop
        console.info = noop
        console.warn = (...args: any[]) => { buffer.push({ level: "warn", args }) }
        console.error = (...args: any[]) => { buffer.push({ level: "error", args }) }
    }
})

afterEach(({ task }) => {
    if (task.result?.state === "fail") {
        flush()
    }
    buffer.length = 0
})

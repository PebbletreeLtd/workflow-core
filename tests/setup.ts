
/**
 * Vitest global setup — silences console output unless VERBOSE=1.
 *
 *   npx vitest run              → quiet
 *   VERBOSE=1 npx vitest run    → noisy
 */
import { beforeAll } from "vitest"

beforeAll(() => {
    if (!process.env["VERBOSE"]) {
        const noop = () => { }
        console.log = noop
        console.debug = noop
        console.warn = noop
        console.error = noop
        console.info = noop
    }
})

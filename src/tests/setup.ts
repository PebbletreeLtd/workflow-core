
/**
 * Global test setup — silences noisy console output unless VERBOSE=1.
 *
 * Loaded via `node --import ./dist-tests/tests/setup.js`. Runs at module load,
 * so it takes effect before any test file runs.
 *
 *   npm test              → quiet
 *   VERBOSE=1 npm test    → everything
 */
if (!process.env["VERBOSE"]) {
    const noop = () => { }
    console.log = noop
    console.debug = noop
    console.info = noop
    console.warn = noop
    console.error = noop
}

export { }

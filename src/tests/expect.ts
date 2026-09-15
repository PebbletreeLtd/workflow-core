/**
 * Minimal `expect`-style assertion facade over `node:assert/strict`.
 *
 * Lets us keep vitest-style assertion syntax while running under `node --test`.
 * Only the matchers actually used in the suite are implemented.
 */
import { strictEqual, notStrictEqual, deepStrictEqual, notDeepStrictEqual } from "node:assert/strict"

interface Assertion<T> {
    toBe(expected: T): void
    toEqual(expected: unknown): void
    toContain(value: unknown): void
    toBeDefined(): void
    toBeUndefined(): void
    toBeNull(): void
    toBeGreaterThan(value: number): void
    toBeGreaterThanOrEqual(value: number): void
    toBeLessThan(value: number): void
    toBeLessThanOrEqual(value: number): void
    readonly not: Assertion<T>
}

export function expect<T>(actual: T): Assertion<T> {
    const build = (negate: boolean): Assertion<T> => ({
        toBe(expected) {
            if (negate) notStrictEqual(actual, expected)
            else strictEqual(actual, expected)
        },
        toEqual(expected) {
            if (negate) notDeepStrictEqual(actual, expected)
            else deepStrictEqual(actual, expected)
        },
        toContain(value) {
            const has = actual != null && typeof (actual as any).includes === "function"
                ? (actual as any).includes(value)
                : false
            strictEqual(has, !negate, `expected ${JSON.stringify(actual)} ${negate ? "not " : ""}to contain ${JSON.stringify(value)}`)
        },
        toBeDefined() {
            if (negate) strictEqual(actual, undefined)
            else notStrictEqual(actual, undefined)
        },
        toBeUndefined() {
            if (negate) notStrictEqual(actual, undefined)
            else strictEqual(actual, undefined)
        },
        toBeNull() {
            if (negate) notStrictEqual(actual, null)
            else strictEqual(actual, null)
        },
        toBeGreaterThan(value) {
            const cond = (actual as any) > value
            strictEqual(cond, !negate, `expected ${actual} ${negate ? "not " : ""}> ${value}`)
        },
        toBeGreaterThanOrEqual(value) {
            const cond = (actual as any) >= value
            strictEqual(cond, !negate, `expected ${actual} ${negate ? "not " : ""}>= ${value}`)
        },
        toBeLessThan(value) {
            const cond = (actual as any) < value
            strictEqual(cond, !negate, `expected ${actual} ${negate ? "not " : ""}< ${value}`)
        },
        toBeLessThanOrEqual(value) {
            const cond = (actual as any) <= value
            strictEqual(cond, !negate, `expected ${actual} ${negate ? "not " : ""}<= ${value}`)
        },
        get not() { return build(!negate) },
    })
    return build(false)
}

/**
 * Generic workflow capabilities system.
 * 
 * Maps job types to JobRunner constructors and manages the capability
 * bitmap buffer. The consumer provides the runner map at construction time —
 * the package has no knowledge of specific job types or processor implementations.
 */
import type { BasicJobPayload } from "./workflowTypes"
import type { JobRunnerConstructor } from "./jobRunner"

export class WorkflowCapabilities<PAYLOAD_T extends BasicJobPayload = BasicJobPayload> {
    /** Map of job type → JobRunner constructor */
    readonly runnerMap: ReadonlyMap<PAYLOAD_T["type"], JobRunnerConstructor<PAYLOAD_T>>
    /** Compact bitmap encoding of which types this server can run */
    readonly capabilitiesBuffer: Buffer

    protected constructor(args: {
        /** All possible job type values, sorted ascending */
        allCapabilities: PAYLOAD_T["type"][],
        /** Map of job type → JobRunner constructor for types this server can handle */
        runners: Map<PAYLOAD_T["type"], JobRunnerConstructor<PAYLOAD_T>>
    }) {
        this.runnerMap = args.runners
        this.capabilitiesBuffer = WorkflowCapabilities.capabilitiesToBuffer(
            Array.from(args.runners.keys()),
            args.allCapabilities,
        )
    }

    /**
     * Check whether this server can run a given job type.
     * Accepts either a numeric type value or a string type name that will
     * be resolved via the provided enum lookup.
     */

    canRunType(type: PAYLOAD_T["type"]): boolean {
        return this.runnerMap.has(type)
    }

    /**
     * Get the runner constructor for a given job payload.
     * Returns null if no runner is registered for this payload type.
     */
    getRunner(payload: PAYLOAD_T): JobRunnerConstructor<PAYLOAD_T> | null {
        return this.runnerMap.get(payload.type) ?? null
    }


    // =========================================================================
    // Statics
    // =========================================================================
    /**
     * Capability buffer utilities.
     * 
     * Pure bitwise logic for encoding, decoding, and merging capability bitmaps.
     * No dependencies beyond Node Buffer — ready for extraction.
     */

    /**
     * Merge two capability buffers using bitwise OR.
     * Returns a buffer large enough to contain the larger of the two inputs.
     * 
     * @param target The buffer to merge INTO (modified in place, or replaced if too small)
     * @param source The buffer to merge FROM
     */
    static mergeCapabilityBuffers(target: Buffer, source: Buffer): Buffer {
        const maxLen = Math.max(target.length, source.length)
        if (target.length < maxLen) {
            const newBuffer = Buffer.alloc(maxLen)
            target.copy(newBuffer)
            target = newBuffer
        }
        for (let i = 0; i < source.length; i++) {
            target[i] = target[i]! | source[i]!
        }
        return target
    }

    /**
     * Convert an array of string capability values to a bitmap buffer.
     * Each capability N occupies bit (N-1) in the buffer: byte Math.floor((N-1)/8), bit (N-1)%8.
     * 
     * @param capabilities The capability values to encode
     * @param allSorted All possible capability values, sorted ascending. Used to determine buffer size.
     */
    static capabilitiesToBuffer<T extends string>(capabilities: T[], allSorted: T[]): Buffer {
        const maxValue = allSorted.length
        if (maxValue === undefined) throw new Error("No capabilities defined")
        const buffer = Buffer.alloc(Math.ceil(maxValue / 8))
        for (const [idx, cap] of allSorted.entries()) {
            if (capabilities.includes(cap)) {
                const byteIdx = Math.floor(idx / 8)
                const bitIdx = (idx) % 8
                buffer[byteIdx]! |= (1 << bitIdx)
            }
        }
        return buffer
    }

    /**
     * Decode a capability bitmap buffer back to an array of capability values.
     * Inverse of capabilitiesToBuffer.
     * 
     * @param buffer The bitmap buffer to decode
     * @param allSorted All possible capability values, sorted ascending.
     */
    static bufferToCapabilities(buffer: Buffer, allSorted: number[]): number[] {
        const ret: number[] = []
        for (const cap of allSorted) {
            const byteIdx = Math.floor((cap - 1) / 8)
            const bitIdx = (cap - 1) % 8
            const byte = buffer[byteIdx]
            if (byte !== undefined && (byte & (1 << bitIdx))) {
                ret.push(cap)
            }
        }
        return ret
    }

    static Create<T extends string[]>(allCapabilities: T) {
        return <PAYLOAD_T extends BasicJobPayload & { type: T[number] }>(args: {
            runners: Map<PAYLOAD_T["type"] & T[number], JobRunnerConstructor<PAYLOAD_T>>
        }) => {
            return new WorkflowCapabilities<PAYLOAD_T>({
                runners: args.runners,
                allCapabilities
            })
        }
    }
}

/**
 * Generic workflow capabilities system.
 * 
 * Maps numeric job types to runner functions and manages the capability
 * bitmap buffer. The consumer provides the runner map at construction time —
 * the package has no knowledge of specific job types or processor implementations.
 */
import { capabilitiesToBuffer, bufferToCapabilities, mergeCapabilityBuffers } from "./capabilityBuffer"
import type { BasicJobPayload, JobRunner } from "./workflowTypes"

export class WorkflowCapabilities<PAYLOAD_T extends BasicJobPayload = BasicJobPayload> {
    /** Map of numeric job type → runner */
    readonly runnerMap: ReadonlyMap<number, JobRunner<PAYLOAD_T>>
    /** Compact bitmap encoding of which types this server can run */
    readonly capabilitiesBuffer: Buffer

    constructor(args: {
        /** All possible numeric job type values, sorted ascending */
        allSortedCapabilities: number[]
        /** Map of numeric job type → runner function/object for types this server can handle */
        runners: Map<number, JobRunner<PAYLOAD_T>>
    }) {
        this.runnerMap = args.runners
        this.capabilitiesBuffer = capabilitiesToBuffer(
            Array.from(args.runners.keys()),
            args.allSortedCapabilities,
        )
    }

    /**
     * Check whether this server can run a given job type.
     * Accepts either a numeric type value or a string type name that will
     * be resolved via the provided enum lookup.
     */
    canRunType(type: number): boolean
    canRunType(type: string, enumLookup: Record<string, number>): boolean
    canRunType(type: number | string, enumLookup?: Record<string, number>): boolean {
        const numericType = typeof type === "string"
            ? enumLookup?.[type]
            : type
        if (numericType === undefined) return false
        return this.runnerMap.has(numericType)
    }

    /**
     * Get the runner for a given job payload.
     * Returns null if no runner is registered for this payload type.
     */
    getRunner(payload: PAYLOAD_T, enumLookup: Record<string, number>): JobRunner<PAYLOAD_T> | null {
        const enumValue = enumLookup[payload.type]
        if (enumValue === undefined) {
            console.error("Capability type " + payload.type + " not found in enum lookup")
            return null
        }
        return (this.runnerMap.get(enumValue) as JobRunner<PAYLOAD_T>) ?? null
    }

    // =========================================================================
    // Static bitmap utilities
    // =========================================================================

    /**
     * Convert an array of numeric capability values to a bitmap buffer.
     */
    static convertCapabilitiesArrayToBuffer(capabilities: number[], allSorted: number[]): Buffer {
        return capabilitiesToBuffer(capabilities, allSorted)
    }

    /**
     * Merge two capability buffers using bitwise OR.
     */
    static mergeCapabilityBuffers(target: Buffer, source: Buffer): Buffer {
        return mergeCapabilityBuffers(target, source)
    }

    /**
     * Decode a capability buffer back to an array of numeric values.
     */
    static bufferToCapabilities(buffer: Buffer, allSorted: number[]): number[] {
        return bufferToCapabilities(buffer, allSorted)
    }
}

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
export function mergeCapabilityBuffers(target: Buffer, source: Buffer): Buffer {
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
 * Convert an array of numeric capability values to a bitmap buffer.
 * Each capability N occupies bit (N-1) in the buffer: byte Math.floor((N-1)/8), bit (N-1)%8.
 * 
 * @param capabilities The capability values to encode
 * @param allSorted All possible capability values, sorted ascending. Used to determine buffer size.
 */
export function capabilitiesToBuffer(capabilities: number[], allSorted: number[]): Buffer {
    const maxValue = allSorted[allSorted.length - 1]
    if (maxValue === undefined) throw new Error("No capabilities defined")
    const buffer = Buffer.alloc(Math.ceil(maxValue / 8))
    for (const cap of allSorted) {
        if (capabilities.includes(cap)) {
            const byteIdx = Math.floor((cap - 1) / 8)
            const bitIdx = (cap - 1) % 8
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
export function bufferToCapabilities(buffer: Buffer, allSorted: number[]): number[] {
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

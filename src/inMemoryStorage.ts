/**
 * In-memory job storage implementation.
 *
 * A fully functional WorkflowJobStorage backed by @pebbletree/mvcc-testing,
 * suitable for tests and simulated job runners (no external database required).
 */
import type {
    WorkflowJobKey,
    WorkflowJobValue,
    WorkflowJobLogKey,
    WorkflowJobLogValue,
    BasicJobPayload,
} from "./workflowTypes"
import * as tuple from "fdb-tuple"
import type {
    atSubspaceKey,
    WorkflowStorageTransaction,
    WorkflowJobStorage,
} from "./workflowStorageAdapter"
import { MVCCCore } from "@pebbletree/mvcc-testing"
import { TOMBSTONE } from "@pebbletree/mvcc-testing/dist/types";


export class InMemoryJobStorage<PAYLOAD_T extends BasicJobPayload = BasicJobPayload>
    implements WorkflowJobStorage<PAYLOAD_T, WorkflowStorageTransaction<PAYLOAD_T>> {
    readonly JobDatabase = new MVCCCore.Store<WorkflowJobKey, WorkflowJobKey, WorkflowJobValue<PAYLOAD_T>, WorkflowJobValue<PAYLOAD_T>>({
        keyTransformer: {
            pack(value) {
                return tuple.pack([value.job_id])
            },
            unpack(buffer) {
                const [job_id] = tuple.unpack(buffer)
                if (typeof job_id !== "string") {
                    throw new Error("Invalid job_id in key")
                }
                return { job_id }
            }
        },
        prefix: "jobs"
    });
    readonly JoblogSubpace = new MVCCCore.Subspace<WorkflowJobLogKey, WorkflowJobLogKey, unknown, WorkflowJobLogValue>({
        pack(value) {
            return tuple.pack([value.timestamp, value.job_id, value.random])
        },
        unpack(buffer) {
            const [timestamp, job_id, random] = tuple.unpack(buffer)
            if (typeof job_id !== "string" || typeof timestamp !== "number" || typeof random !== "string") {
                throw new Error("Invalid log key")
            }
            return { job_id, timestamp, random }
        }
    }, "logs")
    readonly atSubspace;
    readonly executorIndex = new MVCCCore.DerivedSubspace<WorkflowJobKey, WorkflowJobKey, WorkflowJobValue<PAYLOAD_T>, WorkflowJobValue<PAYLOAD_T>, { execution_id: string } & WorkflowJobKey, { execution_id: string } & WorkflowJobKey>({
        source: this.JobDatabase,
        mapKey(key, value) {
            return { execution_id: value.header.execution_id || "", job_id: key.job_id }
        },
        prefix: "executor",
        keyTransformer: {
            pack(value) {
                return tuple.pack([value.execution_id, value.job_id])
            },
            unpack(buffer) {
                const [execution_id, job_id] = tuple.unpack(buffer)
                if (typeof execution_id !== "string" || typeof job_id !== "string") {
                    throw new Error("Invalid executor index key")
                }
                return { execution_id, job_id }
            }
        }
    });
    readonly executorSubspace;

    private watchers = new Map<string /* hex-packed job key */, Set<{
        predicate: (job: WorkflowJobValue<PAYLOAD_T> | undefined) => boolean
        resolve: (job: WorkflowJobValue<PAYLOAD_T> | undefined) => void
    }>>()

    /**
     * Resolve when `predicate` becomes true for the job at `jobKey`.
     *
     * Checks the current committed value first (resolves immediately if the
     * predicate already holds), otherwise subscribes to commits on that key
     * and resolves on the first matching write. `undefined` is passed to the
     * predicate for tombstoned (cleared) values.
     *
     * Rejects after `timeoutMs` (default 5000). This is a test helper — the
     * timeout uses real time regardless of any injected WorkflowClock.
     */
    async waitFor(
        jobKey: WorkflowJobKey,
        predicate: (job: WorkflowJobValue<PAYLOAD_T> | undefined) => boolean,
        options?: { timeoutMs?: number; message?: string },
    ): Promise<WorkflowJobValue<PAYLOAD_T> | undefined> {
        const current = await this.doTn(txn => txn.job.get(jobKey))
        if (predicate(current ?? undefined)) return current ?? undefined

        const hexKey = this.JobDatabase.packKey(jobKey).toString("hex")
        return new Promise<WorkflowJobValue<PAYLOAD_T> | undefined>((resolve, reject) => {
            const bucket = this.watchers.get(hexKey) ?? new Set()
            this.watchers.set(hexKey, bucket)

            const timeout = globalThis[`set${"Timeout"}`](() => {
                bucket.delete(watcher)
                if (bucket.size === 0) this.watchers.delete(hexKey)
                reject(new Error(`waitFor timed out after ${options?.timeoutMs ?? 5000}ms: ${options?.message ?? "predicate not satisfied"}`))
            }, options?.timeoutMs ?? 5000)
            if (typeof timeout?.unref === "function") timeout.unref()

            const watcher = {
                predicate,
                resolve: (job: WorkflowJobValue<PAYLOAD_T> | undefined) => {
                    globalThis[`clear${"Timeout"}`](timeout)
                    resolve(job)
                },
            }
            bucket.add(watcher)
        })
    }

    private notifyWatchers(hexKey: string, value: WorkflowJobValue<PAYLOAD_T> | undefined) {
        const bucket = this.watchers.get(hexKey)
        if (!bucket) return
        for (const w of Array.from(bucket)) {
            if (w.predicate(value)) {
                bucket.delete(w)
                w.resolve(value)
            }
        }
        if (bucket.size === 0) this.watchers.delete(hexKey)
    }

    doTn<R>(callback: (txn: WorkflowStorageTransaction<PAYLOAD_T>) => Promise<R>) {
        return this.JobDatabase.doTransaction(async (txn) => {
            const sTxn: WorkflowStorageTransaction<PAYLOAD_T> = {
                job: {
                    get: async (key) => txn.get(key),
                    snapshotGet: async (key) => txn.snapshot().get(key),
                    set: (key, value) => txn.set(key, value),
                    clear: (key) => txn.clear(key),
                },
                jobLogKey: {
                    set: (key, value) => txn.at(this.JoblogSubpace).set(key, value),
                },
                at: {
                    getRangeSnapshot: (startKey, endKey, options) => txn.snapshot().at(this.atSubspace).getRange(startKey, endKey, options)
                },
                executor: {
                    getRangeAllStartsWith: async (startKey, options) => txn.at(this.executorSubspace).getRangeAllStartsWith(startKey, options)
                }
            }
            return callback(sTxn)
        })
    };
    constructor(args: {
        typeIndex: boolean
    }) {
        this.JobDatabase.onCommit((committed) => {
            try {
                if (this.watchers.size)
                    for (const [hexKey, value] of committed) {
                        if (!this.watchers.has(hexKey)) continue
                        const unpacked = value === TOMBSTONE
                            ? undefined
                            : typeof value === "string"
                                ? this.JobDatabase.unpackValue(Buffer.from(value, "utf-8"))
                                : this.JobDatabase.unpackValue(value)
                        this.notifyWatchers(hexKey, unpacked)
                    }
            } catch (e) {
                throw e;
            }
        })
        const atIndex = new MVCCCore.DerivedSubspace<WorkflowJobKey, WorkflowJobKey, WorkflowJobValue<PAYLOAD_T>, WorkflowJobValue<PAYLOAD_T>, atSubspaceKey<PAYLOAD_T>, atSubspaceKey<PAYLOAD_T>>({
            source: this.JobDatabase,
            mapKey(key, value) {
                return { at: value.header.at, type: args.typeIndex ? value.payload.type : undefined, job_id: key.job_id }
            },
            prefix: "at",
            keyTransformer: {
                pack(value) {
                    if (value.type && args.typeIndex)
                        return tuple.pack([value.at, value.type, value.job_id])
                    return tuple.pack([value.at, value.job_id])
                },
                unpack(buffer) {
                    if (args.typeIndex) {
                        const [at, type, job_id] = tuple.unpack(buffer)
                        if (typeof at !== "number" || typeof job_id !== "string" || typeof type !== "string") {
                            throw new Error("Invalid at index key")
                        }
                        return { at, type, job_id }
                    } else {
                        const [at, job_id] = tuple.unpack(buffer)
                        if (typeof at !== "number" || typeof job_id !== "string") {
                            throw new Error("Invalid at index key")
                        }
                        return { at, job_id, type: undefined }
                    }
                }
            }
        })
        this.atSubspace = atIndex.withKeyEncoding({
            unpack: (val) => atIndex.keyXf.unpack(val),
            pack(value: { at: number }) {
                return tuple.pack([value.at])
            },
        });
        this.executorSubspace = this.executorIndex.withKeyEncoding({
            ...this.executorIndex.keyXf,
            pack(value: { execution_id: string }) {
                return tuple.pack([value.execution_id])
            },
        })
    }
}
